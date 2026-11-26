//! Hidden `daemon` subcommand: starting, running, inspecting, stopping, and emitting events.

use crate::config::{
    TRANSFER_ADDR, Tools, check_listen_addr, check_routable_addr, http_addr, http_token,
    instructions_file, instructions_tool_mode, transfer_addr,
};
use crate::daemon;
use crate::endpoint;
use crate::events;
use crate::id;
use crate::ipc::{self, ToDaemon, ToEndpoint};
use crate::lease;
use crate::platform::runtime;
use crate::session::LaunchContext;
use crate::transport;

/// The hidden `daemon` subcommand: the daemon process itself, and the CLI that starts, inspects,
/// and stops one.
pub(crate) fn daemon_main(args: &[std::ffi::OsString]) -> i32 {
    let Some((action, arguments)) = args.split_first() else {
        return daemon_usage_error("expected a daemon subcommand");
    };
    let action = action.to_string_lossy();
    let options = match action.as_ref() {
        "serve" => {
            let Some(options) = arguments.get(2..) else {
                eprintln!(
                    "fastexec: usage: fastexec daemon serve <namespace> <build> [--http <addr>] [--idle-exit on|off]"
                );
                return 2;
            };
            options
        }
        "start" | "run" | "status" | "stop" | "emit" => arguments,
        _ => return daemon_usage_error(&format!("unknown argument {action}")),
    };
    let flags = match daemon_flags(&action, options) {
        Ok(flags) => flags,
        Err(message) => return daemon_usage_error(&message),
    };
    match action.as_ref() {
        "serve" => daemon::serve_process(
            arguments[0].to_string_lossy().into_owned(),
            arguments[1].to_string_lossy().into_owned(),
            flags.http,
            flags.idle_exit.unwrap_or_default(),
            false,
        ),
        "run" => cli_run(flags),
        _ => match tokio::runtime::Runtime::new() {
            Ok(runtime) => {
                let code = runtime.block_on(cli(&action, flags));
                // A start worker abandoned at the deadline must not delay the CLI's exit.
                std::process::exit(code);
            }
            Err(error) => fail(&format!("cannot start the runtime: {error}")),
        },
    }
}

const DAEMON_USAGE: &str = "usage: fastexec daemon start [--http <addr>] [--idle-exit on|off] | run [--http <addr>] | status | stop | emit [--namespace <ns>] [--source <src>] [--topic <topic>] [--stream]";

#[derive(Default)]
pub(super) struct DaemonFlags {
    pub(super) http: Option<String>,
    pub(super) idle_exit: Option<bool>,
    pub(super) namespace: Option<std::ffi::OsString>,
    pub(super) source: Option<String>,
    pub(super) topic: Option<String>,
    pub(super) stream: bool,
}

pub(super) fn daemon_flags(
    action: &str,
    args: &[std::ffi::OsString],
) -> Result<DaemonFlags, String> {
    let mut flags = DaemonFlags::default();
    let mut args = args.iter();
    while let Some(flag) = args.next() {
        match flag.to_str() {
            Some("--http")
                if matches!(action, "start" | "run" | "serve") && flags.http.is_none() =>
            {
                let value = args
                    .next()
                    .ok_or("--http needs an address (host:port)")?
                    .to_string_lossy();
                check_listen_addr("--http", &value, true)?;
                flags.http = Some(value.into_owned());
            }
            Some("--idle-exit")
                if matches!(action, "start" | "serve") && flags.idle_exit.is_none() =>
            {
                let value = args.next().ok_or("--idle-exit needs on or off")?;
                flags.idle_exit = Some(match value.to_str() {
                    Some("on") => true,
                    Some("off") => false,
                    _ => {
                        return Err(format!(
                            "--idle-exit {} must be on or off",
                            value.to_string_lossy()
                        ));
                    }
                });
            }
            Some("--namespace") if action == "emit" && flags.namespace.is_none() => {
                let value = args.next().ok_or("--namespace needs a value")?;
                if value.is_empty() {
                    return Err("--namespace must not be empty".to_owned());
                }
                flags.namespace = Some(value.clone());
            }
            Some("--source") if action == "emit" && flags.source.is_none() => {
                let value = args
                    .next()
                    .ok_or("--source needs a value")?
                    .to_string_lossy();
                if value.trim().is_empty() {
                    return Err("--source must not be empty".to_owned());
                }
                flags.source = Some(value.into_owned());
            }
            Some("--topic") if action == "emit" && flags.topic.is_none() => {
                let value = args
                    .next()
                    .ok_or("--topic needs a value")?
                    .to_string_lossy();
                if value.trim().is_empty() {
                    return Err("--topic must not be empty".to_owned());
                }
                flags.topic = Some(value.into_owned());
            }
            Some("--stream") if action == "emit" && !flags.stream => {
                flags.stream = true;
            }
            _ => return Err(format!("unknown argument {}", flag.to_string_lossy())),
        }
    }
    Ok(flags)
}

fn daemon_usage_error(message: &str) -> i32 {
    eprintln!("fastexec: {message}\n{DAEMON_USAGE}");
    2
}

fn fail(message: &str) -> i32 {
    eprintln!("fastexec: {message}");
    1
}

/// `daemon run`: the daemon in this process, in the foreground. It derives the namespace and the
/// build exactly as `start` does, and it holds no runtime of its own here: the daemon's runtime
/// starts once, inside the daemon.
fn cli_run(flags: DaemonFlags) -> i32 {
    let launch = match LaunchContext::current() {
        Ok(launch) => launch,
        Err(error) => return fail(&format!("cannot read the launch context: {error}")),
    };
    let identity = match runtime::identity(&launch) {
        Ok(identity) => identity,
        Err(error) => return fail(&format!("cannot identify this image: {error}")),
    };
    if let Err(error) = runtime::paths(&identity.namespace) {
        return fail(&format!("cannot use the runtime directory: {error}"));
    }
    daemon::serve_process(identity.namespace, identity.build, flags.http, false, true)
}

/// The CLI: one exchange with the namespace's daemon, and what it prints.
async fn cli(action: &str, flags: DaemonFlags) -> i32 {
    let launch = match LaunchContext::current() {
        Ok(launch) => launch,
        Err(error) => return fail(&format!("cannot read the launch context: {error}")),
    };
    let identity = match runtime::identity_with_namespace(&launch, flags.namespace.as_deref()) {
        Ok(identity) => identity,
        Err(error) => return fail(&format!("cannot identify this image: {error}")),
    };
    let paths = match runtime::paths(&identity.namespace) {
        Ok(paths) => paths,
        Err(error) => return fail(&format!("cannot use the runtime directory: {error}")),
    };
    if action == "emit" {
        return cli_emit(&identity, &paths, flags).await;
    }
    // One deadline bounds each daemon control exchange.
    let deadline = std::time::Instant::now() + runtime::START_WAIT;
    match action {
        "start" => cli_start(&identity, &paths, &launch, deadline, flags).await,
        "status" => cli_status(&identity, &paths, &launch, deadline).await,
        _ => cli_stop(&identity, &paths, &launch, deadline).await,
    }
}

/// Parses a single JSON object from stdin into an [`events::ExternalEventInput`], applying CLI
/// `--source` and `--topic` defaults.
pub(super) fn parse_emit_payload(
    raw: &str,
    default_source: Option<&str>,
    default_topic: Option<&str>,
) -> Result<events::ExternalEventInput, String> {
    if raw.len() > events::MAX_EXTERNAL_EVENT_BYTES {
        return Err(format!(
            "event payload ({} bytes) exceeds {} byte limit",
            raw.len(),
            events::MAX_EXTERNAL_EVENT_BYTES
        ));
    }
    let value: serde_json::Value =
        serde_json::from_str(raw).map_err(|e| format!("invalid JSON: {e}"))?;
    let obj = value
        .as_object()
        .ok_or_else(|| "event payload must be a JSON object".to_owned())?;

    for key in obj.keys() {
        if !matches!(
            key.as_str(),
            "eventId"
                | "name"
                | "timestamp"
                | "data"
                | "source"
                | "topic"
                | "eventType"
                | "cursor"
                | "_meta"
        ) {
            return Err(format!("unknown field `{key}`"));
        }
    }

    let str_field = |key: &str| -> Result<Option<String>, String> {
        match obj.get(key) {
            None | Some(serde_json::Value::Null) => Ok(None),
            Some(serde_json::Value::String(s)) => Ok(Some(s.clone())),
            Some(_) => Err(format!("field `{key}` must be a string")),
        }
    };

    let event_type = match obj.get("eventType") {
        None => None,
        Some(serde_json::Value::String(s)) if s == events::EVENT_EXTERNAL => Some(s.clone()),
        Some(serde_json::Value::String(s)) => {
            return Err(format!(
                "field `eventType` must be {:?}, got {s:?}",
                events::EVENT_EXTERNAL
            ));
        }
        Some(_) => return Err("field `eventType` must be a string".to_owned()),
    };
    let timestamp = match obj.get("timestamp") {
        None => None,
        Some(serde_json::Value::String(s)) if !s.trim().is_empty() => Some(s.clone()),
        Some(_) => return Err("field `timestamp` must be a non-empty string".to_owned()),
    };
    let source = str_field("source")?
        .filter(|s| !s.trim().is_empty())
        .or_else(|| {
            default_source
                .filter(|s| !s.trim().is_empty())
                .map(str::to_owned)
        })
        .ok_or_else(|| "missing required `source` (provide in JSON or via --source)".to_owned())?;
    let topic = str_field("topic")?
        .filter(|s| !s.trim().is_empty())
        .or_else(|| {
            default_topic
                .filter(|s| !s.trim().is_empty())
                .map(str::to_owned)
        });
    let name = str_field("name")?
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| "missing required `name`".to_owned())?;
    let event_id = str_field("eventId")?
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| "missing required `eventId`".to_owned())?;
    let data = obj
        .get("data")
        .cloned()
        .ok_or_else(|| "missing required `data`".to_owned())?;

    Ok(events::ExternalEventInput {
        event_type,
        source,
        topic,
        name,
        event_id,
        timestamp,
        data,
    })
}

async fn connect_emit_daemon(
    identity: &runtime::Identity,
    paths: &runtime::Paths,
    deadline: std::time::Instant,
) -> Result<transport::Client, String> {
    let Some(mut client) = within(deadline, connect_existing(paths)).await? else {
        return Err(format!(
            "no fastexec daemon is running for namespace {}",
            identity.namespace
        ));
    };
    let hello = ToDaemon::HelloEmit {
        protocol: ipc::PROTOCOL,
        build: identity.build.clone(),
    };
    within(deadline, async {
        ipc::write_frame_idle(&mut client, &hello, daemon::WRITE_STALL)
            .await
            .map_err(|e| format!("cannot send HelloEmit: {e}"))?;
        let frame = ipc::read_endpoint_frame_leased(
            &mut client,
            lease::FRAME_LIMIT,
            lease::process_pool(),
            lease::control_pool(),
            |_| true,
        )
        .await
        .map_err(|e| format!("cannot read Welcome: {e}"))?;
        match frame {
            ipc::LeasedEndpointFrame::Frame(ToEndpoint::Welcome { protocol, .. }, _)
                if protocol == ipc::PROTOCOL =>
            {
                Ok(())
            }
            ipc::LeasedEndpointFrame::Frame(ToEndpoint::Reject { message }, _) => Err(message),
            _ => Err("unexpected response to HelloEmit".to_owned()),
        }
    })
    .await?;
    Ok(client)
}

async fn send_emit_event(
    client: &mut transport::Client,
    event: events::ExternalEventInput,
) -> Result<ToEndpoint, String> {
    ipc::write_frame_idle(client, &ToDaemon::EmitEvent { event }, daemon::WRITE_STALL)
        .await
        .map_err(|e| format!("cannot send EmitEvent: {e}"))?;
    match ipc::read_endpoint_frame_leased(
        client,
        lease::FRAME_LIMIT,
        lease::process_pool(),
        lease::control_pool(),
        |_| true,
    )
    .await
    .map_err(|e| format!("cannot read EmitAck: {e}"))?
    {
        ipc::LeasedEndpointFrame::Frame(ack @ ToEndpoint::EmitAck { .. }, _) => Ok(ack),
        ipc::LeasedEndpointFrame::Eof => Err("daemon closed the connection".to_owned()),
        _ => Err("unexpected frame from daemon".to_owned()),
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum BoundedLine {
    Eof,
    Line(Vec<u8>),
    TooLong,
}

/// Reads one line up to `max_bytes` (including `\r\n`). If a line exceeds `max_bytes`, drains and
/// discards bytes until the next `\n` or EOF so an unbounded single line cannot OOM the process.
pub(super) async fn read_bounded_line<R: tokio::io::AsyncBufRead + Unpin>(
    reader: &mut R,
    buf: &mut Vec<u8>,
    max_bytes: usize,
) -> std::io::Result<BoundedLine> {
    use tokio::io::AsyncBufReadExt as _;
    buf.clear();
    let mut overflowed = false;
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            return Ok(if overflowed {
                BoundedLine::TooLong
            } else if buf.is_empty() {
                BoundedLine::Eof
            } else {
                BoundedLine::Line(std::mem::take(buf))
            });
        }
        let newline_pos = available.iter().position(|&b| b == b'\n');
        let consume = match newline_pos {
            Some(pos) => pos + 1,
            None => available.len(),
        };
        if !overflowed {
            if buf.len().saturating_add(consume) > max_bytes {
                overflowed = true;
                buf.clear();
            } else {
                buf.extend_from_slice(&available[..consume]);
            }
        }
        reader.consume(consume);
        if newline_pos.is_some() {
            return Ok(if overflowed {
                BoundedLine::TooLong
            } else {
                BoundedLine::Line(std::mem::take(buf))
            });
        }
    }
}

async fn cli_emit(identity: &runtime::Identity, paths: &runtime::Paths, flags: DaemonFlags) -> i32 {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    if !flags.stream {
        let mut buf = Vec::new();
        let mut stdin = tokio::io::stdin().take((events::MAX_EXTERNAL_EVENT_BYTES + 1) as u64);
        if let Err(error) = stdin.read_to_end(&mut buf).await {
            return fail(&format!("cannot read stdin: {error}"));
        }
        if buf.len() > events::MAX_EXTERNAL_EVENT_BYTES {
            return fail(&format!(
                "event payload exceeds {} byte limit",
                events::MAX_EXTERNAL_EVENT_BYTES
            ));
        }
        let raw = match String::from_utf8(buf) {
            Ok(s) => s,
            Err(error) => return fail(&format!("stdin is not valid UTF-8: {error}")),
        };
        let event =
            match parse_emit_payload(raw.trim(), flags.source.as_deref(), flags.topic.as_deref()) {
                Ok(ev) => ev,
                Err(message) => return fail(&message),
            };
        let deadline = std::time::Instant::now() + runtime::START_WAIT;
        let mut client = match connect_emit_daemon(identity, paths, deadline).await {
            Ok(client) => client,
            Err(message) => return fail(&message),
        };
        let ack = match within(deadline, send_emit_event(&mut client, event)).await {
            Ok(ack) => ack,
            Err(message) => return fail(&message),
        };
        let _ = ipc::write_frame_idle(
            &mut client,
            &ToDaemon::Bye,
            std::time::Duration::from_secs(1),
        )
        .await;
        return match ack {
            ToEndpoint::EmitAck { ok: true, .. } => 0,
            ToEndpoint::EmitAck { error, .. } => fail(
                &error
                    .map(|e| e.message)
                    .unwrap_or_else(|| "daemon rejected event".to_owned()),
            ),
            _ => fail("unexpected response from daemon"),
        };
    }

    let deadline = std::time::Instant::now() + runtime::START_WAIT;
    let mut client = match connect_emit_daemon(identity, paths, deadline).await {
        Ok(client) => client,
        Err(message) => return fail(&message),
    };
    let mut reader = tokio::io::BufReader::new(tokio::io::stdin());
    let mut stdout = tokio::io::stdout();
    let max_line_bytes = events::MAX_EXTERNAL_EVENT_BYTES + 2;
    let mut line_buf = Vec::new();
    loop {
        let line_bytes = match read_bounded_line(&mut reader, &mut line_buf, max_line_bytes).await {
            Ok(BoundedLine::Eof) => break,
            Ok(BoundedLine::TooLong) => {
                let out = format!(
                    "{}\n",
                    serde_json::json!({
                        "ok": false,
                        "error": format!(
                            "event payload exceeds {} byte limit",
                            events::MAX_EXTERNAL_EVENT_BYTES
                        )
                    })
                );
                if stdout.write_all(out.as_bytes()).await.is_err() || stdout.flush().await.is_err()
                {
                    return 1;
                }
                continue;
            }
            Ok(BoundedLine::Line(bytes)) => bytes,
            Err(error) => return fail(&format!("cannot read stdin: {error}")),
        };
        let line_str = match std::str::from_utf8(&line_bytes) {
            Ok(s) => s,
            Err(_) => {
                let out = format!(
                    "{}\n",
                    serde_json::json!({ "ok": false, "error": "line is not valid UTF-8" })
                );
                if stdout.write_all(out.as_bytes()).await.is_err() || stdout.flush().await.is_err()
                {
                    return 1;
                }
                continue;
            }
        };
        let trimmed = line_str.trim_end_matches(['\r', '\n']);
        if trimmed.trim().is_empty() {
            continue;
        }
        let reply = match parse_emit_payload(
            trimmed,
            flags.source.as_deref(),
            flags.topic.as_deref(),
        ) {
            Err(message) => serde_json::json!({ "ok": false, "error": message }),
            Ok(event) => match send_emit_event(&mut client, event).await {
                Ok(ToEndpoint::EmitAck {
                    ok: true,
                    event_id,
                    cursor,
                    deduplicated,
                    ..
                }) => serde_json::json!({
                    "ok": true,
                    "eventId": event_id,
                    "cursor": cursor,
                    "deduplicated": deduplicated,
                }),
                Ok(ToEndpoint::EmitAck { error, .. }) => serde_json::json!({
                    "ok": false,
                    "error": error.map(|e| e.message).unwrap_or_else(|| "daemon rejected event".to_owned()),
                }),
                Ok(_) => {
                    serde_json::json!({ "ok": false, "error": "unexpected frame from daemon" })
                }
                Err(message) => {
                    let out = format!("{}\n", serde_json::json!({ "ok": false, "error": message }));
                    let _ = stdout.write_all(out.as_bytes()).await;
                    let _ = stdout.flush().await;
                    return 1;
                }
            },
        };
        let out = format!("{reply}\n");
        if stdout.write_all(out.as_bytes()).await.is_err() || stdout.flush().await.is_err() {
            return 1;
        }
    }
    let _ = ipc::write_frame_idle(
        &mut client,
        &ToDaemon::Bye,
        std::time::Duration::from_secs(1),
    )
    .await;
    0
}

pub(super) const NO_ANSWER: &str = "the daemon did not answer in time";

/// Runs `work` under what is left of `deadline`.
pub(super) async fn within<T>(
    deadline: std::time::Instant,
    work: impl Future<Output = Result<T, String>>,
) -> Result<T, String> {
    let left = deadline
        .checked_duration_since(std::time::Instant::now())
        .filter(|left| !left.is_zero())
        .ok_or_else(|| NO_ANSWER.to_owned())?;
    match tokio::time::timeout(left, work).await {
        Ok(result) => result,
        Err(_) => Err(NO_ANSWER.to_owned()),
    }
}

/// A separate thread keeps stalled file I/O from holding up Tokio runtime shutdown.
pub(super) async fn blocking_within<T: Send + 'static>(
    deadline: std::time::Instant,
    work: impl FnOnce() -> Result<T, String> + Send + 'static,
) -> Result<T, String> {
    within(deadline, async move {
        let (send, receive) = tokio::sync::oneshot::channel();
        std::thread::Builder::new()
            .name("instructions-check".into())
            .spawn(move || {
                let _ = send.send(work());
            })
            .map_err(|error| format!("cannot start instructions check: {error}"))?;
        receive
            .await
            .map_err(|error| format!("instructions check did not finish: {error}"))?
    })
    .await
}

async fn cli_start(
    identity: &runtime::Identity,
    paths: &runtime::Paths,
    launch: &LaunchContext,
    deadline: std::time::Instant,
    flags: DaemonFlags,
) -> i32 {
    let tools = match Tools::from_env() {
        Ok(tools) => tools,
        Err(message) => return fail(&message),
    };
    let public_root = if flags.http.is_some() || tools.transfer {
        match crate::config::public_root() {
            Ok(root) => root,
            Err(message) => return fail(&message),
        }
    } else {
        None
    };
    // A start that cannot serve HTTP or `/t/` is refused before anything runs, so it leaves no
    // daemon behind that the caller did not ask for.
    let mut extra: Vec<std::ffi::OsString> = Vec::new();
    if let Some(value) = &flags.http {
        if std::env::var_os(TRANSFER_ADDR).is_some() {
            return fail(&format!(
                "cannot set {TRANSFER_ADDR} with --http; --http already serves /t/"
            ));
        }
        if let Err(message) = http_addr(value) {
            return fail(&message);
        }
        if let Err(message) = http_token() {
            return fail(&message);
        }
        extra.push("--http".into());
        extra.push(value.into());
    } else if tools.transfer {
        let requested = match transfer_addr() {
            Ok(addr) => addr,
            Err(message) => return fail(&message),
        };
        if let Some(addr) = requested
            && let Err(message) = check_routable_addr(TRANSFER_ADDR, addr, public_root.is_some())
        {
            return fail(&message);
        }
    }
    if let Some(idle_exit) = flags.idle_exit {
        extra.push("--idle-exit".into());
        extra.push(if idle_exit { "on" } else { "off" }.into());
    }
    let (mut client, lock) = match within(deadline, connect_existing(paths)).await {
        Ok(Some(client)) => (client, None),
        Ok(None) => {
            if flags.http.is_some() {
                if let Err(message) = instructions_tool_mode() {
                    return fail(&message);
                }
                if let Err(message) = {
                    let launch = launch.for_session();
                    blocking_within(deadline, move || instructions_file(&launch)).await
                } {
                    return fail(&message);
                }
            }
            match runtime::connect_or_start(identity, paths, deadline, &extra).await {
                Ok(pair) => pair,
                Err(message) => return fail(&message),
            }
        }
        Err(message) => return fail(&message),
    };
    let hello = match hello(identity, launch) {
        Ok(hello) => hello,
        Err(message) => return fail(&message),
    };
    let epoch = match within(deadline, async {
        endpoint::handshake(&mut client, hello, identity)
            .await
            .map_err(|error| error.to_string())
    })
    .await
    {
        Ok(epoch) => epoch,
        Err(message) => return fail(&message),
    };
    // The Welcome arrived, so the start no longer needs to be exclusive.
    drop(lock);
    let status = match within(deadline, request_status(&mut client)).await {
        Ok(status) => status,
        Err(message) => return fail(&message),
    };
    if let Err(message) = within(deadline, bye(&mut client)).await {
        return fail(&message);
    }
    // A daemon that was already running keeps the answer it had: one that serves HTTP is used as
    // it is, and one that does not cannot be turned into one without a stop.
    if flags.http.is_some() && status.http.is_none() {
        return fail(&format!(
            "a daemon already runs for this namespace (epoch {epoch}, HTTP off); stop it first"
        ));
    }
    match &status.http {
        Some(addr) => println!(
            "fastexec daemon {epoch} pid {} namespace {} http {addr}",
            status.pid, identity.namespace
        ),
        None => println!(
            "fastexec daemon {epoch} pid {} namespace {}",
            status.pid, identity.namespace
        ),
    }
    0
}

async fn cli_status(
    identity: &runtime::Identity,
    paths: &runtime::Paths,
    launch: &LaunchContext,
    deadline: std::time::Instant,
) -> i32 {
    let mut client = match within(deadline, connect_existing(paths)).await {
        Ok(Some(client)) => client,
        Ok(None) => {
            println!("No fastexec daemon for namespace {}.", identity.namespace);
            return 0;
        }
        Err(message) => return fail(&message),
    };
    let status = match within(deadline, async {
        let hello = hello(identity, launch)?;
        endpoint::handshake(&mut client, hello, identity)
            .await
            .map_err(|error| error.to_string())?;
        request_status(&mut client).await
    })
    .await
    {
        Ok(status) => status,
        Err(message) => return fail(&message),
    };
    if let Err(message) = within(deadline, bye(&mut client)).await {
        return fail(&message);
    }
    println!("epoch {}", status.epoch);
    println!("build {}", status.build);
    println!("pid {}", status.pid);
    println!("sessions {}", status.sessions);
    println!("tasks {}", status.tasks);
    match &status.http {
        Some(addr) => println!("http {addr}"),
        None => println!("http off"),
    }
    0
}

async fn cli_stop(
    identity: &runtime::Identity,
    paths: &runtime::Paths,
    launch: &LaunchContext,
    deadline: std::time::Instant,
) -> i32 {
    let mut client = match within(deadline, connect_existing(paths)).await {
        Ok(Some(client)) => client,
        Ok(None) => {
            println!("No fastexec daemon for namespace {}.", identity.namespace);
            return 0;
        }
        Err(message) => return fail(&message),
    };
    let epoch = match within(deadline, async {
        let hello = hello(identity, launch)?;
        let epoch = endpoint::handshake(&mut client, hello, identity)
            .await
            .map_err(|error| error.to_string())?;
        ipc::write_request_frame(
            &mut client,
            &ToDaemon::Stop,
            lease::process_pool(),
            lease::control_pool(),
        )
        .await
        .map_err(|error| format!("cannot send Stop: {error}"))?;
        // The daemon ends every connection and exits, so this one ends too.
        read_to_end(&mut client).await?;
        Ok(epoch)
    })
    .await
    {
        Ok(epoch) => epoch,
        Err(message) => return fail(&message),
    };
    if !wait_for_exit(paths, deadline).await {
        return fail(&format!(
            "the daemon did not exit; see {}",
            paths.ns.join("daemon.log").display()
        ));
    }
    println!("Stopped fastexec daemon {epoch}.");
    0
}

/// Connects to the namespace's daemon without starting one. `Ok(None)` means none is there.
async fn connect_existing(paths: &runtime::Paths) -> Result<Option<transport::Client>, String> {
    match transport::connect(&paths.endpoint).await {
        Ok(client) => Ok(Some(client)),
        // Nothing listening is an answer, not a failure; anything else is reported as it is.
        Err(error) if runtime::absent(&error) => Ok(None),
        Err(error) => Err(format!(
            "cannot connect to {}: {error}",
            std::path::Path::new(&paths.endpoint).display()
        )),
    }
}

/// The Hello a CLI command sends: a session of its own, one connection, and the launch context.
fn hello(
    identity: &runtime::Identity,
    launch: &LaunchContext,
) -> Result<lease::Owned<ToDaemon>, String> {
    let session =
        id::random_chars(26).map_err(|error| format!("cannot name the session: {error}"))?;
    endpoint::hello_owned(launch, identity, &session, 1, "cli").map_err(|error| error.to_string())
}

/// Says Bye and reads until the daemon ends the connection, so the session is gone before the CLI
/// exits.
async fn bye(client: &mut transport::Client) -> Result<(), String> {
    ipc::write_request_frame(
        client,
        &ToDaemon::Bye,
        lease::process_pool(),
        lease::control_pool(),
    )
    .await
    .map_err(|error| format!("cannot send Bye: {error}"))?;
    read_to_end(client).await
}

async fn read_to_end(client: &mut transport::Client) -> Result<(), String> {
    loop {
        match ipc::read_endpoint_frame_leased(
            client,
            lease::FRAME_LIMIT,
            lease::process_pool(),
            lease::control_pool(),
            |_| false,
        )
        .await
        {
            Ok(ipc::LeasedEndpointFrame::Eof) => return Ok(()),
            Ok(_) => continue,
            Err(error) => return Err(format!("cannot read the daemon's answer: {error}")),
        }
    }
}

/// The daemon's state, as `Status` reports it.
struct DaemonStatus {
    epoch: String,
    build: String,
    pid: u32,
    sessions: usize,
    tasks: usize,
    /// The address it serves Streamable HTTP on, when it serves it.
    http: Option<String>,
    _owner: lease::Lease,
}

/// Asks the daemon for its state, after the Welcome.
async fn request_status(client: &mut transport::Client) -> Result<DaemonStatus, String> {
    ipc::write_request_frame(
        client,
        &ToDaemon::Status,
        lease::process_pool(),
        lease::control_pool(),
    )
    .await
    .map_err(|error| format!("cannot send Status: {error}"))?;
    match ipc::read_endpoint_frame_leased(
        client,
        lease::FRAME_LIMIT,
        lease::process_pool(),
        lease::control_pool(),
        |_| false,
    )
    .await
    {
        Ok(ipc::LeasedEndpointFrame::Frame(
            ToEndpoint::Status {
                epoch,
                build,
                pid,
                sessions,
                tasks,
                http,
            },
            owner,
        )) => Ok(DaemonStatus {
            epoch,
            build,
            pid,
            sessions,
            tasks,
            http,
            _owner: owner,
        }),
        Ok(other) => Err(format!("the daemon answered Status with {other:?}")),
        Err(error) => Err(format!("cannot read Status: {error}")),
    }
}

async fn wait_for_exit(paths: &runtime::Paths, deadline: std::time::Instant) -> bool {
    let path = paths.ns.join(runtime::DAEMON_LOCK);
    while std::time::Instant::now() < deadline {
        if matches!(runtime::Lock::try_lock(&path), Ok(Some(_))) {
            return true;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    false
}
