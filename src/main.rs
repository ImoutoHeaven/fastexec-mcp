//! MCP stdio server: the rmcp adapter over the service, and the process lifecycle around it.

mod bash;
mod conpty;
#[cfg(windows)]
mod conpty_spawn;
mod daemon;
mod endpoint;
mod files;
mod frames;
#[cfg(unix)]
mod host;
mod http;
mod images;
mod ipc;
mod keys;
mod output;
mod process;
mod runtime;
mod service;
mod session;
mod tasks;
mod terminal;
mod transfer;
mod transport;
#[cfg(target_os = "macos")]
mod tree_macos;
mod wait;

use ipc::{ToDaemon, ToEndpoint};
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, ContentBlock, Implementation, ServerCapabilities, ServerConfig};
use rmcp::service::RequestContext;
use rmcp::{RoleServer, ServerHandler, ServiceExt, tool, tool_handler, tool_router};
use service::{Call, Render, Service};
use session::{LaunchContext, Session};
use std::future::Future;
use std::sync::{Arc, OnceLock};
use tasks::{Limits, Tasks};
use tokio::sync::watch;

#[derive(Clone)]
struct Server {
    backend: Backend,
    /// This server's own routes: the tool list and the description of `exec` are built per
    /// server, because what they say about the host differs between servers.
    router: ToolRouter<Server>,
    /// The files this server publishes under `/t/`, for an HTTP session only. Its absence is what
    /// removes the `transfer` route from a stdio session.
    transfers: Option<Arc<transfer::Transfers>>,
    /// The base a `transfer` URL is minted from when a proxy names it at daemon start. `None` means
    /// each request's Host and `X-Forwarded-Proto` are the address the client saw.
    public_root: Option<Arc<str>>,
    /// Sessionless HTTP calls share a reader under the listener's single bearer credential.
    stateless_session: Option<Arc<Session>>,
}

/// Where this server's calls run: in this process, or on the namespace's daemon.
#[derive(Clone)]
enum Backend {
    Local {
        service: Arc<Service>,
        render: Render,
        /// Created on the first tool call, when the client's name is known.
        session: Arc<OnceLock<Result<Arc<Session>, String>>>,
        /// What this server's tasks inherit: the environment captured at startup, so a later
        /// change to the server's own environment cannot reach them.
        launch: Arc<LaunchContext>,
        /// What this server tells a client about where its tasks live.
        instructions: &'static str,
    },
    /// `on` mode: the daemon owns every task, and this process only carries calls to it.
    Daemon(Arc<endpoint::Endpoint>),
}

impl Backend {
    fn instructions(&self) -> &'static str {
        match self {
            Self::Local { instructions, .. } => instructions,
            Self::Daemon(_) => INSTRUCTIONS_DAEMON,
        }
    }
}

impl Server {
    /// A server over `backend`, whose `exec` tool says where its tasks run.
    fn new(
        backend: Backend,
        launch: &LaunchContext,
        transfers: Option<Arc<transfer::Transfers>>,
        public_root: Option<Arc<str>>,
    ) -> Self {
        let mut router = Self::tool_router();
        let exec = router
            .map
            .get_mut("exec")
            .expect("the generated router has the exec route");
        exec.attr.description = Some(exec_description(launch).into());
        if transfers.is_none() {
            // A stdio session has no route to serve a URL from, so it never sees the tool.
            router.remove_route("transfer");
        }
        Self {
            backend,
            router,
            transfers,
            public_root,
            stateless_session: None,
        }
    }

    /// This server's session: the client name and a short random ID, its tasks' owner.
    fn session(&self, context: &RequestContext<RoleServer>) -> Result<Arc<Session>, String> {
        if context
            .protocol_version()
            .is_some_and(|version| version >= rmcp::model::ProtocolVersion::V_2026_07_28)
            && let Some(session) = &self.stateless_session
        {
            return Ok(Arc::clone(session));
        }
        let Backend::Local {
            session, launch, ..
        } = &self.backend
        else {
            return Err("this server has no tasks of its own".to_owned());
        };
        session
            .get_or_init(|| {
                let short = tasks::random_chars(6)
                    .map_err(|error| format!("Cannot name the session: {error}"))?;
                Ok(Session::new(
                    short.clone(),
                    format!("{}-{short}", client_name(context)),
                    Arc::clone(launch),
                ))
            })
            .clone()
    }
}

/// The client's name from request metadata or the legacy handshake.
fn client_name(context: &RequestContext<RoleServer>) -> String {
    context
        .client_info()
        .map_or_else(|| "client".to_string(), |info| info.name)
}

/// Server instructions say when to use the tool; hosts that show one line take the first. The
/// second line says where the tasks live, which is the one thing that differs between the modes.
const INSTRUCTIONS_LOCAL: &str = "Run bash tasks with exec and read, write, and edit files on this host.
Prefer it over one-shot shell tools when a command may outlive a single call or needs input; tasks end when the session ends.";
const INSTRUCTIONS_DAEMON: &str = "Run bash tasks with exec and read, write, and edit files on this host.
Prefer it over one-shot shell tools when a command may outlive a single call or needs input; tasks run in a shared background daemon, outlive this session, and are visible to every session using it.";

/// The `exec` tool's description: the static text, the Windows paragraph on Windows, and one
/// line saying where this server's tasks run. Nothing in it changes while the server runs.
fn exec_description(launch: &LaunchContext) -> String {
    let mut description = include_str!("description.md").trim_end().to_owned();
    if cfg!(windows) {
        description.push_str("\n\n");
        description.push_str(include_str!("description-windows.md").trim_end());
    }
    if cfg!(target_os = "macos") {
        description.push_str("\n\n");
        description.push_str(include_str!("description-macos.md").trim_end());
    }
    description.push_str("\n\nHost: ");
    description.push_str(&host_line(launch));
    description
}

/// `os arch · hostname · user <user> · home <home> · default cwd <cwd>`, with `unknown` for a
/// value the launch environment does not hold.
fn host_line(launch: &LaunchContext) -> String {
    let value = |name: &str| {
        launch
            .var(name)
            .filter(|value| !value.is_empty())
            .map(|value| value.to_string_lossy().into_owned())
    };
    let hostname = hostname(launch).unwrap_or_else(|| "unknown".to_owned());
    let user = value("USER")
        .or_else(|| value("USERNAME"))
        .unwrap_or_else(|| "unknown".to_owned());
    let home = launch
        .home()
        .map_or_else(|| "unknown".to_owned(), |home| home.display().to_string());
    format!(
        "{} {} · {hostname} · user {user} · home {home} · default cwd {}",
        std::env::consts::OS,
        std::env::consts::ARCH,
        launch.cwd.display()
    )
}

/// This host's name: the launch environment's `COMPUTERNAME` on Windows, the kernel's answer on
/// Unix.
#[cfg(windows)]
fn hostname(launch: &LaunchContext) -> Option<String> {
    launch
        .var("COMPUTERNAME")
        .filter(|value| !value.is_empty())
        .map(|value| value.to_string_lossy().into_owned())
}

#[cfg(unix)]
fn hostname(launch: &LaunchContext) -> Option<String> {
    let _ = launch;
    let mut name = [0u8; 256];
    // SAFETY: `name` is writable for the length passed, as `gethostname` requires.
    let answer = unsafe { libc::gethostname(name.as_mut_ptr().cast(), name.len()) };
    if answer != 0 {
        return None;
    }
    let end = name.iter().position(|byte| *byte == 0)?;
    String::from_utf8(name[..end].to_vec()).ok()
}

#[tool_handler(router = self.router)]
impl ServerHandler for Server {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("fastexec", env!("CARGO_PKG_VERSION")))
            .with_instructions(self.backend.instructions())
    }
}

#[tool_router]
impl Server {
    #[doc = include_str!("description.md")]
    #[tool(
        name = "exec",
        annotations(
            title = "Run bash command",
            read_only_hint = false,
            destructive_hint = true,
            open_world_hint = true
        )
    )]
    async fn exec(
        &self,
        Parameters(request): Parameters<service::Request>,
        context: RequestContext<RoleServer>,
    ) -> CallToolResult {
        match &self.backend {
            Backend::Daemon(endpoint) => {
                let (progress, _forwarder) = progress(&context);
                // The daemon validates the call again; this is only how it travels.
                let arguments = match serde_json::to_value(&request) {
                    Ok(arguments) => arguments,
                    Err(problem) => {
                        return CallToolResult::error(vec![rmcp::model::ContentBlock::text(
                            format!("cannot serialize the call: {problem}"),
                        )]);
                    }
                };
                endpoint
                    .call(
                        "exec",
                        arguments,
                        context.id.to_string(),
                        &client_name(&context),
                        context.ct.clone(),
                        progress,
                    )
                    .await
            }
            Backend::Local {
                service,
                render,
                session: _,
                launch: _,
                instructions: _,
            } => {
                let result = match self.session(&context) {
                    Ok(session) => {
                        let (progress, _forwarder) = progress(&context);
                        let call = Call {
                            session: &session,
                            render: *render,
                            cancel: context.ct.clone(),
                            progress,
                            call_id: None,
                        };
                        service.call(request, call).await
                    }
                    Err(message) => service::error_result(request.action, &message),
                };
                service::suppress(result, *render)
            }
        }
    }

    #[doc = "Read the contents of a file. Supports text files and images (jpg, png, gif, webp, bmp). Images are sent as attachments. For text files, output is truncated to 2000 lines or 50KB (whichever is hit first). Use offset/limit for large files. When you need the full file, continue with offset until complete."]
    #[tool(name = "read", annotations(title = "Read file", read_only_hint = true))]
    async fn read(
        &self,
        Parameters(request): Parameters<files::ReadRequest>,
        context: RequestContext<RoleServer>,
    ) -> CallToolResult {
        match &self.backend {
            Backend::Local { launch, .. } => {
                files::read(request, Arc::clone(launch), self.transfers.is_some()).await
            }
            Backend::Daemon(endpoint) => forward(endpoint, "read", &request, &context).await,
        }
    }

    #[doc = "Write content to a file. Creates the file if it doesn't exist, overwrites if it does. Automatically creates parent directories."]
    #[tool(
        name = "write",
        annotations(title = "Write file", destructive_hint = true, idempotent_hint = false)
    )]
    async fn write(
        &self,
        Parameters(request): Parameters<files::WriteRequest>,
        context: RequestContext<RoleServer>,
    ) -> CallToolResult {
        match &self.backend {
            Backend::Local { launch, .. } => files::write(request, Arc::clone(launch)).await,
            Backend::Daemon(endpoint) => forward(endpoint, "write", &request, &context).await,
        }
    }

    #[doc = "Edit a single file using exact text replacement. Every edits[].oldText must match a unique, non-overlapping region of the original file. If two changes affect the same block or nearby lines, merge them into one edit instead of emitting overlapping edits. Do not include large unchanged regions just to connect distant changes."]
    #[tool(
        name = "edit",
        annotations(title = "Edit file", destructive_hint = true, idempotent_hint = false)
    )]
    async fn edit(
        &self,
        Parameters(request): Parameters<files::EditRequest>,
        context: RequestContext<RoleServer>,
    ) -> CallToolResult {
        match &self.backend {
            Backend::Local { launch, .. } => files::edit(request, Arc::clone(launch)).await,
            Backend::Daemon(endpoint) => forward(endpoint, "edit", &request, &context).await,
        }
    }

    #[doc = "Publish one file at a URL that serves it for GET (download), HEAD (current size) and PUT (upload). The URL stops working after 10 minutes unused. Pack a directory with tar through exec first. Start every new upload with `curl -fsS -X PUT --data-binary '' \"$url?offset=0\"`, then `curl -fsS -T file \"$url\"`; on HTTP 413 or a broken connection, read the uploaded size with `curl -fsSI \"$url\"` and send the rest of the same unchanged file in parts, one at a time: `tail -c +$((off+1)) file | head -c 1048576 | curl -fsS -T - \"$url?offset=$off\"`. Download with `curl -fsS -o file \"$url\"`, resume with `curl -fsS -C - -o file \"$url\"`, and verify with sha256sum on both ends."]
    #[tool(
        name = "transfer",
        annotations(
            title = "Transfer file",
            destructive_hint = true,
            open_world_hint = true
        )
    )]
    async fn transfer(
        &self,
        Parameters(request): Parameters<transfer::Request>,
        context: RequestContext<RoleServer>,
    ) -> CallToolResult {
        let Some(table) = &self.transfers else {
            return CallToolResult::error(vec![ContentBlock::text(
                "transfer needs an HTTP session, and this one is stdio.",
            )]);
        };
        let Backend::Local { launch, render, .. } = &self.backend else {
            return CallToolResult::error(vec![ContentBlock::text(
                "transfer needs an HTTP session, and this one is stdio.",
            )]);
        };
        let path = match files::resolve(&request.path, launch) {
            Ok(path) => path,
            Err(message) => return CallToolResult::error(vec![ContentBlock::text(message)]),
        };
        let url = match transfer_url(path, table, &context, self.public_root.as_deref()) {
            Ok(url) => url,
            Err(message) => return CallToolResult::error(vec![ContentBlock::text(message)]),
        };
        let mut result = CallToolResult::success(vec![ContentBlock::text(url.clone())]);
        result.structured_content = Some(serde_json::json!({ "url": url }));
        service::suppress(result, *render)
    }
}

/// The URL a transfer publishes: the public root the daemon was started with, or else this
/// listener as the client reached it, and a fresh id.
fn transfer_url(
    path: std::path::PathBuf,
    table: &Arc<transfer::Transfers>,
    context: &RequestContext<RoleServer>,
    public_root: Option<&str>,
) -> Result<String, String> {
    let Some(parts) = context.extensions.get::<::http::request::Parts>() else {
        return Err("Cannot make a transfer URL outside an HTTP request.".to_owned());
    };
    // A named root is the whole address a proxy publishes, so the request's own Host says nothing;
    // without one, the request's Host over the scheme it was forwarded as is the client's address.
    let base = match public_root {
        Some(root) => root.to_owned(),
        None => inferred_base(&parts.headers)?,
    };
    let id = table.create(path)?;
    Ok(format!("{base}t/{id}"))
}

/// The base inferred from a request: its Host, over the scheme the client saw.
fn inferred_base(headers: &::http::HeaderMap) -> Result<String, String> {
    let host = headers
        .get(::http::header::HOST)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| "Cannot make a transfer URL: the request names no Host.".to_owned())?;
    let authority: ::http::uri::Authority = host
        .parse()
        .map_err(|_| format!("Cannot make a transfer URL: {host} is not a host."))?;
    let scheme = forwarded_scheme(headers);
    Ok(format!("{scheme}://{authority}/"))
}

/// The scheme the client saw: the first `X-Forwarded-Proto` token when it names one of the two.
fn forwarded_scheme(headers: &::http::HeaderMap) -> &'static str {
    let token = headers
        .get("x-forwarded-proto")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(',').next())
        .map(str::trim);
    match token {
        Some(token) if token.eq_ignore_ascii_case("https") => "https",
        Some(token) if token.eq_ignore_ascii_case("http") => "http",
        _ => "http",
    }
}

/// Sends a file tool's call to the daemon, which runs it with the session's launch context.
async fn forward<T: serde::Serialize>(
    endpoint: &endpoint::Endpoint,
    tool: &str,
    request: &T,
    context: &RequestContext<RoleServer>,
) -> CallToolResult {
    let arguments = match serde_json::to_value(request) {
        Ok(arguments) => arguments,
        Err(problem) => {
            return CallToolResult::error(vec![rmcp::model::ContentBlock::text(format!(
                "cannot serialize the call: {problem}"
            ))]);
        }
    };
    let (progress, _forwarder) = progress(context);
    endpoint
        .call(
            tool,
            arguments,
            context.id.to_string(),
            &client_name(context),
            context.ct.clone(),
            progress,
        )
        .await
}

/// Aborts the progress forwarder however the call returns, including through `?`.
struct Forwarder(tokio::task::JoinHandle<()>);

impl Drop for Forwarder {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Sends each latest progress message. A transport that stalls only delays these; the call keeps
/// watching its own futures, so completion and cancellation never queue behind progress.
async fn forward_progress(
    peer: rmcp::service::Peer<RoleServer>,
    token: rmcp::model::ProgressToken,
    mut latest: watch::Receiver<Option<wait::Progress>>,
) {
    while latest.changed().await.is_ok() {
        // The watch borrow ends before the send awaits, so this future stays `Send`.
        let next = latest.borrow_and_update().clone();
        let Some((seconds, message)) = next else {
            continue;
        };
        let progress = rmcp::model::ProgressNotificationParam::new(token.clone(), seconds)
            .with_message(message);
        if peer.notify_progress(progress).await.is_err() {
            return;
        }
    }
}

/// The sink the call replaces with its latest progress message, when the client asked for
/// progress; the forwarder sends them.
fn progress(
    context: &RequestContext<RoleServer>,
) -> (
    Option<watch::Sender<Option<wait::Progress>>>,
    Option<Forwarder>,
) {
    let Some(token) = context.meta.get_progress_token() else {
        return (None, None);
    };
    let (sender, latest) = watch::channel(None);
    let forwarder = Forwarder(tokio::spawn(forward_progress(
        context.peer.clone(),
        token,
        latest,
    )));
    (Some(sender), Some(forwarder))
}

fn main() {
    let args: Vec<std::ffi::OsString> = std::env::args_os().skip(1).collect();
    #[cfg(unix)]
    {
        if args.first().is_some_and(|arg| arg == host::FLAG) {
            host::main(&args[1..]);
        }
    }
    if args.first().is_some_and(|arg| arg == "daemon") {
        std::process::exit(daemon_main(&args[1..]));
    }
    serve();
}

/// The hidden `daemon` subcommand: the daemon process itself, and the CLI that starts, inspects,
/// and stops one.
fn daemon_main(args: &[std::ffi::OsString]) -> i32 {
    let action = args
        .first()
        .map(|arg| arg.to_string_lossy().into_owned())
        .unwrap_or_default();
    match action.as_str() {
        "serve" => {
            let (Some(namespace), Some(build)) = (args.get(1), args.get(2)) else {
                eprintln!(
                    "fastexec: usage: fastexec daemon serve <namespace> <build> [--http <addr>] [--idle-exit on|off]"
                );
                return 2;
            };
            let flags = match daemon_flags(&args[3..]) {
                Ok(flags) => flags,
                Err(message) => return fail(&message),
            };
            daemon::serve_process(
                namespace.to_string_lossy().into_owned(),
                build.to_string_lossy().into_owned(),
                flags.http,
                flags.idle_exit.unwrap_or_default(),
                false,
            )
        }
        // The daemon in this process, in the foreground, until it is stopped: the supervisor that
        // started it owns its lifetime, so it never detaches and never reuses a running daemon.
        "run" => {
            let flags = match daemon_flags(&args[1..]) {
                Ok(flags) => flags,
                Err(message) => return fail(&message),
            };
            // A foreground daemon serves until it is stopped, so there is no idleness to exit on.
            if flags.idle_exit.is_some() {
                return fail(DAEMON_USAGE);
            }
            cli_run(flags)
        }
        "start" | "status" | "stop" => {
            let flags = match daemon_flags(&args[1..]) {
                Ok(flags) => flags,
                Err(message) => return fail(&message),
            };
            if (flags.http.is_some() || flags.idle_exit.is_some()) && action != "start" {
                return fail(DAEMON_USAGE);
            }
            match tokio::runtime::Runtime::new() {
                Ok(runtime) => {
                    let code = runtime.block_on(cli(&action, flags));
                    // Exit here rather than through the runtime's drop, which would wait for a
                    // start worker the deadline abandoned.
                    std::process::exit(code);
                }
                Err(error) => fail(&format!("cannot start the runtime: {error}")),
            }
        }
        _ => {
            eprintln!("fastexec: {DAEMON_USAGE}");
            2
        }
    }
}

const DAEMON_USAGE: &str = "usage: fastexec daemon start [--http <addr>] [--idle-exit on|off] | run [--http <addr>] | status | stop";

#[derive(Default)]
struct DaemonFlags {
    http: Option<String>,
    idle_exit: Option<bool>,
}

fn daemon_flags(args: &[std::ffi::OsString]) -> Result<DaemonFlags, String> {
    let mut flags = DaemonFlags::default();
    let (pairs, rest) = args.as_chunks::<2>();
    for pair in pairs {
        if pair[0] == "--http" && flags.http.is_none() {
            flags.http = Some(pair[1].to_string_lossy().into_owned());
        } else if pair[0] == "--idle-exit" && flags.idle_exit.is_none() {
            flags.idle_exit = Some(match pair[1].to_str() {
                Some("on") => true,
                Some("off") => false,
                _ => return Err("--idle-exit must be on or off".to_owned()),
            });
        } else {
            return Err(DAEMON_USAGE.to_owned());
        }
    }
    if !rest.is_empty() {
        return Err(DAEMON_USAGE.to_owned());
    }
    Ok(flags)
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
    let identity = match runtime::identity(&launch) {
        Ok(identity) => identity,
        Err(error) => return fail(&format!("cannot identify this image: {error}")),
    };
    let paths = match runtime::paths(&identity.namespace) {
        Ok(paths) => paths,
        Err(error) => return fail(&format!("cannot use the runtime directory: {error}")),
    };
    // One deadline for the whole command, so a daemon that stops answering cannot hold the CLI.
    let deadline = std::time::Instant::now() + runtime::START_WAIT;
    match action {
        "start" => cli_start(&identity, &paths, &launch, deadline, flags).await,
        "status" => cli_status(&identity, &paths, &launch, deadline).await,
        _ => cli_stop(&identity, &paths, &launch, deadline).await,
    }
}

const NO_ANSWER: &str = "the daemon did not answer in time";

/// Runs `work` under what is left of `deadline`.
async fn within<T>(
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

async fn cli_start(
    identity: &runtime::Identity,
    paths: &runtime::Paths,
    launch: &LaunchContext,
    deadline: std::time::Instant,
    flags: DaemonFlags,
) -> i32 {
    // A start that cannot serve HTTP is refused before anything runs, so it leaves no daemon
    // behind that the caller did not ask for.
    let mut extra: Vec<std::ffi::OsString> = Vec::new();
    if let Some(value) = &flags.http {
        if let Err(message) = http_addr(value) {
            return fail(&message);
        }
        if let Err(message) = http_token() {
            return fail(&message);
        }
        // A root that cannot be used is refused here too, so the terminal gets the reason and no
        // daemon is left behind to fail binding for it.
        if let Err(message) = http::public_root() {
            return fail(&message);
        }
        extra.push("--http".into());
        extra.push(value.into());
    }
    if let Some(idle_exit) = flags.idle_exit {
        extra.push("--idle-exit".into());
        extra.push(if idle_exit { "on" } else { "off" }.into());
    }
    let (mut client, lock) =
        match runtime::connect_or_start(identity, paths, deadline, &extra).await {
            Ok(pair) => pair,
            Err(message) => return fail(&message),
        };
    let hello = match hello(identity, launch) {
        Ok(hello) => hello,
        Err(message) => return fail(&message),
    };
    let epoch = match within(deadline, endpoint::handshake(&mut client, hello, identity)).await {
        Ok(epoch) => epoch,
        Err(message) => return fail(&message),
    };
    // The Welcome arrived, so the start no longer needs to be exclusive.
    drop(lock);
    let status = match within(deadline, request_status(&mut client)).await {
        Ok(status) => status,
        Err(message) => return fail(&message),
    };
    // A daemon that was already running keeps the answer it had: one that serves HTTP is used as
    // it is, and one that does not cannot be turned into one without a stop.
    if flags.http.is_some() && status.http.is_none() {
        return fail(&format!(
            "a daemon already runs for this namespace (epoch {epoch}, HTTP off); stop it first"
        ));
    }
    if let Err(message) = within(deadline, bye(&mut client)).await {
        return fail(&message);
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
    let mut client = match connect_existing(paths).await {
        Ok(Some(client)) => client,
        Ok(None) => {
            println!("No fastexec daemon for namespace {}.", identity.namespace);
            return 0;
        }
        Err(message) => return fail(&message),
    };
    let status = match within(deadline, async {
        let hello = hello(identity, launch)?;
        endpoint::handshake(&mut client, hello, identity).await?;
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
    let mut client = match connect_existing(paths).await {
        Ok(Some(client)) => client,
        Ok(None) => {
            println!("No fastexec daemon for namespace {}.", identity.namespace);
            return 0;
        }
        Err(message) => return fail(&message),
    };
    let epoch = match within(deadline, async {
        let hello = hello(identity, launch)?;
        let epoch = endpoint::handshake(&mut client, hello, identity).await?;
        ipc::write_frame(&mut client, &ToDaemon::Stop)
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
fn hello(identity: &runtime::Identity, launch: &LaunchContext) -> Result<ToDaemon, String> {
    let session =
        tasks::random_chars(26).map_err(|error| format!("cannot name the session: {error}"))?;
    let (cwd, env) = launch.to_wire();
    Ok(ToDaemon::Hello {
        protocol: ipc::PROTOCOL,
        namespace: identity.namespace.clone(),
        build: identity.build.clone(),
        session,
        connection: 1,
        client_name: "cli".to_owned(),
        output_mode: "text".to_owned(),
        structured_content: true,
        cwd,
        env,
    })
}

/// Says Bye and reads until the daemon ends the connection, so the session is gone before the CLI
/// exits.
async fn bye(client: &mut transport::Client) -> Result<(), String> {
    ipc::write_frame(client, &ToDaemon::Bye)
        .await
        .map_err(|error| format!("cannot send Bye: {error}"))?;
    read_to_end(client).await
}

async fn read_to_end(client: &mut transport::Client) -> Result<(), String> {
    loop {
        match ipc::read_frame::<_, ToEndpoint>(client, ipc::RESPONSE_LIMIT).await {
            Ok(Some(_)) => continue,
            Ok(None) => return Ok(()),
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
}

/// Asks the daemon for its state, after the Welcome.
async fn request_status(client: &mut transport::Client) -> Result<DaemonStatus, String> {
    ipc::write_frame(client, &ToDaemon::Status)
        .await
        .map_err(|error| format!("cannot send Status: {error}"))?;
    match ipc::read_frame::<_, ToEndpoint>(client, ipc::RESPONSE_LIMIT).await {
        Ok(Some(ToEndpoint::Status {
            epoch,
            build,
            pid,
            sessions,
            tasks,
            http,
        })) => Ok(DaemonStatus {
            epoch,
            build,
            pid,
            sessions,
            tasks,
            http,
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

/// The output mode `FASTEXEC_OUTPUT_MODE` names, or the message saying what it held instead.
pub fn output_mode() -> Result<service::OutputMode, String> {
    match std::env::var("FASTEXEC_OUTPUT_MODE").as_deref() {
        Err(std::env::VarError::NotPresent) | Ok("text") => Ok(service::OutputMode::Text),
        Ok("both") => Ok(service::OutputMode::Both),
        Ok("structured") => Ok(service::OutputMode::Structured),
        value => Err(format!(
            "invalid FASTEXEC_OUTPUT_MODE {value:?}; expected text, both, or structured"
        )),
    }
}

/// What `FASTEXEC_STRUCTURED_CONTENT` asks for, or the message saying what it held instead.
pub fn structured_content() -> Result<bool, String> {
    match std::env::var("FASTEXEC_STRUCTURED_CONTENT").as_deref() {
        Err(std::env::VarError::NotPresent) | Ok("true") => Ok(true),
        Ok("false") => Ok(false),
        value => Err(format!(
            "invalid FASTEXEC_STRUCTURED_CONTENT {value:?}; expected true or false"
        )),
    }
}

/// How this process renders results: both settings, or the message naming the one that is wrong.
pub fn render() -> Result<Render, String> {
    Ok(Render {
        output_mode: output_mode()?,
        structured_content: structured_content()?,
    })
}

/// The token a `--http` daemon admits requests with: at least 16 visible ASCII characters, from
/// the environment. A token an `Authorization` header cannot carry exactly is refused here rather
/// than starting a daemon nobody can reach.
pub fn http_token() -> Result<String, String> {
    const MESSAGE: &str = "FASTEXEC_HTTP_TOKEN must be at least 16 visible ASCII characters";
    match std::env::var("FASTEXEC_HTTP_TOKEN") {
        Ok(token)
            if token.len() >= 16 && token.bytes().all(|byte| (0x21..=0x7e).contains(&byte)) =>
        {
            Ok(token)
        }
        Ok(_) | Err(_) => Err(MESSAGE.to_owned()),
    }
}

/// The address a `--http` argument names, refused when it is outside loopback and serving there
/// was not allowed.
pub fn http_addr(value: &str) -> Result<std::net::SocketAddr, String> {
    let addr: std::net::SocketAddr = value
        .parse()
        .map_err(|_| format!("--http {value} is not an address (host:port)"))?;
    if !addr.ip().is_loopback() && !http::remote_allowed() {
        return Err(format!(
            "--http {value} is not a loopback address; set FASTEXEC_HTTP_ALLOW_REMOTE=1 only behind a TLS-terminating proxy"
        ));
    }
    Ok(addr)
}

#[tokio::main]
async fn serve() {
    let render = match render() {
        Ok(render) => render,
        Err(message) => {
            eprintln!("fastexec: {message}");
            std::process::exit(1);
        }
    };
    // Captured once, at startup: the client's own environment is the server's own here.
    let launch = match LaunchContext::current() {
        Ok(launch) => Arc::new(launch),
        Err(error) => {
            eprintln!("fastexec: cannot read the launch context: {error}");
            std::process::exit(1);
        }
    };
    // `on` forwards every call to the namespace's daemon and keeps no tasks of its own.
    let daemon = match std::env::var("FASTEXEC_DAEMON") {
        Err(std::env::VarError::NotPresent) => false,
        Ok(value) if value == "off" => false,
        Ok(value) if value == "on" => true,
        Ok(value) => {
            eprintln!("fastexec: invalid FASTEXEC_DAEMON {value:?}; expected on or off");
            std::process::exit(1);
        }
        Err(error) => {
            eprintln!("fastexec: invalid FASTEXEC_DAEMON {error}; expected on or off");
            std::process::exit(1);
        }
    };
    let (backend, tasks) = if daemon {
        let endpoint = match endpoint::Endpoint::new(Arc::clone(&launch), render) {
            Ok(endpoint) => endpoint,
            Err(error) => {
                eprintln!("fastexec: cannot name this endpoint's session: {error}");
                std::process::exit(1);
            }
        };
        (Backend::Daemon(Arc::new(endpoint)), None)
    } else {
        let tasks = match Tasks::new(Limits::OFF) {
            Ok(tasks) => tasks,
            Err(error) => {
                eprintln!("fastexec: cannot create the log directory: {error}");
                std::process::exit(1);
            }
        };
        let backend = Backend::Local {
            service: Arc::new(Service::new(Arc::clone(&tasks), false)),
            render,
            session: Arc::new(OnceLock::new()),
            launch: Arc::clone(&launch),
            instructions: INSTRUCTIONS_LOCAL,
        };
        (backend, Some(tasks))
    };
    let server = Server::new(backend, &launch, None, None);
    let eof = Arc::new(tokio::sync::Notify::new());
    let stdin = EofSignal {
        inner: tokio::io::stdin(),
        eof: Arc::clone(&eof),
    };
    // Signals are watched from the start, so an exit during initialization still cleans up.
    let signal = shutdown_signal();
    tokio::pin!(signal);
    // The backend outlives the serve call, which consumes the handler.
    let backend = server.backend.clone();
    let served = tokio::select! {
        served = server.serve((stdin, tokio::io::stdout())) => Some(served),
        _ = &mut signal => None,
    };
    match served {
        Some(Ok(service)) => {
            // Stdin EOF ends the session at once; rmcp would first drain in-flight waits.
            tokio::select! {
                _ = service.waiting() => {}
                _ = eof.notified() => {}
                _ = &mut signal => {}
            }
        }
        Some(Err(error)) => eprintln!("fastexec: MCP initialization failed: {error}"),
        None => {}
    }
    match &backend {
        // The endpoint never kills tasks: they belong to the daemon, and other sessions may be
        // using them.
        Backend::Daemon(endpoint) => endpoint.bye().await,
        Backend::Local { .. } => {
            if let Some(tasks) = tasks {
                tasks.shutdown();
            }
        }
    }
    std::process::exit(0);
}

/// Stdin wrapper that reports EOF: the host closing stdin is the MCP stdio shutdown signal.
struct EofSignal<R> {
    inner: R,
    eof: Arc<tokio::sync::Notify>,
}

impl<R: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for EofSignal<R> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let result = std::pin::Pin::new(&mut self.inner).poll_read(cx, buf);
        if matches!(result, std::task::Poll::Ready(Ok(()))) && buf.filled().len() == before {
            self.eof.notify_one();
        }
        result
    }
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
        let mut hangup = signal(SignalKind::hangup()).expect("SIGHUP handler");
        tokio::select! {
            _ = term.recv() => {}
            _ = hangup.recv() => {}
            _ = tokio::signal::ctrl_c() => {}
        }
    }
    #[cfg(windows)]
    let _ = tokio::signal::ctrl_c().await;
}

#[cfg(test)]
mod daemon_flag_tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<DaemonFlags, String> {
        daemon_flags(
            &args
                .iter()
                .map(std::ffi::OsString::from)
                .collect::<Vec<_>>(),
        )
    }

    #[test]
    fn idle_exit_is_opt_in_and_combines_with_http_in_either_order() {
        assert!(!parse(&[]).unwrap().idle_exit.unwrap_or_default());
        assert_eq!(
            parse(&["--idle-exit", "off"]).unwrap().idle_exit,
            Some(false)
        );
        assert_eq!(parse(&["--idle-exit", "on"]).unwrap().idle_exit, Some(true));
        for args in [
            ["--http", "127.0.0.1:0", "--idle-exit", "on"],
            ["--idle-exit", "on", "--http", "127.0.0.1:0"],
        ] {
            let flags = parse(&args).unwrap();
            assert_eq!(flags.http.as_deref(), Some("127.0.0.1:0"));
            assert_eq!(flags.idle_exit, Some(true));
        }
    }

    #[test]
    fn malformed_or_repeated_daemon_flags_are_rejected() {
        for args in [
            vec!["--idle-exit"],
            vec!["--idle-exit", "true"],
            vec!["--idle-exit", "on", "--idle-exit", "off"],
            vec!["--http", "127.0.0.1:0", "--http", "127.0.0.1:1"],
            vec!["--unknown", "off"],
        ] {
            assert!(parse(&args).is_err(), "accepted {args:?}");
        }
    }
}
