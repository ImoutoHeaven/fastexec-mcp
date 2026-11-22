//! MCP stdio server: the rmcp adapter over the service, and the process lifecycle around it.

mod bash;
mod codemode;
mod conpty;
#[cfg(windows)]
mod conpty_spawn;
mod daemon;
mod endpoint;
mod events;
mod files;
mod frames;
#[cfg(target_os = "freebsd")]
mod freebsd_fs;
#[cfg(unix)]
mod host;
mod http;
mod ingress;
mod ipc;
mod keys;
mod lease;
mod media;
mod output;
mod process;
mod runtime;
mod service;
mod session;
mod tasks;
mod terminal;
mod transfer;
mod transport;
#[cfg(target_os = "freebsd")]
mod tree_freebsd;
#[cfg(target_os = "linux")]
mod tree_linux;
#[cfg(target_os = "macos")]
mod tree_macos;
#[cfg(unix)]
mod unix_pty;
mod wait;

use ipc::{ToDaemon, ToEndpoint};
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{
    CallToolResult, ContentBlock, Implementation, InitializeRequestParams, InitializeResult,
    ServerCapabilities, ServerConfig, ToolAnnotations,
};
use rmcp::service::RequestContext;
use rmcp::{RoleServer, ServerHandler, ServiceExt, tool, tool_handler};
use service::{Call, Render, Service};
use session::{LaunchContext, Session};
use std::future::Future;
use std::sync::{Arc, OnceLock};
use tasks::{Limits, Tasks};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

/// Optional fixed `host:port` listener address for standalone `/t/` transfers.
pub const TRANSFER_ADDR: &str = "FASTEXEC_TRANSFER_ADDR";
/// Optional path to a plain UTF-8 file appended to MCP server instructions at startup.
pub const INSTRUCTIONS_FILE: &str = "FASTEXEC_INSTRUCTIONS_FILE";
/// Controls whether server instructions are also exposed as an MCP tool (`off`, `move`, `copy`).
pub const INSTRUCTIONS_TOOL: &str = "FASTEXEC_INSTRUCTIONS_TOOL";
/// Maximum raw byte size accepted for `FASTEXEC_INSTRUCTIONS_FILE`.
const MAX_INSTRUCTIONS_BYTES: usize = 16 * 1024;
const MOVE_INSTRUCTIONS_POINTER: &str = "See the instructions tool for host-specific guidance.";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum InstructionsToolMode {
    #[default]
    Off,
    Move,
    Copy,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Tools {
    pub codemode: bool,
    pub transfer: bool,
    pub yolo: bool,
}

impl Tools {
    pub fn from_env() -> Result<Self, String> {
        session::parse_image_compress(std::env::var_os(session::IMAGE_COMPRESS).as_deref())?;
        session::parse_max_media_bytes(std::env::var_os(session::MAX_MEDIA_BYTES).as_deref())?;
        Ok(Self {
            codemode: codemode()?,
            transfer: transfer()?,
            yolo: yolo()?,
        })
    }
}

/// How a local or HTTP server publishes `/t/` capabilities when `transfer` is enabled.
#[derive(Clone)]
pub(crate) enum TransferEndpoint {
    /// Local stdio: a dedicated `/t/` listener with a fixed base URL.
    Fixed {
        table: Arc<transfer::Transfers>,
        base: Arc<str>,
    },
    /// Streamable HTTP: `public_root` when configured, else inferred from each request.
    Http {
        table: Arc<transfer::Transfers>,
        public_root: Option<Arc<str>>,
    },
}

// Keep the advertised schema while returning invalid tool arguments as tool errors.
#[derive(serde::Deserialize)]
#[serde(transparent)]
struct CodemodeArguments(serde_json::Value);

impl schemars::JsonSchema for CodemodeArguments {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        codemode::Request::schema_name()
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        codemode::Request::json_schema(generator)
    }
}

#[derive(Clone)]
struct Server {
    backend: Backend,
    instructions: Arc<str>,
    tool_instructions: Option<Arc<str>>,
    /// Connection instances share the immutable routing and tool metadata for their host.
    router: Arc<ToolRouter<Server>>,
    transfer: Option<TransferEndpoint>,
    tools: Tools,
    /// Sessionless HTTP calls select the session for their authenticated token.
    principals: Option<Arc<[Arc<Session>]>>,
    /// Outbound active-byte leases indexed by JSON-RPC request ID for stdio responses.
    stdio_leases: Option<Arc<RequestLeases>>,
}

/// Where this server's calls run: in this process, or on the namespace's daemon.
#[derive(Clone)]
enum Backend {
    Local {
        service: Arc<Service>,
        render: Render,
        /// Created by initialize, or the first tool call of a stdio client that skips it.
        session: Arc<OnceLock<Result<Arc<Session>, String>>>,
        /// What this server's tasks inherit: the environment captured at startup, so a later
        /// change to the server's own environment cannot reach them.
        launch: Arc<LaunchContext>,
        /// True when a shared daemon owns tasks past the end of this session.
        daemon: bool,
    },
    /// `on` mode: the daemon owns every task, and this process only carries calls to it.
    Daemon(Arc<endpoint::Endpoint>),
}

impl Backend {
    fn daemon_owned(&self) -> bool {
        match self {
            Self::Local { daemon, .. } => *daemon,
            Self::Daemon(_) => true,
        }
    }
}

impl Drop for Backend {
    fn drop(&mut self) {
        if let Self::Local {
            service, session, ..
        } = self
            && Arc::strong_count(session) == 1
            && let Some(Ok(sess)) = session.get()
        {
            service.event_bus().remove_session(&sess.id);
        }
    }
}

impl Server {
    /// A server over `backend`, whose `exec` tool says where its tasks run.
    fn new(
        backend: Backend,
        launch: &LaunchContext,
        transfer: Option<TransferEndpoint>,
        tools: Tools,
        extra_instructions: Option<&str>,
        instructions_tool: InstructionsToolMode,
    ) -> Self {
        let (instructions, tool_instructions) = compose_instructions(
            backend.daemon_owned(),
            tools.codemode,
            extra_instructions,
            instructions_tool,
        );
        let router = Arc::new(Self::build_router(
            launch,
            tools,
            tool_instructions.as_deref(),
        ));
        Self::with_shared(
            backend,
            router,
            instructions,
            tool_instructions,
            transfer,
            tools,
        )
    }

    /// A server reusing a pre-built tool router and composed instructions snapshot.
    pub(crate) fn with_shared(
        backend: Backend,
        router: Arc<ToolRouter<Self>>,
        instructions: Arc<str>,
        tool_instructions: Option<Arc<str>>,
        transfer: Option<TransferEndpoint>,
        tools: Tools,
    ) -> Self {
        Self {
            backend,
            instructions,
            tool_instructions,
            router,
            transfer,
            tools,
            principals: None,
            stdio_leases: None,
        }
    }

    fn with_stdio_leases(mut self, stdio_leases: Arc<RequestLeases>) -> Self {
        self.stdio_leases = Some(stdio_leases);
        self
    }

    pub(crate) fn build_router(
        launch: &LaunchContext,
        tools: Tools,
        tool_instructions: Option<&str>,
    ) -> ToolRouter<Self> {
        let mut exec = Self::exec_tool_attr();
        exec.description = Some(exec_description(launch, tools.codemode).into());
        let mut router = ToolRouter::new()
            .with_route((prepare_tool(exec, tools.yolo), Self::exec))
            .with_route((prepare_tool(Self::read_tool_attr(), tools.yolo), Self::read))
            .with_route((
                prepare_tool(Self::write_tool_attr(), tools.yolo),
                Self::write,
            ))
            .with_route((prepare_tool(Self::edit_tool_attr(), tools.yolo), Self::edit));
        if tools.codemode {
            router = router.with_route((
                prepare_tool(Self::codemode_tool_attr(), tools.yolo),
                Self::codemode,
            ));
        }
        if tools.transfer {
            router = router.with_route((
                prepare_tool(Self::transfer_tool_attr(), tools.yolo),
                Self::transfer,
            ));
        }
        if let Some(description) = tool_instructions {
            let mut instructions = Self::instructions_tool_attr();
            instructions.description = Some(description.to_owned().into());
            // Pin the exact zero-arg schema so schemars root metadata (like `title`) cannot leak.
            instructions.input_schema = Arc::new(
                serde_json::json!({
                    "type": "object",
                    "properties": {},
                    "additionalProperties": false,
                })
                .as_object()
                .expect("object schema")
                .clone(),
            );
            router =
                router.with_route((prepare_tool(instructions, tools.yolo), Self::instructions));
        }
        router
    }

    /// The principal session for sessionless HTTP, or this server's client-named session.
    fn session(&self, context: &RequestContext<RoleServer>) -> Result<Arc<Session>, String> {
        let Backend::Local {
            session, launch, ..
        } = &self.backend
        else {
            return Err("this server has no tasks of its own".to_owned());
        };
        if session.get().is_none()
            && let Some(principals) = &self.principals
        {
            let principal = context
                .extensions
                .get::<::http::request::Parts>()
                .and_then(|parts| parts.extensions.get::<http::Principal>())
                .ok_or_else(|| "Internal error: HTTP request has no principal.".to_owned())?;
            return principals
                .get(principal.0)
                .cloned()
                .ok_or_else(|| "Internal error: HTTP principal is out of range.".to_owned());
        }
        session
            .get_or_init(|| client_session(context, launch))
            .clone()
    }
}

/// A session named for the client that initializes or first uses this server.
fn client_session(
    context: &RequestContext<RoleServer>,
    launch: &Arc<LaunchContext>,
) -> Result<Arc<Session>, String> {
    let short =
        tasks::random_chars(6).map_err(|error| format!("Cannot name the session: {error}"))?;
    Ok(Session::new(
        short.clone(),
        format!("{}-{short}", client_name(context)),
        Arc::clone(launch),
    ))
}

/// The client's name from request metadata or the legacy handshake.
fn client_name(context: &RequestContext<RoleServer>) -> String {
    context
        .client_info()
        .map_or_else(|| "client".to_string(), |info| info.name)
}

/// Server instructions say when to use the tools; hosts that show one line take the first. The
/// second line states how long tasks live, and codemode adds a routing hint when enabled.
pub(crate) fn server_instructions(
    daemon_owned: bool,
    codemode: bool,
    extra: Option<&str>,
) -> Arc<str> {
    let lifetime = if daemon_owned {
        "tasks run in a shared background daemon, outlive this session, and are visible to every session using it."
    } else {
        "tasks end when the session ends."
    };
    let mut text = if codemode {
        format!(
            "Run bash tasks with exec, orchestrate multi-step tool workflows in JavaScript with codemode, and read, write, and edit files on this host.\nPrefer exec over one-shot shell tools when a command may outlive a single call or needs input; {lifetime}\nPrefer codemode over multi-turn exec or read calls when aggregating across multiple files, looping or branching on tool results, or filtering bulky output in-process to save context."
        )
    } else {
        format!(
            "Run bash tasks with exec and read, write, and edit files on this host.\nPrefer it over one-shot shell tools when a command may outlive a single call or needs input; {lifetime}"
        )
    };
    if let Some(extra) = extra {
        text.push_str("\n\n");
        text.push_str(extra);
    }
    Arc::from(text)
}

/// Composes the protocol `instructions` snapshot and optional `instructions` tool snapshot.
pub(crate) fn compose_instructions(
    daemon_owned: bool,
    codemode: bool,
    extra: Option<&str>,
    mode: InstructionsToolMode,
) -> (Arc<str>, Option<Arc<str>>) {
    match mode {
        InstructionsToolMode::Off => (server_instructions(daemon_owned, codemode, extra), None),
        InstructionsToolMode::Move => match extra {
            Some(extra) => (
                server_instructions(daemon_owned, codemode, Some(MOVE_INSTRUCTIONS_POINTER)),
                Some(Arc::from(extra)),
            ),
            None => {
                let base = server_instructions(daemon_owned, codemode, None);
                (Arc::clone(&base), Some(base))
            }
        },
        InstructionsToolMode::Copy => {
            let full = server_instructions(daemon_owned, codemode, extra);
            (Arc::clone(&full), Some(full))
        }
    }
}

/// Loads and normalizes `FASTEXEC_INSTRUCTIONS_FILE` once at startup.
pub(crate) fn instructions_file(launch: &LaunchContext) -> Result<Option<String>, String> {
    use std::io::Read;

    let Some(value) = launch.var(INSTRUCTIONS_FILE) else {
        return Ok(None);
    };
    let Some(raw) = value.to_str() else {
        return Err(format!("{INSTRUCTIONS_FILE} is not UTF-8"));
    };
    if raw.is_empty() {
        return Err(format!("{INSTRUCTIONS_FILE} must not be empty"));
    }
    let resolved = files::resolve(raw, launch).map_err(|error| {
        format!(
            "invalid {INSTRUCTIONS_FILE}: {}",
            error.trim_end_matches('.')
        )
    })?;
    let shown_path = files::display_path(&resolved);
    let (file, _) = files::open_regular(&resolved)
        .map_err(|error| format!("cannot read {INSTRUCTIONS_FILE} {shown_path}: {error}"))?;
    let mut bytes = Vec::new();
    file.take(MAX_INSTRUCTIONS_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| format!("cannot read {INSTRUCTIONS_FILE} {shown_path}: {error}"))?;
    if bytes.len() > MAX_INSTRUCTIONS_BYTES {
        return Err(format!(
            "{INSTRUCTIONS_FILE} {shown_path} exceeds {MAX_INSTRUCTIONS_BYTES} bytes"
        ));
    }
    if bytes.contains(&0) {
        return Err(format!(
            "{INSTRUCTIONS_FILE} {shown_path} contains a NUL byte"
        ));
    }
    let text = String::from_utf8(bytes)
        .map_err(|_| format!("{INSTRUCTIONS_FILE} {shown_path} is not valid UTF-8"))?;
    let normalized = files::normalize_to_lf(&text);
    let content = normalized.trim_start_matches('\u{feff}');
    if content.trim().is_empty() {
        return Ok(None);
    }
    Ok(Some(content.to_owned()))
}

/// The `exec` tool's description: the static text, the Windows paragraph on Windows, and one
/// line saying where this server's tasks run. Nothing in it changes while the server runs.
fn exec_description(launch: &LaunchContext, codemode: bool) -> String {
    let mut description = include_str!("description.md").trim_end().to_owned();
    if cfg!(windows) {
        description.push_str("\n\n");
        description.push_str(include_str!("description-windows.md").trim_end());
    }
    if codemode {
        description.push_str("\n\npoll, kill, and list also take codemode tasks. A codemode task takes no input, eof, keys,\nresize, screen, raw, encoding, or transcript; killing it cancels only the calls the script is\nawaiting, and the tasks it started keep running.");
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
        .map_or_else(|| "unknown".to_owned(), |home| files::display_path(&home));
    format!(
        "{} {} · {hostname} · user {user} · home {home} · default cwd {}",
        std::env::consts::OS,
        std::env::consts::ARCH,
        files::display_path(&launch.cwd)
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
    async fn initialize(
        &self,
        request: InitializeRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<InitializeResult, rmcp::ErrorData> {
        let _response_workspace = reserve_response_content(
            lease::process_pool(),
            self.instructions.len().saturating_add(512),
        )?;
        let result = self
            .negotiate_initialize(&request)?
            .with_instructions(self.instructions.as_ref());
        context.peer.set_peer_info(request);
        if let Backend::Local {
            session, launch, ..
        } = &self.backend
        {
            session
                .get_or_init(|| client_session(&context, launch))
                .clone()
                .map_err(|message| rmcp::ErrorData::internal_error(message, None))?;
        }
        Ok(result)
    }

    async fn discover(
        &self,
        _context: RequestContext<RoleServer>,
    ) -> Result<rmcp::model::DiscoverResult, rmcp::ErrorData> {
        let _response_workspace = reserve_response_content(
            lease::process_pool(),
            self.instructions.len().saturating_add(512),
        )?;
        let mut info = self.get_info();
        let mut extensions = info.capabilities.extensions.take().unwrap_or_default();
        let mut events_cap = serde_json::Map::new();
        events_cap.insert("listChanged".to_owned(), serde_json::Value::Bool(false));
        extensions.insert("io.modelcontextprotocol/events".to_owned(), events_cap);
        info.capabilities.extensions = Some(extensions);
        Ok(rmcp::model::DiscoverResult::from_server_info(
            self.supported_protocol_versions().into_owned(),
            info.with_instructions(self.instructions.as_ref()),
        ))
    }

    async fn on_custom_request(
        &self,
        request: rmcp::model::CustomRequest,
        context: RequestContext<RoleServer>,
    ) -> Result<rmcp::model::CustomResult, rmcp::ErrorData> {
        // Version isolation: events/* is only available on MCP 2026-07-28+.
        let supports_events = context
            .protocol_version()
            .is_some_and(|version| version >= rmcp::model::ProtocolVersion::V_2026_07_28);
        if !supports_events {
            return Err(rmcp::ErrorData::new(
                rmcp::model::ErrorCode::METHOD_NOT_FOUND,
                request.method,
                None,
            ));
        }

        match request.method.as_str() {
            "events/list" => self.handle_events_list(request.params, context).await,
            "events/poll" => self.handle_events_poll(request.params, context).await,
            "events/stream" => self.handle_events_stream(request.params, context).await,
            // Webhook delivery (`events/subscribe`, `events/unsubscribe`) is intentionally out of scope.
            "events/subscribe" | "events/unsubscribe" => Err(rmcp::ErrorData::new(
                rmcp::model::ErrorCode::METHOD_NOT_FOUND,
                request.method,
                None,
            )),
            _ => Err(rmcp::ErrorData::new(
                rmcp::model::ErrorCode::METHOD_NOT_FOUND,
                request.method,
                None,
            )),
        }
    }

    async fn list_tools(
        &self,
        _request: Option<rmcp::model::PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<rmcp::model::ListToolsResult, rmcp::ErrorData> {
        let bytes = self.router.map.values().fold(512usize, |bytes, route| {
            bytes.saturating_add(tool_metadata_copy_bytes(&route.attr))
        });
        let _response_workspace = reserve_response_content(lease::process_pool(), bytes)?;
        let supports_cache_hints = context
            .protocol_version()
            .is_some_and(|version| version >= rmcp::model::ProtocolVersion::V_2026_07_28);
        Ok(rmcp::model::ListToolsResult {
            result_type: Some(rmcp::model::ResultType::COMPLETE),
            tools: self.router.list_all(),
            ttl_ms: supports_cache_hints.then_some(0),
            cache_scope: supports_cache_hints.then_some(rmcp::model::CacheScope::Public),
            ..Default::default()
        })
    }

    fn get_info(&self) -> ServerConfig {
        // Internal capability checks need no per-request instructions copy.
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("fastexec", env!("CARGO_PKG_VERSION")))
    }
}

fn event_error_to_rmcp(err: events::EventError) -> rmcp::ErrorData {
    rmcp::ErrorData::new(
        rmcp::model::ErrorCode(err.code()),
        err.to_string(),
        err.data(),
    )
}

fn ipc_event_error_to_rmcp(err: ipc::IpcEventError) -> rmcp::ErrorData {
    rmcp::ErrorData::new(rmcp::model::ErrorCode(err.code), err.message, err.data)
}

fn decode_mcp_header_value(raw: &str) -> Option<String> {
    use base64::Engine as _;
    if let Some(rest) = raw.strip_prefix("=?base64?") {
        let encoded = rest.strip_suffix("?=")?;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .ok()?;
        return String::from_utf8(bytes).ok();
    }
    Some(raw.to_owned())
}

/// Validates the HTTP `Mcp-Name` header against `params.name` (supporting `=?base64?...?=`
/// decoding). During Beta interop, a missing header is tolerated while any present header that
/// fails decoding or mismatches `params.name` returns `ErrorData::header_mismatch` (HTTP 400 / -32020).
fn validate_mcp_name_header(
    context: &RequestContext<RoleServer>,
    expected_name: &str,
) -> Result<(), rmcp::ErrorData> {
    let Some(parts) = context.extensions.get::<::http::request::Parts>() else {
        return Ok(());
    };
    let Some(raw_header) = parts.headers.get("mcp-name") else {
        return Ok(());
    };
    let raw_str = raw_header.to_str().map_err(|_| {
        rmcp::ErrorData::header_mismatch("Mcp-Name header is not valid ASCII", None)
    })?;
    let decoded = decode_mcp_header_value(raw_str).ok_or_else(|| {
        rmcp::ErrorData::header_mismatch("Mcp-Name header has invalid base64 encoding", None)
    })?;
    if decoded != expected_name {
        return Err(rmcp::ErrorData::header_mismatch(
            format!("Mcp-Name header ({decoded:?}) does not match params.name ({expected_name:?})"),
            None,
        ));
    }
    Ok(())
}

fn build_event_notification(
    method: &str,
    params: serde_json::Value,
    subscription_id: &rmcp::model::RequestId,
) -> rmcp::model::ServerNotification {
    let mut notif = rmcp::model::CustomNotification::new(method, Some(params));
    rmcp::model::GetMeta::get_meta_mut(&mut notif).set_subscription_id(subscription_id.clone());
    rmcp::model::ServerNotification::CustomNotification(notif)
}

fn stream_complete_result(subscription_id: &rmcp::model::RequestId) -> rmcp::model::CustomResult {
    let id_value = serde_json::to_value(subscription_id).unwrap_or(serde_json::Value::Null);
    rmcp::model::CustomResult::new(serde_json::json!({
        "resultType": "complete",
        "_meta": {
            "io.modelcontextprotocol/subscriptionId": id_value
        }
    }))
}

impl Server {
    async fn handle_events_list(
        &self,
        params: Option<serde_json::Value>,
        context: RequestContext<RoleServer>,
    ) -> Result<rmcp::model::CustomResult, rmcp::ErrorData> {
        if let Some(value) = params
            && !value.is_object()
            && !value.is_null()
        {
            return Err(rmcp::ErrorData::invalid_params(
                "events/list params must be an object",
                None,
            ));
        }
        let result = match &self.backend {
            Backend::Local { service, .. } => service.event_bus().list_event_types(),
            Backend::Daemon(endpoint) => endpoint
                .events_list(&client_name(&context), context.ct.clone())
                .await
                .map_err(ipc_event_error_to_rmcp)?,
        };
        let value = serde_json::to_value(result)
            .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?;
        Ok(rmcp::model::CustomResult::new(value))
    }

    async fn handle_events_poll(
        &self,
        params: Option<serde_json::Value>,
        context: RequestContext<RoleServer>,
    ) -> Result<rmcp::model::CustomResult, rmcp::ErrorData> {
        let Some(raw) = params else {
            return Err(rmcp::ErrorData::invalid_params(
                "events/poll requires params with `name`",
                None,
            ));
        };
        let poll_req: events::PollRequest = serde_json::from_value(raw)
            .map_err(|e| rmcp::ErrorData::invalid_params(format!("invalid params: {e}"), None))?;
        validate_mcp_name_header(&context, &poll_req.name)?;

        let response = match &self.backend {
            Backend::Local { service, .. } => {
                let caller_session = self.session(&context).ok().map(|s| s.id.clone());
                service
                    .event_bus()
                    .poll(poll_req, caller_session.as_deref())
                    .map_err(event_error_to_rmcp)?
            }
            Backend::Daemon(endpoint) => endpoint
                .events_poll(&client_name(&context), poll_req, context.ct.clone())
                .await
                .map_err(ipc_event_error_to_rmcp)?,
        };
        let value = serde_json::to_value(response)
            .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?;
        Ok(rmcp::model::CustomResult::new(value))
    }

    async fn handle_events_stream(
        &self,
        params: Option<serde_json::Value>,
        context: RequestContext<RoleServer>,
    ) -> Result<rmcp::model::CustomResult, rmcp::ErrorData> {
        let Some(raw) = params else {
            return Err(rmcp::ErrorData::invalid_params(
                "events/stream requires params with `name`",
                None,
            ));
        };
        let sub_req: events::SubscribeRequest = serde_json::from_value(raw)
            .map_err(|e| rmcp::ErrorData::invalid_params(format!("invalid params: {e}"), None))?;
        validate_mcp_name_header(&context, &sub_req.name)?;

        let sub_id = context.id.clone();
        match &self.backend {
            Backend::Local { service, .. } => {
                let _guard = service
                    .event_bus()
                    .try_acquire_stream()
                    .map_err(event_error_to_rmcp)?;
                let caller_session = self.session(&context).ok().map(|s| s.id.clone());
                let mut sub = service
                    .event_bus()
                    .subscribe(sub_req, caller_session.as_deref())
                    .map_err(event_error_to_rmcp)?;

                let initial = serde_json::to_value(sub.initial_active())
                    .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?;
                if context
                    .peer
                    .send_notification(build_event_notification(
                        "notifications/events/active",
                        initial,
                        &sub_id,
                    ))
                    .await
                    .is_err()
                {
                    return Ok(stream_complete_result(&sub_id));
                }

                let heartbeat = std::time::Duration::from_millis(sub.heartbeat_ms());
                let sleep = tokio::time::sleep(heartbeat);
                tokio::pin!(sleep);
                loop {
                    tokio::select! {
                        biased;
                        _ = context.ct.cancelled() => {
                            // Client cancelled: rmcp drops the response when local_ct_pool entry is gone.
                            return Ok(stream_complete_result(&sub_id));
                        }
                        item = sub.next() => {
                            match item {
                                Some(events::StreamItem::Active(active)) => {
                                    let payload = serde_json::to_value(active)
                                        .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?;
                                    if context.peer.send_notification(build_event_notification(
                                        "notifications/events/active",
                                        payload,
                                        &sub_id,
                                    )).await.is_err() {
                                        return Ok(stream_complete_result(&sub_id));
                                    }
                                    sleep.as_mut().reset(tokio::time::Instant::now() + heartbeat);
                                }
                                Some(events::StreamItem::Event(ev)) => {
                                    let payload = serde_json::to_value(ev)
                                        .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?;
                                    if context.peer.send_notification(build_event_notification(
                                        "notifications/events/event",
                                        payload,
                                        &sub_id,
                                    )).await.is_err() {
                                        return Ok(stream_complete_result(&sub_id));
                                    }
                                    sleep.as_mut().reset(tokio::time::Instant::now() + heartbeat);
                                }
                                None => {
                                    let _ = context.peer.send_notification(build_event_notification(
                                        "notifications/events/terminated",
                                        serde_json::json!({
                                            "error": {
                                                "code": -32000,
                                                "message": "event bus closed"
                                            }
                                        }),
                                        &sub_id,
                                    )).await;
                                    return Ok(stream_complete_result(&sub_id));
                                }
                            }
                        }
                        _ = &mut sleep => {
                            if context.peer.send_notification(build_event_notification(
                                "notifications/events/heartbeat",
                                serde_json::json!({ "cursor": sub.current_cursor() }),
                                &sub_id,
                            )).await.is_err() {
                                return Ok(stream_complete_result(&sub_id));
                            }
                            sleep.as_mut().reset(tokio::time::Instant::now() + heartbeat);
                        }
                    }
                }
            }
            Backend::Daemon(endpoint) => {
                let (initial, mut stream) = endpoint
                    .events_stream(&client_name(&context), sub_req, context.ct.clone())
                    .await
                    .map_err(ipc_event_error_to_rmcp)?;

                let initial_val = serde_json::to_value(initial)
                    .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?;
                if context
                    .peer
                    .send_notification(build_event_notification(
                        "notifications/events/active",
                        initial_val,
                        &sub_id,
                    ))
                    .await
                    .is_err()
                {
                    stream.cancel().await;
                    return Ok(stream_complete_result(&sub_id));
                }

                let mut terminated_sent = false;
                loop {
                    tokio::select! {
                        biased;
                        _ = context.ct.cancelled() => {
                            stream.cancel().await;
                            return Ok(stream_complete_result(&sub_id));
                        }
                        frame = stream.next() => {
                            match frame {
                                Some((ipc::EventWireFrame::Active(active), _lease)) => {
                                    let payload = serde_json::to_value(active)
                                        .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?;
                                    if context.peer.send_notification(build_event_notification(
                                        "notifications/events/active",
                                        payload,
                                        &sub_id,
                                    )).await.is_err() {
                                        stream.cancel().await;
                                        return Ok(stream_complete_result(&sub_id));
                                    }
                                }
                                Some((ipc::EventWireFrame::Event(ev), _lease)) => {
                                    let payload = serde_json::to_value(ev)
                                        .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?;
                                    if context.peer.send_notification(build_event_notification(
                                        "notifications/events/event",
                                        payload,
                                        &sub_id,
                                    )).await.is_err() {
                                        stream.cancel().await;
                                        return Ok(stream_complete_result(&sub_id));
                                    }
                                }
                                Some((ipc::EventWireFrame::Heartbeat { cursor }, _lease)) => {
                                    if context.peer.send_notification(build_event_notification(
                                        "notifications/events/heartbeat",
                                        serde_json::json!({ "cursor": cursor }),
                                        &sub_id,
                                    )).await.is_err() {
                                        stream.cancel().await;
                                        return Ok(stream_complete_result(&sub_id));
                                    }
                                }
                                Some((ipc::EventWireFrame::Error(err), _lease)) => {
                                    let mut obj = serde_json::json!({
                                        "code": err.code,
                                        "message": err.message,
                                    });
                                    if let Some(data) = err.data {
                                        obj["data"] = data;
                                    }
                                    if context.peer.send_notification(build_event_notification(
                                        "notifications/events/error",
                                        serde_json::json!({ "error": obj }),
                                        &sub_id,
                                    )).await.is_err() {
                                        stream.cancel().await;
                                        return Ok(stream_complete_result(&sub_id));
                                    }
                                }
                                Some((ipc::EventWireFrame::Terminated(err), _lease)) => {
                                    terminated_sent = true;
                                    let mut obj = serde_json::json!({
                                        "code": err.code,
                                        "message": err.message,
                                    });
                                    if let Some(data) = err.data {
                                        obj["data"] = data;
                                    }
                                    let _ = context.peer.send_notification(build_event_notification(
                                        "notifications/events/terminated",
                                        serde_json::json!({ "error": obj }),
                                        &sub_id,
                                    )).await;
                                }
                                Some((ipc::EventWireFrame::Complete, _lease)) => {
                                    return Ok(stream_complete_result(&sub_id));
                                }
                                None => {
                                    if !terminated_sent {
                                        let _ = context.peer.send_notification(build_event_notification(
                                            "notifications/events/terminated",
                                            serde_json::json!({
                                                "error": {
                                                    "code": -32000,
                                                    "message": "daemon connection closed"
                                                }
                                            }),
                                            &sub_id,
                                        )).await;
                                    }
                                    return Ok(stream_complete_result(&sub_id));
                                }
                                Some((ipc::EventWireFrame::ListResult(_), _))
                                | Some((ipc::EventWireFrame::PollResult(_), _)) => {}
                            }
                        }
                    }
                }
            }
        }
    }
}

fn prepare_tool(mut attr: rmcp::model::Tool, yolo: bool) -> rmcp::model::Tool {
    let raw = serde_json::Value::Object((*attr.input_schema).clone());
    if let serde_json::Value::Object(portable) = portable_schema(raw) {
        attr.input_schema = Arc::new(portable);
    }
    if yolo {
        let title = attr
            .annotations
            .as_ref()
            .and_then(|annotations| annotations.title.clone());
        attr.annotations = Some(ToolAnnotations::from_raw(
            title,
            Some(true),
            Some(false),
            Some(false),
            Some(false),
        ));
    }
    attr
}

impl Server {
    fn admit_tool_input<T: serde::Serialize>(
        &self,
        context: &mut RequestContext<RoleServer>,
        parameters: &T,
        is_control: bool,
    ) -> Result<(), CallToolResult> {
        let (pool, control) = match &self.backend {
            Backend::Local { launch, .. } => (launch.memory(), launch.control_memory()),
            Backend::Daemon(endpoint) => (endpoint.memory(), endpoint.control_memory()),
        };
        let owner = application_input_owner(
            &(parameters, &context.id),
            context.extensions.get::<lease::Owner>(),
            pool,
            control,
            is_control,
        )
        .map_err(|message| CallToolResult::error(vec![ContentBlock::text(message)]))?;
        context.extensions.insert(owner);
        Ok(())
    }

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
        let is_control = matches!(
            request.action,
            service::Action::List | service::Action::Kill
        );
        let mut context = context;
        if let Err(error) = self.admit_tool_input(&mut context, &request, is_control) {
            return error;
        }
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
                let tracked = endpoint
                    .call_tracked(
                        "exec",
                        arguments,
                        context.id.to_string(),
                        &client_name(&context),
                        context.ct.clone(),
                        progress,
                    )
                    .await;
                self.deliver_daemon(&context, tracked, endpoint, is_control)
            }
            Backend::Local {
                service,
                render,
                session: _,
                launch,
                daemon: _,
            } => match self.session(&context) {
                Ok(session) => {
                    let (progress, _forwarder) = progress(&context);
                    let call = Call {
                        session: &session,
                        launch: None,
                        render: *render,
                        cancel: context.ct.clone(),
                        progress,
                        call_id: None,
                        nested: false,
                        started: None,
                        render_slot: None,
                        http_response: http_context(&context),
                        rpc_id_bytes: ipc::json_len(&context.id).unwrap_or(usize::MAX),
                        input_owner: request_input_owner(&context),
                    };
                    let tracked = service.call_tracked(request, call).await;
                    self.deliver_local(&context, tracked, launch, *render, is_control)
                }
                Err(message) => {
                    let err =
                        service::suppress(service::error_result(request.action, &message), *render);
                    self.deliver_local(
                        &context,
                        files::TrackedResult::plain(err),
                        launch,
                        *render,
                        true,
                    )
                }
            },
        }
    }

    #[doc = include_str!("description-codemode.md")]
    #[tool(
        name = "codemode",
        annotations(
            title = "Run JavaScript",
            read_only_hint = false,
            destructive_hint = true,
            open_world_hint = true
        )
    )]
    async fn codemode(
        &self,
        Parameters(arguments): Parameters<CodemodeArguments>,
        context: RequestContext<RoleServer>,
    ) -> CallToolResult {
        let mut context = context;
        if let Err(error) = self.admit_tool_input(&mut context, &arguments.0, false) {
            return error;
        }
        let request: codemode::Request = match serde_json::from_value(arguments.0) {
            Ok(request) => request,
            Err(error) => {
                return CallToolResult::error(vec![ContentBlock::text(error.to_string())]);
            }
        };
        match &self.backend {
            Backend::Daemon(endpoint) => {
                self.forward_daemon(endpoint, "codemode", &request, &context)
                    .await
            }
            Backend::Local {
                service,
                render,
                launch,
                ..
            } => {
                let session = match self.session(&context) {
                    Ok(session) => session,
                    Err(message) => {
                        return service::suppress(service::codemode_error(&message), *render);
                    }
                };
                let (progress, _forwarder) = progress(&context);
                let call = Call {
                    session: &session,
                    launch: None,
                    render: *render,
                    cancel: context.ct.clone(),
                    progress,
                    call_id: None,
                    nested: false,
                    started: None,
                    render_slot: None,
                    http_response: http_context(&context),
                    rpc_id_bytes: ipc::json_len(&context.id).unwrap_or(usize::MAX),
                    input_owner: request_input_owner(&context),
                };
                let tracked = service
                    .codemode_tracked(request, call, self.tools.transfer)
                    .await;
                self.deliver_local(&context, tracked, launch, *render, false)
            }
        }
    }

    #[doc = "Read a file. Supports text, images, audio, PDF, and video; media files attach directly and ignore offset and limit. Text output caps at 2000 lines or 50KB. Pass offset (1-indexed, or negative from the end) and limit for text slices, continuing with nextOffset until complete."]
    #[tool(name = "read", annotations(title = "Read file", read_only_hint = true))]
    async fn read(
        &self,
        Parameters(request): Parameters<files::ReadRequest>,
        context: RequestContext<RoleServer>,
    ) -> CallToolResult {
        let mut context = context;
        if let Err(error) = self.admit_tool_input(&mut context, &request, false) {
            return error;
        }
        match &self.backend {
            Backend::Local { launch, render, .. } => {
                let tracked = files::read_tracked(
                    request,
                    Arc::clone(launch),
                    self.tools.transfer,
                    *render,
                    Some(context.ct.clone()),
                    request_input_owner(&context),
                )
                .await;
                self.deliver_local(&context, tracked, launch, *render, false)
            }
            Backend::Daemon(endpoint) => {
                self.forward_daemon(endpoint, "read", &request, &context)
                    .await
            }
        }
    }

    #[doc = "Write content to a regular file. Creates missing parent directories and saves a complete replacement. Requires a writable parent directory and refuses files with multiple hard links; use exec for deliberate in-place writes."]
    #[tool(
        name = "write",
        annotations(title = "Write file", destructive_hint = true, idempotent_hint = false)
    )]
    async fn write(
        &self,
        Parameters(request): Parameters<files::WriteRequest>,
        context: RequestContext<RoleServer>,
    ) -> CallToolResult {
        let mut context = context;
        if let Err(error) = self.admit_tool_input(&mut context, &request, false) {
            return error;
        }
        match &self.backend {
            Backend::Local { launch, render, .. } => {
                let res = files::write_tracked(
                    request,
                    Arc::clone(launch),
                    *render,
                    request_input_owner(&context),
                )
                .await;
                self.deliver_local(&context, res, launch, *render, false)
            }
            Backend::Daemon(endpoint) => {
                self.forward_daemon(endpoint, "write", &request, &context)
                    .await
            }
        }
    }

    #[doc = "Edit a single file using exact text replacement. Every edits[].oldText must match a unique, non-overlapping region of the original file (or start within startLine..=endLine). Merge changes touching the same block or nearby lines instead of emitting overlapping edits, and avoid large unchanged bridges. Unmatched bytes are preserved. Saving requires a writable parent directory and a single-link regular file."]
    #[tool(
        name = "edit",
        annotations(title = "Edit file", destructive_hint = true, idempotent_hint = false)
    )]
    async fn edit(
        &self,
        Parameters(request): Parameters<files::EditRequest>,
        context: RequestContext<RoleServer>,
    ) -> CallToolResult {
        let mut context = context;
        if let Err(error) = self.admit_tool_input(&mut context, &request, false) {
            return error;
        }
        match &self.backend {
            Backend::Local { launch, render, .. } => {
                let res = files::edit_tracked(
                    request,
                    Arc::clone(launch),
                    *render,
                    request_input_owner(&context),
                )
                .await;
                self.deliver_local(&context, res, launch, *render, false)
            }
            Backend::Daemon(endpoint) => {
                self.forward_daemon(endpoint, "edit", &request, &context)
                    .await
            }
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
        let mut context = context;
        if let Err(error) = self.admit_tool_input(&mut context, &request, false) {
            return error;
        }
        match &self.backend {
            Backend::Daemon(endpoint) => {
                self.forward_daemon(endpoint, "transfer", &request, &context)
                    .await
            }
            Backend::Local { launch, render, .. } => {
                let res = match &self.transfer {
                    None => files::TrackedResult::plain(transfer_error(
                        "transfer is disabled; set FASTEXEC_TRANSFER=true.".to_owned(),
                        *render,
                    )),
                    Some(TransferEndpoint::Fixed { table, base }) => {
                        publish_transfer(
                            request,
                            Arc::clone(launch),
                            table,
                            base,
                            *render,
                            request_input_owner(&context),
                        )
                        .await
                    }
                    Some(TransferEndpoint::Http { table, public_root }) => {
                        let base = match public_root {
                            Some(root) => Ok(root.to_string()),
                            None => context
                                .extensions
                                .get::<::http::request::Parts>()
                                .ok_or_else(|| {
                                    "Cannot make a transfer URL outside an HTTP request.".to_owned()
                                })
                                .and_then(|parts| inferred_base(&parts.headers)),
                        };
                        match base {
                            Ok(base) => {
                                publish_transfer(
                                    request,
                                    Arc::clone(launch),
                                    table,
                                    &base,
                                    *render,
                                    request_input_owner(&context),
                                )
                                .await
                            }
                            Err(message) => {
                                files::TrackedResult::plain(transfer_error(message, *render))
                            }
                        }
                    }
                };
                self.deliver_local(&context, res, launch, *render, false)
            }
        }
    }

    #[tool(
        name = "instructions",
        annotations(
            title = "Server instructions",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn instructions(
        &self,
        Parameters(arguments): Parameters<serde_json::Map<String, serde_json::Value>>,
        _context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        if !arguments.is_empty() {
            return Err(rmcp::ErrorData::invalid_params(
                "instructions takes no arguments",
                None,
            ));
        }
        match &self.tool_instructions {
            Some(text) => {
                let _response_workspace =
                    reserve_response_content(lease::process_pool(), text.len())?;
                Ok(CallToolResult::success(vec![ContentBlock::text(
                    text.as_ref(),
                )]))
            }
            None => Err(rmcp::ErrorData::invalid_params(
                "instructions tool is disabled",
                None,
            )),
        }
    }
}

impl Server {
    fn deliver_local(
        &self,
        context: &RequestContext<RoleServer>,
        mut tracked: files::TrackedResult,
        launch: &LaunchContext,
        render: Render,
        is_control: bool,
    ) -> CallToolResult {
        // Official rmcp owns HTTP response models and encodings after this
        // handoff. Its internal copies and queues are outside the app budget.
        if http_context(context) || self.stdio_leases.is_none() {
            return tracked.result;
        }
        let use_control = is_control || tracked.result.is_error == Some(true);
        let b64_bytes =
            service::unleased_result_payload_capacity(&tracked.result, &tracked.payload_owner);
        let exact_json = ipc::json_len(&tracked.result).unwrap_or(usize::MAX);
        let id_bytes = ipc::json_len(&context.id).unwrap_or(usize::MAX);
        let outbound_bytes = exact_json.saturating_add(id_bytes).saturating_add(256);
        let encoded_peak = outbound_bytes;
        let admitted = b64_bytes.checked_add(encoded_peak).and_then(|peak| {
            admit_delivery(
                &mut tracked.lease,
                launch.memory(),
                launch.control_memory(),
                peak,
                use_control,
            )
        });
        let Some(mut held) = admitted else {
            let meta = tracked.task_meta.take();
            drop(tracked);
            let msg = match &meta {
                Some(m) => service::overload_task_message(launch.memory().limit(), m),
                None => format!(
                    "Server active memory budget ({} bytes) is full; retry once in-flight transfers complete.",
                    launch.memory().limit()
                ),
            };
            let effective = render.effective();
            let mut err = CallToolResult::error(vec![ContentBlock::text(msg.clone())]);
            err.structured_content = Some(serde_json::json!({ "ok": false, "error": msg }));
            let suppressed = service::suppress(err, effective);
            let err_outbound = ipc::json_len(&suppressed)
                .unwrap_or(usize::MAX)
                .saturating_add(id_bytes)
                .saturating_add(256);
            let err_peak =
                service::result_payload_capacity(&suppressed).saturating_add(err_outbound);
            if let Some(mut err_lease) = (err_peak <= lease::CONTROL_LIMIT)
                .then(|| launch.control_memory().try_acquire(err_peak))
                .flatten()
                && let Some(stdio) = &self.stdio_leases
            {
                let outbound = err_lease.split_off(err_outbound);
                return stdio.deliver(
                    context.id.clone(),
                    &context.ct,
                    suppressed,
                    err_lease,
                    outbound,
                    Default::default(),
                );
            }
            return suppressed;
        };
        if let Some(stdio) = &self.stdio_leases {
            let outbound = held.split_off(outbound_bytes.min(held.bytes()));
            return stdio.deliver(
                context.id.clone(),
                &context.ct,
                tracked.result,
                held,
                outbound,
                tracked.payload_owner,
            );
        }
        tracked.result
    }

    fn deliver_daemon(
        &self,
        context: &RequestContext<RoleServer>,
        mut tracked: files::TrackedResult,
        endpoint: &endpoint::Endpoint,
        is_control: bool,
    ) -> CallToolResult {
        let Some(stdio) = &self.stdio_leases else {
            drop(tracked.lease.take());
            return tracked.result;
        };
        let pool = endpoint.memory();
        let control_pool = endpoint.control_memory();
        let use_control = is_control || tracked.result.is_error == Some(true);
        let b64_bytes =
            service::unleased_result_payload_capacity(&tracked.result, &tracked.payload_owner);
        let exact_json = ipc::json_len(&tracked.result).unwrap_or(usize::MAX);
        let id_bytes = ipc::json_len(&context.id).unwrap_or(usize::MAX);
        let outbound_bytes = exact_json.saturating_add(id_bytes).saturating_add(256);
        let admitted = b64_bytes.checked_add(outbound_bytes).and_then(|peak| {
            admit_delivery(&mut tracked.lease, pool, control_pool, peak, use_control)
        });
        let Some(mut held) = admitted else {
            drop(tracked);
            let err = CallToolResult::error(vec![ContentBlock::text(format!(
                "Endpoint active memory budget ({} bytes) is full while writing stdio response; use action list or poll once in-flight transfers complete.",
                pool.limit()
            ))]);
            let err_outbound = ipc::json_len(&err)
                .unwrap_or(usize::MAX)
                .saturating_add(id_bytes)
                .saturating_add(256);
            let err_peak = service::result_payload_capacity(&err).saturating_add(err_outbound);
            if let Some(mut err_lease) = (err_peak <= lease::CONTROL_LIMIT)
                .then(|| control_pool.try_acquire(err_peak))
                .flatten()
            {
                let outbound = err_lease.split_off(err_outbound);
                return stdio.deliver(
                    context.id.clone(),
                    &context.ct,
                    err,
                    err_lease,
                    outbound,
                    Default::default(),
                );
            }
            return err;
        };
        let outbound = held.split_off(outbound_bytes.min(held.bytes()));
        stdio.deliver(
            context.id.clone(),
            &context.ct,
            tracked.result,
            held,
            outbound,
            tracked.payload_owner,
        )
    }

    async fn forward_daemon<T: serde::Serialize>(
        &self,
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
        let tracked = endpoint
            .call_tracked(
                tool,
                arguments,
                context.id.to_string(),
                &client_name(context),
                context.ct.clone(),
                progress,
            )
            .await;
        self.deliver_daemon(context, tracked, endpoint, false)
    }
}

pub(crate) fn transfer_error(message: String, render: Render) -> CallToolResult {
    let effective = render.effective();
    let mut result = CallToolResult::error(vec![ContentBlock::text(message.clone())]);
    result.structured_content = Some(serde_json::json!({ "ok": false, "error": message }));
    service::suppress(result, effective)
}

pub(crate) type TransferInspection = Result<(std::path::PathBuf, String, Option<u64>), String>;

pub(crate) fn inspect_transfer_request(
    request: transfer::Request,
    launch: Arc<LaunchContext>,
    input_owner: crate::lease::Owner,
) -> impl std::future::Future<Output = ipc::Owned<TransferInspection>> {
    // Build the owner before creating the future, including its never-polled case.
    let input = ipc::Owned::new(request, input_owner);
    async move {
        match tokio::task::spawn_blocking(move || {
            input.flat_map(|request| {
                files::with_path_workspace(&request.path, &launch, 0, |lease| {
                    let path = files::resolve(&request.path, &launch)?;
                    let shown = files::display_path(&path);
                    let bytes = inspect_transfer_target(&path, &shown, launch.memory(), lease)?;
                    Ok::<_, String>((path, shown, bytes))
                })
                .unwrap_or_else(|message| ipc::Owned::new(Err(message), Default::default()))
            })
        })
        .await
        {
            Ok(result) => result,
            Err(error) => ipc::Owned::new(
                Err(format!("the file operation did not finish: {error}")),
                Default::default(),
            ),
        }
    }
}

pub(crate) fn mint_transfer(
    path: std::path::PathBuf,
    shown_path: String,
    bytes: Option<u64>,
    table: &Arc<transfer::Transfers>,
    base: &str,
    render: Render,
) -> CallToolResult {
    let effective = render.effective();
    let id = match table.create(path) {
        Ok(id) => id,
        Err(message) => return transfer_error(message, render),
    };
    let url = format!("{base}t/{id}");
    let text = match bytes {
        Some(size) => format!("{url}\n{shown_path} · {size} bytes"),
        None => format!("{url}\n{shown_path} · does not exist yet; a PUT creates it"),
    };
    let mut result = CallToolResult::success(vec![ContentBlock::text(text)]);
    result.structured_content = Some(serde_json::json!({
        "ok": true,
        "url": url,
        "path": shown_path,
        "bytes": bytes,
    }));
    service::suppress(result, effective)
}

fn publish_transfer<'a>(
    request: transfer::Request,
    launch: Arc<LaunchContext>,
    table: &'a Arc<transfer::Transfers>,
    base: &'a str,
    render: Render,
    input_owner: crate::lease::Owner,
) -> impl std::future::Future<Output = files::TrackedResult> + 'a {
    let inspection = inspect_transfer_request(request, launch, input_owner);
    async move {
        let inspected = inspection.await;
        let owner = inspected.owner.clone();
        let result = inspected.run(|inspected| match inspected {
            Ok((path, shown_path, bytes)) => {
                mint_transfer(path, shown_path, bytes, table, base, render)
            }
            Err(message) => transfer_error(message, render),
        });
        files::TrackedResult::owned(result, owner)
    }
}

/// Verifies that `path` is either an existing regular file (returning its byte size) or a
/// not-yet-existing path under accessible directory ancestors (`None`). Directories, special
/// files, and permission or I/O errors fail with a tar hint.
fn inspect_transfer_target(
    path: &std::path::Path,
    shown_path: &str,
    pool: &Arc<crate::lease::BytePool>,
    lease: &mut crate::lease::Lease,
) -> Result<Option<u64>, String> {
    let failed = |error: std::io::Error| {
        format!(
            "Cannot transfer {shown_path}: {error}. Pack directories with tar through exec first."
        )
    };
    match std::fs::metadata(path) {
        Ok(metadata) if metadata.is_file() => Ok(Some(metadata.len())),
        Ok(metadata) if metadata.is_dir() => Err(format!(
            "Cannot transfer {shown_path}: it is a directory; pack it with tar through exec first."
        )),
        Ok(_) => Err(format!(
            "Cannot transfer {shown_path}: it is not a regular file; pack directories or special files with tar through exec first."
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let target = files::write_target(path, pool).map_err(failed)?;
            files::reserve_target_workspace(&target.value, lease, pool).map_err(failed)?;
            for ancestor in target.value.ancestors().skip(1) {
                if ancestor.as_os_str().is_empty() {
                    break;
                }
                match std::fs::symlink_metadata(ancestor) {
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(error) => return Err(failed(error)),
                    Ok(metadata) if metadata.is_dir() => break,
                    Ok(metadata) if metadata.is_symlink() => match std::fs::metadata(ancestor) {
                        Ok(target) if target.is_dir() => break,
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                            return Err(format!(
                                "Cannot transfer {shown_path}: {} is a dangling directory link. Create its target before uploading.",
                                files::display_path(ancestor)
                            ));
                        }
                        Err(error) => return Err(failed(error)),
                        Ok(_) => {}
                    },
                    Ok(_) => {}
                }
                return Err(format!(
                    "Cannot transfer {shown_path}: {} is not a directory.",
                    files::display_path(ancestor)
                ));
            }
            Ok(None)
        }
        Err(error) => Err(failed(error)),
    }
}

fn is_null_schema(value: &serde_json::Value) -> bool {
    value
        .as_object()
        .is_some_and(|obj| obj.len() == 1 && obj.get("type") == Some(&serde_json::json!("null")))
}

/// Rewrites a schemars-generated JSON Schema into the portable subset understood across MCP
/// clients: no `$ref`, `$defs`, `$schema`, `anyOf`, `oneOf`, array `type`, or non-standard `format`.
pub(crate) fn portable_schema(mut schema: serde_json::Value) -> serde_json::Value {
    let defs = schema
        .as_object_mut()
        .and_then(|root| {
            root.remove("$schema");
            root.remove("$defs").or_else(|| root.remove("definitions"))
        })
        .and_then(|value| match value {
            serde_json::Value::Object(map) => Some(map),
            _ => None,
        })
        .unwrap_or_default();
    rewrite_schema_node(schema, &defs)
}

fn rewrite_schema_node(
    node: serde_json::Value,
    defs: &serde_json::Map<String, serde_json::Value>,
) -> serde_json::Value {
    let serde_json::Value::Object(mut obj) = node else {
        return node;
    };
    obj.remove("$schema");

    loop {
        let mut progressed = false;
        if let Some(serde_json::Value::String(reference)) = obj.remove("$ref") {
            let name = reference
                .strip_prefix("#/$defs/")
                .or_else(|| reference.strip_prefix("#/definitions/"))
                .unwrap_or(&reference);
            if let Some(serde_json::Value::Object(mut target)) = defs.get(name).cloned() {
                if obj.contains_key("description") {
                    target.remove("description");
                }
                for (key, value) in target {
                    obj.entry(key).or_insert(value);
                }
                progressed = true;
            }
        }
        if let Some(serde_json::Value::Array(items)) = obj.get("anyOf")
            && items.len() == 2
            && items.iter().any(is_null_schema)
            && let Some(non_null) = items.iter().find(|item| !is_null_schema(item)).cloned()
        {
            obj.remove("anyOf");
            if let serde_json::Value::Object(mut inner) = non_null {
                if obj.contains_key("description") {
                    inner.remove("description");
                }
                for (key, value) in inner {
                    obj.entry(key).or_insert(value);
                }
            }
            progressed = true;
        }
        if !progressed {
            break;
        }
    }

    if let Some(serde_json::Value::Array(variants)) = obj.get("oneOf")
        && !variants.is_empty()
        && variants
            .iter()
            .all(|v| v.get("const").and_then(serde_json::Value::as_str).is_some())
    {
        let Some(serde_json::Value::Array(variants)) = obj.remove("oneOf") else {
            unreachable!();
        };
        let mut values = Vec::with_capacity(variants.len());
        let mut notes = Vec::new();
        for variant in variants {
            let val = variant["const"].as_str().unwrap().to_owned();
            if let Some(desc) = variant
                .get("description")
                .and_then(serde_json::Value::as_str)
            {
                notes.push(format!("{val}: {desc}"));
            }
            values.push(serde_json::Value::String(val));
        }
        obj.insert("type".into(), serde_json::Value::String("string".into()));
        obj.insert("enum".into(), serde_json::Value::Array(values));
        if !notes.is_empty() {
            let suffix = notes.join(" ");
            let merged = match obj.get("description").and_then(serde_json::Value::as_str) {
                Some(existing) if !existing.is_empty() => format!("{existing} {suffix}"),
                _ => suffix,
            };
            obj.insert("description".into(), serde_json::Value::String(merged));
        }
    }

    if let Some(serde_json::Value::Array(types)) = obj.get("type")
        && types.len() == 2
        && types.iter().any(|t| t.as_str() == Some("null"))
        && let Some(non_null) = types.iter().find(|t| t.as_str() != Some("null")).cloned()
    {
        obj.insert("type".into(), non_null);
    }

    if let Some(serde_json::Value::String(fmt)) = obj.get("format")
        && matches!(
            fmt.as_str(),
            "uint"
                | "uint8"
                | "uint16"
                | "uint32"
                | "uint64"
                | "int"
                | "int8"
                | "int16"
                | "int32"
                | "int64"
                | "float"
                | "double"
        )
    {
        obj.remove("format");
    }

    for (key, value) in &mut obj {
        let taken = std::mem::take(value);
        *value = match (key.as_str(), taken) {
            (
                "properties" | "patternProperties" | "dependentSchemas",
                serde_json::Value::Object(map),
            ) => serde_json::Value::Object(
                map.into_iter()
                    .map(|(prop, schema)| (prop, rewrite_schema_node(schema, defs)))
                    .collect(),
            ),
            (_, serde_json::Value::Object(sub)) => {
                rewrite_schema_node(serde_json::Value::Object(sub), defs)
            }
            ("items" | "prefixItems", serde_json::Value::Array(items)) => serde_json::Value::Array(
                items
                    .into_iter()
                    .map(|item| rewrite_schema_node(item, defs))
                    .collect(),
            ),
            (_, other) => other,
        };
    }
    serde_json::Value::Object(obj)
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

// A failed growth keeps the old lease until the caller drops the owned result.
fn admit_delivery(
    held: &mut Option<lease::Lease>,
    pool: &Arc<lease::BytePool>,
    control: &Arc<lease::BytePool>,
    peak: usize,
    use_control: bool,
) -> Option<lease::Lease> {
    if let Some(previous) = held.as_mut() {
        if previous.try_resize(peak) {
            return held.take();
        }
    } else if let Some(acquired) = pool.try_acquire(peak) {
        return Some(acquired);
    }
    if use_control && peak <= lease::CONTROL_LIMIT {
        let acquired = control.try_acquire(peak)?;
        drop(held.take());
        return Some(acquired);
    }
    None
}

fn tool_metadata_copy_bytes(tool: &rmcp::model::Tool) -> usize {
    use std::borrow::Cow;
    let owned = |text: &Cow<'static, str>| match text {
        Cow::Borrowed(_) => 0,
        Cow::Owned(text) => text.len(),
    };
    // Schemas are shared Arc values; list_all only clones the independent fields.
    owned(&tool.name)
        .saturating_add(tool.title.as_ref().map_or(0, String::len))
        .saturating_add(tool.description.as_ref().map_or(0, owned))
        .saturating_add(ipc::json_len(&tool.annotations).unwrap_or(usize::MAX))
        .saturating_add(ipc::json_len(&tool.icons).unwrap_or(usize::MAX))
        .saturating_add(ipc::json_len(&tool.meta).unwrap_or(usize::MAX))
}

fn reserve_response_content(
    pool: &Arc<lease::BytePool>,
    bytes: usize,
) -> Result<lease::Lease, rmcp::ErrorData> {
    pool.try_acquire(bytes).ok_or_else(|| {
        rmcp::ErrorData::internal_error(
            "Server active memory budget is full while preparing the response",
            None,
        )
    })
}

// SDK parsing happened before this boundary. Reserve only the subsequent
// application copies of parameters/IDs and pass this owner into actual workers.
fn application_input_owner<T: serde::Serialize>(
    parameters_and_id: &T,
    existing: Option<&lease::Owner>,
    pool: &Arc<lease::BytePool>,
    control: &Arc<lease::BytePool>,
    is_control: bool,
) -> Result<lease::Owner, &'static str> {
    if let Some(owner) = existing {
        return Ok(owner.clone());
    }
    const REFUSED: &str = "Server active memory budget is full while preparing tool arguments; the operation was not dispatched.";
    let length = ipc::json_len(parameters_and_id).map_err(|_| REFUSED)?;
    let bytes = length.checked_mul(4).ok_or(REFUSED)?;
    let held = pool
        .try_acquire(bytes)
        .or_else(|| {
            (is_control && length <= ipc::SHORT_INPUT)
                .then(|| control.try_acquire(bytes))
                .flatten()
        })
        .ok_or(REFUSED)?;
    Ok(lease::Owner::new(held))
}

fn request_input_owner(context: &RequestContext<RoleServer>) -> crate::lease::Owner {
    context
        .extensions
        .get::<crate::lease::Owner>()
        .cloned()
        .unwrap_or_default()
}

fn http_context(context: &RequestContext<RoleServer>) -> bool {
    context
        .extensions
        .get::<::http::request::Parts>()
        .is_some_and(|parts| parts.extensions.get::<http::Principal>().is_some())
}

/// Prepaid application payload, staging and wire bytes for each stdio response ID.
#[derive(Default)]
struct RequestLeases(std::sync::Mutex<RequestLeaseState>);

#[derive(Default)]
struct RequestLeaseState {
    closed: bool,
    entries: std::collections::HashMap<
        rmcp::model::RequestId,
        (lease::Lease, lease::Lease, lease::Owner),
    >,
    cancelled: std::collections::VecDeque<(rmcp::model::RequestId, crate::lease::Owner)>,
}

const MAX_RECENT_CANCELLED: usize = 1024;

impl RequestLeases {
    fn on_request(&self, id: &rmcp::model::RequestId) {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.cancelled.retain(|(entry, _)| entry != id);
    }

    // A complete queued response remains deliverable after a late cancellation.
    #[cfg(test)]
    fn on_cancel(&self, id: &rmcp::model::RequestId) -> bool {
        self.on_cancel_owned(id, crate::lease::Owner::default())
    }

    fn on_cancel_owned(&self, id: &rmcp::model::RequestId, owner: crate::lease::Owner) -> bool {
        let mut state = self.0.lock().unwrap_or_else(|p| p.into_inner());
        if state.closed || state.entries.contains_key(id) {
            return false;
        }
        if !state.cancelled.iter().any(|(entry, _)| entry == id) {
            if state.cancelled.len() >= MAX_RECENT_CANCELLED {
                state.cancelled.pop_front();
            }
            state
                .cancelled
                .push_back((id.clone(), ingress::retain_id(&owner, id)));
        }
        true
    }

    fn deliver(
        &self,
        id: rmcp::model::RequestId,
        cancel: &CancellationToken,
        result: CallToolResult,
        staging: lease::Lease,
        outbound: lease::Lease,
        payload_owner: lease::Owner,
    ) -> CallToolResult {
        let mut state = self.0.lock().unwrap_or_else(|p| p.into_inner());
        if state.closed
            || cancel.is_cancelled()
            || state.cancelled.iter().any(|(entry, _)| entry == &id)
        {
            drop(state);
            drop(result);
            drop((staging, outbound, payload_owner));
            return CallToolResult::error(vec![ContentBlock::text(
                "Cancelled before the response was queued.",
            )]);
        }
        let replaced = state.entries.insert(id, (staging, outbound, payload_owner));
        drop(state);
        drop(replaced);
        result
    }

    #[cfg(test)]
    fn insert(
        &self,
        id: rmcp::model::RequestId,
        cancel: &CancellationToken,
        staging: lease::Lease,
        outbound: lease::Lease,
    ) {
        self.deliver(
            id,
            cancel,
            CallToolResult::success(Vec::new()),
            staging,
            outbound,
            Default::default(),
        );
    }

    fn take(
        &self,
        id: &rmcp::model::RequestId,
    ) -> Option<(lease::Lease, lease::Lease, lease::Owner)> {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.cancelled.retain(|(entry, _)| entry != id);
        state.entries.remove(id)
    }

    fn close(&self) {
        let mut state = self.0.lock().unwrap_or_else(|p| p.into_inner());
        state.closed = true;
        state.cancelled.clear();
        // The SDK may still own queued responses during shutdown. Their leases
        // remain with the session until every producer and transport owner ends.
    }
}

const STDIO_MAX_LINE_BYTES: usize = 16 << 20;
const UTF8_BOM: &[u8; 3] = b"\xEF\xBB\xBF";

enum StdioLine {
    Line,
    Oversized,
    Overloaded,
    End,
}

enum StdioReadStep {
    Line,
    More,
    EndDiscard,
    Oversized,
    OversizedNeedsDiscard,
}

/// Custom MCP stdio transport that pairs request-ID leases with exact-capacity `Vec<u8>` writes so
/// cancelled calls, interleaved progress notifications, and large responses never leak capacity.
struct StdioTransport<R, W> {
    read: tokio::io::BufReader<R>,
    line_buf: ingress::Buffer,
    max_line_length: usize,
    discarding: bool,
    write: Arc<tokio::sync::Mutex<Option<W>>>,
    leases: Arc<RequestLeases>,
}

impl<R, W> StdioTransport<R, W>
where
    R: tokio::io::AsyncRead + Unpin,
{
    fn new(read: R, write: W, leases: Arc<RequestLeases>) -> Self {
        Self {
            read: tokio::io::BufReader::new(read),
            line_buf: ingress::Buffer::default(),
            max_line_length: STDIO_MAX_LINE_BYTES,
            discarding: false,
            write: Arc::new(tokio::sync::Mutex::new(Some(write))),
            leases,
        }
    }

    fn reset_line_buf(&mut self) {
        self.line_buf = ingress::Buffer::default();
    }

    async fn read_line(&mut self) -> StdioLine {
        use tokio::io::AsyncBufReadExt;
        loop {
            let (consumed, step) = {
                let Ok(available) = self.read.fill_buf().await else {
                    return StdioLine::End;
                };
                if available.is_empty() {
                    return StdioLine::End;
                }
                let newline = available.iter().position(|&b| b == b'\n');
                let take = newline.map_or(available.len(), |idx| idx + 1);
                if self.discarding {
                    (
                        take,
                        newline
                            .map(|_| StdioReadStep::EndDiscard)
                            .unwrap_or(StdioReadStep::More),
                    )
                } else if self.line_buf.len().saturating_add(take) > self.max_line_length {
                    self.reset_line_buf();
                    (
                        take,
                        if newline.is_some() {
                            StdioReadStep::Oversized
                        } else {
                            StdioReadStep::OversizedNeedsDiscard
                        },
                    )
                } else {
                    if self
                        .line_buf
                        .append(&available[..take], self.max_line_length)
                        .is_err()
                    {
                        self.reset_line_buf();
                        return StdioLine::Overloaded;
                    }
                    (
                        take,
                        if newline.is_some() {
                            StdioReadStep::Line
                        } else {
                            StdioReadStep::More
                        },
                    )
                }
            };
            self.read.consume(consumed);
            match step {
                StdioReadStep::Line => return StdioLine::Line,
                StdioReadStep::More => {}
                StdioReadStep::EndDiscard => self.discarding = false,
                StdioReadStep::Oversized => return StdioLine::Oversized,
                StdioReadStep::OversizedNeedsDiscard => {
                    self.discarding = true;
                    return StdioLine::Oversized;
                }
            }
        }
    }
}

impl<R, W: tokio::io::AsyncWrite + Unpin> StdioTransport<R, W> {
    async fn refuse_request_budget(&mut self) {
        let Some(_lease) = lease::control_pool().try_acquire(1024) else {
            return;
        };
        let reply = rmcp::service::TxJsonRpcMessage::<RoleServer>::error(
            rmcp::model::ErrorData::internal_error(
                "Server active memory budget is full while reading the request.",
                None,
            ),
            None,
        );
        if let Ok(bytes) = encode_stdio_frame(&reply, None) {
            let _ = write_stdio_frame(&self.write, &bytes).await;
        }
    }
}

impl<R, W> Drop for StdioTransport<R, W> {
    fn drop(&mut self) {
        self.leases.close();
    }
}

fn is_standard_mcp_notification(method: &str) -> bool {
    matches!(
        method,
        "notifications/cancelled"
            | "notifications/initialized"
            | "notifications/message"
            | "notifications/progress"
            | "notifications/prompts/list_changed"
            | "notifications/resources/list_changed"
            | "notifications/resources/updated"
            | "notifications/roots/list_changed"
            | "notifications/subscriptions/acknowledged"
            | "notifications/tools/list_changed"
    )
}

fn is_standard_mcp_method(method: &str) -> bool {
    matches!(
        method,
        "initialize"
            | "ping"
            | "server/discover"
            | "prompts/get"
            | "prompts/list"
            | "resources/list"
            | "resources/read"
            | "resources/subscribe"
            | "resources/unsubscribe"
            | "resources/templates/list"
            | "subscriptions/listen"
            | "tools/call"
            | "tools/list"
            | "completion/complete"
            | "elicitation/create"
            | "logging/setLevel"
            | "roots/list"
            | "sampling/createMessage"
    ) || is_standard_mcp_notification(method)
}

async fn write_stdio_frame<W: tokio::io::AsyncWrite + Unpin>(
    write: &tokio::sync::Mutex<Option<W>>,
    bytes: &[u8],
) -> std::io::Result<()> {
    let mut guard = write.lock().await;
    let Some(writer) = guard.as_mut() else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotConnected,
            "stdio transport is closed",
        ));
    };
    tokio::io::AsyncWriteExt::write_all(writer, bytes).await?;
    tokio::io::AsyncWriteExt::flush(writer).await
}

fn encode_stdio_frame<T: serde::Serialize>(
    item: &T,
    outbound: Option<&mut lease::Lease>,
) -> std::io::Result<Vec<u8>> {
    let length = ipc::json_len(item)?.checked_add(1).ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "stdio frame is too large")
    })?;
    if length > ipc::RESPONSE_LIMIT {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "stdio frame exceeds 512 MiB",
        ));
    }
    if let Some(lease) = outbound
        && !lease.try_resize(length)
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::OutOfMemory,
            "stdio frame exceeded active memory budget",
        ));
    }
    let mut bytes = Vec::with_capacity(length);
    serde_json::to_writer(&mut bytes, item).map_err(std::io::Error::other)?;
    bytes.push(b'\n');
    Ok(bytes)
}

struct StdioSendPayload {
    item: rmcp::service::TxJsonRpcMessage<RoleServer>,
    staging: Option<lease::Lease>,
    outbound: Option<lease::Lease>,
    payload_owner: lease::Owner,
}

struct StdioFrame {
    bytes: Vec<u8>,
    _lease: Option<lease::Lease>,
}

impl StdioSendPayload {
    fn encode(mut self) -> std::io::Result<StdioFrame> {
        if self.outbound.is_none() {
            let size = ipc::json_len(&self.item)?.saturating_add(1);
            self.outbound = lease::process_pool().try_acquire(size).or_else(|| {
                (size <= 64 * 1024)
                    .then(|| lease::control_pool().try_acquire(size))
                    .flatten()
            });
            if self.outbound.is_none() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::OutOfMemory,
                    "stdio frame exceeded active memory budget",
                ));
            }
        }
        let bytes = encode_stdio_frame(&self.item, self.outbound.as_mut())?;
        drop(self.item);
        drop(self.staging);
        drop(self.payload_owner);
        Ok(StdioFrame {
            bytes,
            _lease: self.outbound,
        })
    }
}

impl<R, W> rmcp::transport::Transport<RoleServer> for StdioTransport<R, W>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
    W: tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    type Error = std::io::Error;

    fn send(
        &mut self,
        item: rmcp::service::TxJsonRpcMessage<RoleServer>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'static {
        let write = Arc::clone(&self.write);
        let leases = Arc::clone(&self.leases);
        let response_id = match &item {
            rmcp::model::JsonRpcMessage::Response(resp) => Some(&resp.id),
            rmcp::model::JsonRpcMessage::Error(err) => err.id.as_ref(),
            _ => None,
        };
        let (staging_lease, outbound_lease, payload_owner) =
            match response_id.and_then(|id| leases.take(id)) {
                Some((staging, outbound, owner)) => (Some(staging), Some(outbound), owner),
                None => (None, None, Default::default()),
            };
        let payload = StdioSendPayload {
            item,
            staging: staging_lease,
            outbound: outbound_lease,
            payload_owner,
        };
        async move {
            let frame = payload.encode()?;
            write_stdio_frame(&write, &frame.bytes).await
        }
    }

    async fn receive(&mut self) -> Option<rmcp::service::RxJsonRpcMessage<RoleServer>> {
        loop {
            match self.read_line().await {
                StdioLine::Line => {}
                StdioLine::Oversized => continue,
                StdioLine::Overloaded => {
                    self.refuse_request_budget().await;
                    return None;
                }
                StdioLine::End => return None,
            }
            let parsed = std::mem::take(&mut self.line_buf).parse(true);
            match parsed {
                Ok(Some(parsed)) => {
                    let msg = parsed.into_message();
                    match &msg {
                        rmcp::model::JsonRpcMessage::Request(req) => {
                            self.leases.on_request(&req.id);
                        }
                        rmcp::model::JsonRpcMessage::Notification(noti) => {
                            if let rmcp::model::ClientNotification::CancelledNotification(cancel) =
                                &noti.notification
                                && let Some(id) = &cancel.params.request_id
                                && !self.leases.on_cancel_owned(id, {
                                    use rmcp::model::GetExtensions;
                                    noti.notification
                                        .extensions()
                                        .get::<crate::lease::Owner>()
                                        .cloned()
                                        .unwrap_or_default()
                                })
                            {
                                continue;
                            }
                        }
                        _ => {}
                    }
                    return Some(msg);
                }
                Ok(None) => continue,
                Err(ingress::Failure::Budget) => {
                    self.refuse_request_budget().await;
                    return None;
                }
                Err(ingress::Failure::Syntax) => {}
                Err(ingress::Failure::Data | ingress::Failure::Limit) => {
                    let reply = rmcp::service::TxJsonRpcMessage::<RoleServer>::error(
                        rmcp::model::ErrorData::invalid_request("Invalid request", None),
                        None,
                    );
                    let Ok(bytes) = encode_stdio_frame(&reply, None) else {
                        return None;
                    };
                    if write_stdio_frame(&self.write, &bytes).await.is_err() {
                        return None;
                    }
                }
            }
        }
    }

    async fn close(&mut self) -> Result<(), Self::Error> {
        self.leases.close();
        let mut guard = self.write.lock().await;
        drop(guard.take());
        Ok(())
    }
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
fn progress_notification(
    token: &rmcp::model::ProgressToken,
    seconds: f64,
    message: &str,
    input_owner: &crate::lease::Owner,
    _http_response: bool,
) -> Option<rmcp::model::ServerNotification> {
    use rmcp::model::GetExtensions;
    // This owns the application's message allocation. SDK encodings are outside
    // the boundary; stdio acquires its own exact frame owner in Transport::send.
    let held = lease::process_pool()
        .try_acquire(message.len())
        .or_else(|| {
            (message.len() <= 64 * 1024)
                .then(|| lease::control_pool().try_acquire(message.len()))
                .flatten()
        })?;
    let mut notification = rmcp::model::ServerNotification::ProgressNotification(
        rmcp::model::ProgressNotification::new(
            rmcp::model::ProgressNotificationParam::new(token.clone(), seconds)
                .with_message(message),
        ),
    );
    notification
        .extensions_mut()
        .insert(crate::lease::Owner::new((input_owner.clone(), held)));
    Some(notification)
}

async fn forward_progress(
    peer: rmcp::service::Peer<RoleServer>,
    token: rmcp::model::ProgressToken,
    mut latest: watch::Receiver<Option<wait::Progress>>,
    owner: crate::lease::Owner,
    http_response: bool,
) {
    while latest.changed().await.is_ok() {
        let notification = {
            let latest = latest.borrow_and_update();
            let Some((seconds, message)) = latest.as_ref() else {
                continue;
            };
            progress_notification(&token, *seconds, message, &owner, http_response)
        };
        let Some(notification) = notification else {
            continue;
        };
        if peer.send_notification(notification).await.is_err() {
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
    let owner = context
        .extensions
        .get::<crate::lease::Owner>()
        .cloned()
        .unwrap_or_default();
    let (sender, latest) = watch::channel(None);
    let future = forward_progress(
        context.peer.clone(),
        token,
        latest,
        owner.clone(),
        http_context(context),
    );
    let forwarder = Forwarder(tokio::spawn(owner.hold_future(future)));
    (Some(sender), Some(forwarder))
}

fn main() {
    #[cfg(unix)]
    // Keep every child waitable before starting any thread or helper. Inheriting SIG_IGN or
    // SA_NOCLDWAIT would let the kernel reuse a task host's PID before its Tree is finished.
    unsafe {
        let mut action: libc::sigaction = std::mem::zeroed();
        action.sa_sigaction = libc::SIG_DFL;
        libc::sigemptyset(&mut action.sa_mask);
        if libc::sigaction(libc::SIGCHLD, &action, std::ptr::null_mut()) == -1 {
            eprintln!(
                "fastexec: cannot retain child exit statuses: {}",
                std::io::Error::last_os_error()
            );
            std::process::exit(1);
        }
    }
    let args: Vec<std::ffi::OsString> = std::env::args_os().skip(1).collect();
    #[cfg(unix)]
    {
        if args.first().is_some_and(|arg| arg == host::FLAG) {
            host::main(&args[1..]);
        }
    }
    if args.first().is_some_and(|arg| arg == process::ENV_FLAG) {
        process::dump_env();
    }
    if args.first().is_some_and(|arg| arg == "daemon") {
        std::process::exit(daemon_main(&args[1..]));
    }
    match args.as_slice() {
        [] => serve(),
        [arg] if arg == "--version" || arg == "-V" => {
            println!("fastexec {}", env!("CARGO_PKG_VERSION"));
            std::process::exit(0);
        }
        [arg] if arg == "--help" || arg == "-h" => {
            println!("{CLI_USAGE}");
            std::process::exit(0);
        }
        [first, extra, ..]
            if first == "--version" || first == "-V" || first == "--help" || first == "-h" =>
        {
            eprintln!(
                "fastexec: unknown argument {}\n{CLI_USAGE}",
                extra.to_string_lossy()
            );
            std::process::exit(2);
        }
        [unknown, ..] => {
            eprintln!(
                "fastexec: unknown argument {}\n{CLI_USAGE}",
                unknown.to_string_lossy()
            );
            std::process::exit(2);
        }
    }
}

const CLI_USAGE: &str = "\
usage: fastexec [--help | --version]
       fastexec daemon start [--http <addr>] [--idle-exit on|off]
       fastexec daemon run [--http <addr>]
       fastexec daemon status
       fastexec daemon stop
       fastexec daemon emit [--namespace <ns>] [--source <src>] [--topic <topic>] [--stream]

With no arguments, fastexec serves MCP over stdio.";

/// The hidden `daemon` subcommand: the daemon process itself, and the CLI that starts, inspects,
/// and stops one.
fn daemon_main(args: &[std::ffi::OsString]) -> i32 {
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
struct DaemonFlags {
    http: Option<String>,
    idle_exit: Option<bool>,
    namespace: Option<std::ffi::OsString>,
    source: Option<String>,
    topic: Option<String>,
    stream: bool,
}

fn daemon_flags(action: &str, args: &[std::ffi::OsString]) -> Result<DaemonFlags, String> {
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
    // One deadline for the whole command, so a daemon that stops answering cannot hold the CLI.
    let deadline = std::time::Instant::now() + runtime::START_WAIT;
    match action {
        "start" => cli_start(&identity, &paths, &launch, deadline, flags).await,
        "status" => cli_status(&identity, &paths, &launch, deadline).await,
        "emit" => cli_emit(&identity, &paths, deadline, flags).await,
        _ => cli_stop(&identity, &paths, &launch, deadline).await,
    }
}

/// Parses a single JSON object from stdin into an [`events::ExternalEventInput`], applying CLI
/// `--source` / `--topic` defaults and accepting `eventId` / `truncated` aliases from `mcpcli`.
fn parse_emit_payload(
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

    let str_field = |key: &str| -> Result<Option<String>, String> {
        match obj.get(key) {
            None | Some(serde_json::Value::Null) => Ok(None),
            Some(serde_json::Value::String(s)) => Ok(Some(s.clone())),
            Some(_) => Err(format!("field `{key}` must be a string")),
        }
    };
    let bool_field = |key: &str| -> Result<Option<bool>, String> {
        match obj.get(key) {
            None | Some(serde_json::Value::Null) => Ok(None),
            Some(serde_json::Value::Bool(b)) => Ok(Some(*b)),
            Some(_) => Err(format!("field `{key}` must be a boolean")),
        }
    };

    let event_type = str_field("eventType")?;
    let source = str_field("source")?
        .filter(|s| !s.trim().is_empty())
        .or_else(|| default_source.map(str::to_owned))
        .ok_or_else(|| "missing required `source` (provide in JSON or via --source)".to_owned())?;
    let topic = str_field("topic")?
        .filter(|s| !s.trim().is_empty())
        .or_else(|| default_topic.map(str::to_owned));
    let name = str_field("name")?
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| "missing required `name`".to_owned())?;
    let upstream_event_id = str_field("upstreamEventId")?
        .filter(|s| !s.trim().is_empty())
        .or(str_field("eventId")?.filter(|s| !s.trim().is_empty()))
        .ok_or_else(|| "missing required `upstreamEventId` (or `eventId`)".to_owned())?;
    let data = obj.get("data").cloned().unwrap_or(serde_json::Value::Null);
    let upstream_truncated = match bool_field("upstreamTruncated")? {
        Some(v) => Some(v),
        None => bool_field("truncated")?,
    };

    Ok(events::ExternalEventInput {
        event_type,
        source,
        topic,
        name,
        upstream_event_id,
        data,
        upstream_truncated,
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
            ipc::RESPONSE_LIMIT,
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
        ipc::RESPONSE_LIMIT,
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
enum BoundedLine {
    Eof,
    Line(Vec<u8>),
    TooLong,
}

/// Reads one line up to `max_bytes` (including `\r\n`). If a line exceeds `max_bytes`, drains and
/// discards bytes until the next `\n` or EOF so an unbounded single line cannot OOM the process.
async fn read_bounded_line<R: tokio::io::AsyncBufRead + Unpin>(
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

async fn cli_emit(
    identity: &runtime::Identity,
    paths: &runtime::Paths,
    deadline: std::time::Instant,
    flags: DaemonFlags,
) -> i32 {
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

/// A separate thread keeps stalled file I/O from holding up Tokio runtime shutdown.
async fn blocking_within<T: Send + 'static>(
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
    if let Err(message) = render() {
        return fail(&message);
    }
    let tools = match Tools::from_env() {
        Ok(tools) => tools,
        Err(message) => return fail(&message),
    };
    let public_root = if flags.http.is_some() || tools.transfer {
        match http::public_root() {
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
        let epoch = endpoint::handshake(&mut client, hello, identity).await?;
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
) -> Result<ipc::Owned<ToDaemon>, String> {
    let session =
        tasks::random_chars(26).map_err(|error| format!("cannot name the session: {error}"))?;
    endpoint::hello_owned(
        launch,
        identity,
        &session,
        1,
        "cli",
        service::Render {
            output_mode: service::OutputMode::Text,
            structured_content: true,
        },
    )
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
            ipc::RESPONSE_LIMIT,
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
        ipc::RESPONSE_LIMIT,
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

/// The output mode `FASTEXEC_OUTPUT_MODE` names, or the message saying what it held instead.
pub fn output_mode() -> Result<service::OutputMode, String> {
    output_mode_of(std::env::var_os("FASTEXEC_OUTPUT_MODE").as_deref())
}

fn output_mode_of(value: Option<&std::ffi::OsStr>) -> Result<service::OutputMode, String> {
    let Some(value) = value else {
        return Ok(service::OutputMode::Text);
    };
    match value.to_str() {
        Some("text") => Ok(service::OutputMode::Text),
        Some("both") => Ok(service::OutputMode::Both),
        Some("structured") => Ok(service::OutputMode::Structured),
        _ => Err(format!(
            "invalid FASTEXEC_OUTPUT_MODE {value:?}; expected text, both, or structured"
        )),
    }
}

/// What `FASTEXEC_STRUCTURED_CONTENT` asks for, or the message saying what it held instead.
pub fn structured_content() -> Result<bool, String> {
    parse_bool_env(
        "FASTEXEC_STRUCTURED_CONTENT",
        std::env::var_os("FASTEXEC_STRUCTURED_CONTENT").as_deref(),
        true,
    )
}

/// How this process renders results: both settings, or the message naming the one that is wrong.
pub fn render() -> Result<Render, String> {
    Ok(Render {
        output_mode: output_mode()?,
        structured_content: structured_content()?,
    })
}

/// What `FASTEXEC_YOLO` asks for, or the message saying what it held instead.
pub fn yolo() -> Result<bool, String> {
    parse_bool_env(
        "FASTEXEC_YOLO",
        std::env::var_os("FASTEXEC_YOLO").as_deref(),
        false,
    )
}

/// Whether this server advertises JavaScript task orchestration.
pub fn codemode() -> Result<bool, String> {
    parse_bool_env(
        "FASTEXEC_CODEMODE",
        std::env::var_os("FASTEXEC_CODEMODE").as_deref(),
        false,
    )
}

/// Whether this server advertises `/t/` file transfer capabilities.
pub fn transfer() -> Result<bool, String> {
    parse_bool_env(
        "FASTEXEC_TRANSFER",
        std::env::var_os("FASTEXEC_TRANSFER").as_deref(),
        false,
    )
}

/// How `FASTEXEC_INSTRUCTIONS_TOOL` exposes server instructions as an MCP tool.
pub fn instructions_tool_mode() -> Result<InstructionsToolMode, String> {
    instructions_tool_mode_of(std::env::var_os(INSTRUCTIONS_TOOL).as_deref())
}

fn instructions_tool_mode_of(
    value: Option<&std::ffi::OsStr>,
) -> Result<InstructionsToolMode, String> {
    let Some(value) = value else {
        return Ok(InstructionsToolMode::Off);
    };
    match value.to_str() {
        Some("off") => Ok(InstructionsToolMode::Off),
        Some("move") => Ok(InstructionsToolMode::Move),
        Some("copy") => Ok(InstructionsToolMode::Copy),
        _ => Err(format!(
            "invalid {INSTRUCTIONS_TOOL} {value:?}; expected off, move, or copy"
        )),
    }
}

fn parse_bool_env(
    name: &str,
    value: Option<&std::ffi::OsStr>,
    default: bool,
) -> Result<bool, String> {
    let Some(value) = value else {
        return Ok(default);
    };
    match value.to_str() {
        Some("false") => Ok(false),
        Some("true") => Ok(true),
        _ => Err(format!("invalid {name} {value:?}; expected true or false")),
    }
}

/// The tokens a `--http` daemon admits, separated by ASCII whitespace. Diagnostics name
/// positions only, so a refused configuration never exposes a credential.
pub fn http_token() -> Result<Vec<String>, String> {
    http_tokens(std::env::var_os("FASTEXEC_HTTP_TOKEN").as_deref())
}

fn http_tokens(value: Option<&std::ffi::OsStr>) -> Result<Vec<String>, String> {
    let value = value.map_or(&[][..], std::ffi::OsStr::as_encoded_bytes);
    let mut tokens = Vec::new();
    for (index, token) in value
        .split(u8::is_ascii_whitespace)
        .filter(|token| !token.is_empty())
        .enumerate()
    {
        let position = index + 1;
        if token.len() < 16 || !token.iter().all(|byte| (0x21..=0x7e).contains(byte)) {
            return Err(format!(
                "FASTEXEC_HTTP_TOKEN token {position} must be at least 16 visible ASCII characters"
            ));
        }
        let token = std::str::from_utf8(token).expect("visible ASCII");
        if let Some(previous) = tokens.iter().position(|prior| prior == token) {
            return Err(format!(
                "FASTEXEC_HTTP_TOKEN token {position} duplicates token {}",
                previous + 1
            ));
        }
        tokens.push(token.to_owned());
    }
    if tokens.is_empty() {
        return Err(
            "FASTEXEC_HTTP_TOKEN token 1 is missing; at least one token is required".to_owned(),
        );
    }
    Ok(tokens)
}

pub(crate) fn check_listen_addr(
    label: &str,
    value: &str,
    allow_remote: bool,
) -> Result<std::net::SocketAddr, String> {
    let addr: std::net::SocketAddr = value
        .parse()
        .map_err(|_| format!("{label} {value} is not an address (host:port)"))?;
    if !addr.ip().is_loopback() && !allow_remote {
        return Err(format!(
            "{label} {value} is not a loopback address; set FASTEXEC_HTTP_ALLOW_REMOTE=1 only behind a TLS-terminating proxy"
        ));
    }
    Ok(addr)
}

pub(crate) fn check_routable_addr(
    label: &str,
    addr: std::net::SocketAddr,
    has_public_root: bool,
) -> Result<(), String> {
    if !has_public_root && addr.ip().is_unspecified() {
        return Err(format!(
            "{label} {addr} is an unspecified address; set {} to the URL clients reach",
            http::PUBLIC_URL
        ));
    }
    Ok(())
}

/// The address a `--http` argument names, refused when it is outside loopback and serving there
/// was not allowed.
pub fn http_addr(value: &str) -> Result<std::net::SocketAddr, String> {
    check_listen_addr("--http", value, http::remote_allowed())
}

pub(crate) fn transfer_addr_of(
    value: Option<&std::ffi::OsStr>,
    allow_remote: bool,
) -> Result<Option<std::net::SocketAddr>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    let Some(text) = value.to_str() else {
        return Err(format!("{TRANSFER_ADDR} is not UTF-8"));
    };
    check_listen_addr(TRANSFER_ADDR, text, allow_remote).map(Some)
}

pub fn transfer_addr() -> Result<Option<std::net::SocketAddr>, String> {
    transfer_addr_of(
        std::env::var_os(TRANSFER_ADDR).as_deref(),
        http::remote_allowed(),
    )
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
    let tools = match Tools::from_env() {
        Ok(tools) => tools,
        Err(message) => {
            eprintln!("fastexec: {message}");
            std::process::exit(1);
        }
    };
    let instructions_tool = match instructions_tool_mode() {
        Ok(mode) => mode,
        Err(message) => {
            eprintln!("fastexec: {message}");
            std::process::exit(1);
        }
    };
    let transfer_cfg = if tools.transfer {
        let root = match http::public_root() {
            Ok(root) => root,
            Err(message) => {
                eprintln!("fastexec: {message}");
                std::process::exit(1);
            }
        };
        let addr = match transfer_addr() {
            Ok(addr) => addr,
            Err(message) => {
                eprintln!("fastexec: {message}");
                std::process::exit(1);
            }
        };
        Some((root, addr))
    } else {
        None
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
    let extra_instructions = match instructions_file(&launch) {
        Ok(extra) => extra,
        Err(message) => {
            eprintln!("fastexec: {message}");
            std::process::exit(1);
        }
    };
    let (backend, tasks, transfer_endpoint, transfer_stop) = if daemon {
        let endpoint = match endpoint::Endpoint::new(Arc::clone(&launch), render) {
            Ok(endpoint) => endpoint,
            Err(error) => {
                eprintln!("fastexec: cannot name this endpoint's session: {error}");
                std::process::exit(1);
            }
        };
        (Backend::Daemon(Arc::new(endpoint)), None, None, None)
    } else {
        let (transfer_endpoint, transfer_stop) = match transfer_cfg {
            Some((public_root, requested_addr)) => {
                if let Some(addr) = requested_addr
                    && let Err(message) =
                        check_routable_addr(TRANSFER_ADDR, addr, public_root.is_some())
                {
                    eprintln!("fastexec: {message}");
                    std::process::exit(1);
                }
                let addr = requested_addr
                    .unwrap_or_else(|| std::net::SocketAddr::from(([127, 0, 0, 1], 0)));
                let listener = match tokio::net::TcpListener::bind(addr).await {
                    Ok(listener) => listener,
                    Err(error) => {
                        eprintln!("fastexec: cannot listen on {addr} for transfer: {error}");
                        std::process::exit(1);
                    }
                };
                let bound = match listener.local_addr() {
                    Ok(bound) => bound,
                    Err(error) => {
                        eprintln!("fastexec: cannot read transfer listener address: {error}");
                        std::process::exit(1);
                    }
                };
                let table = Arc::new(transfer::Transfers::new());
                let stop = CancellationToken::new();
                tokio::spawn(http::serve_transfers(
                    listener,
                    Arc::clone(&table),
                    stop.clone(),
                ));
                let base: Arc<str> =
                    Arc::from(public_root.unwrap_or_else(|| format!("http://{bound}/")));
                (Some(TransferEndpoint::Fixed { table, base }), Some(stop))
            }
            None => (None, None),
        };
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
            daemon: false,
        };
        (backend, Some(tasks), transfer_endpoint, transfer_stop)
    };
    let stdio_leases = Arc::new(RequestLeases::default());
    let server = Server::new(
        backend,
        &launch,
        transfer_endpoint,
        tools,
        extra_instructions.as_deref(),
        instructions_tool,
    )
    .with_stdio_leases(Arc::clone(&stdio_leases));
    let eof = Arc::new(tokio::sync::Notify::new());
    let stdin = EofSignal {
        inner: tokio::io::stdin(),
        eof: Arc::clone(&eof),
    };
    let transport = StdioTransport::new(stdin, tokio::io::stdout(), stdio_leases);
    // Signals are watched from the start, so an exit during initialization still cleans up.
    let signal = shutdown_signal();
    tokio::pin!(signal);
    // The backend outlives the serve call, which consumes the handler.
    let backend = server.backend.clone();
    let served = tokio::select! {
        served = server.serve(transport) => Some(served),
        _ = &mut signal => None,
    };
    let mut exit_code = 0;
    match served {
        Some(Ok(service)) => {
            // Stdin EOF ends the session at once; rmcp would first drain in-flight waits.
            tokio::select! {
                _ = service.waiting() => {}
                _ = eof.notified() => {}
                _ = &mut signal => {}
            }
        }
        Some(Err(error)) => {
            eprintln!("fastexec: MCP initialization failed: {error}");
            exit_code = 1;
        }
        None => {}
    }
    match &backend {
        // The endpoint never kills tasks: they belong to the daemon, and other sessions may be
        // using them.
        Backend::Daemon(endpoint) => endpoint.bye().await,
        Backend::Local { .. } => {
            if let Some(stop) = transfer_stop {
                stop.cancel();
            }
            if let Some(tasks) = tasks {
                tasks.shutdown();
            }
        }
    }
    std::process::exit(exit_code);
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
            "start",
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

#[cfg(test)]
mod http_token_tests {
    use super::http_tokens;
    use std::ffi::OsStr;

    #[test]
    fn several_tokens_keep_their_positions_across_ascii_whitespace() {
        let first = "0123456789abcdef";
        let second = "fedcba9876543210";
        let third = "visible!@#$%^&*()";
        let value = format!(" \t{first}\n{second}\r\n{third}\x0c ");
        assert_eq!(
            http_tokens(Some(OsStr::new(&value))).unwrap(),
            [first, second, third]
        );
    }

    #[test]
    fn missing_short_non_ascii_and_duplicate_tokens_name_positions_only() {
        let good = "0123456789abcdef";
        for value in [None, Some(""), Some(" \t\r\n\x0c")] {
            let error = http_tokens(value.map(OsStr::new)).unwrap_err();
            assert!(error.contains("token 1"), "{error}");
        }
        for bad in ["secret-short", "0123456789abcdefé", "0123456789abcdef\0"] {
            let value = format!("{good} {bad}");
            let error = http_tokens(Some(OsStr::new(&value))).unwrap_err();
            assert!(error.contains("token 2"), "{error}");
            assert!(!error.contains(good) && !error.contains(bad), "{error}");
        }
        let value = format!("{good} fedcba9876543210 {good}");
        let error = http_tokens(Some(OsStr::new(&value))).unwrap_err();
        assert!(error.contains("token 3 duplicates token 1"), "{error}");
        assert!(!error.contains(good), "{error}");
    }

    #[cfg(unix)]
    #[test]
    fn non_utf8_tokens_are_refused_at_their_position() {
        use std::os::unix::ffi::OsStrExt;
        let error = http_tokens(Some(OsStr::from_bytes(
            b"0123456789abcdef abcdef0123456789\xff",
        )))
        .unwrap_err();
        assert!(error.contains("token 2"), "{error}");
        assert!(!error.contains("abcdef"), "{error}");
    }
}

#[cfg(test)]
mod transfer_owner_tests {
    use super::*;
    use std::future::Future;
    use std::time::Duration;

    fn launch() -> Arc<LaunchContext> {
        Arc::new(LaunchContext::new(
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")),
            Vec::new(),
        ))
    }

    #[tokio::test]
    async fn transfer_owner_survives_unpolled_future_and_completed_inspection() {
        let pool = lease::BytePool::new(64);
        let request = || transfer::Request {
            path: "Cargo.toml".into(),
        };
        let pending = inspect_transfer_request(
            request(),
            launch(),
            crate::lease::Owner::new(pool.try_acquire(64).unwrap()),
        );
        assert_eq!(pool.used(), 64);
        drop(pending);
        assert_eq!(pool.used(), 0);

        for path in ["Cargo.toml", "."] {
            let inspected = inspect_transfer_request(
                transfer::Request { path: path.into() },
                launch(),
                crate::lease::Owner::new(pool.try_acquire(64).unwrap()),
            )
            .await;
            assert_eq!(inspected.value.is_ok(), path == "Cargo.toml");
            assert_eq!(pool.used(), 64);
            drop(inspected);
            assert_eq!(pool.used(), 0);
        }

        let table = Arc::new(transfer::Transfers::new());
        let result = publish_transfer(
            request(),
            launch(),
            &table,
            "http://localhost/",
            Render {
                output_mode: service::OutputMode::Both,
                structured_content: true,
            },
            crate::lease::Owner::new(pool.try_acquire(64).unwrap()),
        )
        .await;
        assert_eq!(result.result.is_error, Some(false));
        assert_eq!(pool.used(), 64);
        drop(result);
        assert_eq!(pool.used(), 0);
    }

    #[tokio::test]
    async fn transfer_path_workspace_retains_success_and_error_until_consumed() {
        for path in ["Cargo.toml", "missing-transfer-file", "."] {
            let pool = lease::BytePool::new(64 * 1024);
            let context = Arc::new(
                LaunchContext::new(
                    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")),
                    Vec::new(),
                )
                .with_memory(Arc::clone(&pool)),
            );
            let result = inspect_transfer_request(
                transfer::Request { path: path.into() },
                context,
                Default::default(),
            )
            .await;
            assert_eq!(result.value.is_ok(), path != ".");
            if path == "missing-transfer-file" {
                assert_eq!(result.value.as_ref().unwrap().2, None);
            }
            assert!(pool.used() > 0);
            drop(result);
            assert_eq!(pool.used(), 0);
        }
        let pool = lease::BytePool::new(0);
        let context = Arc::new(
            LaunchContext::new(
                std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")),
                Vec::new(),
            )
            .with_memory(Arc::clone(&pool)),
        );
        let result = inspect_transfer_request(
            transfer::Request {
                path: "Cargo.toml".into(),
            },
            context,
            Default::default(),
        )
        .await;
        assert!(
            result
                .value
                .as_ref()
                .unwrap_err()
                .contains("not dispatched")
        );
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn transfer_owner_stays_with_a_queued_worker_after_waiter_cancellation() {
        struct Released(Option<tokio::sync::oneshot::Sender<()>>);
        impl Drop for Released {
            fn drop(&mut self) {
                let _ = self.0.take().unwrap().send(());
            }
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            let (started, running) = tokio::sync::oneshot::channel();
            let (release, wait) = std::sync::mpsc::channel();
            let blocker = tokio::task::spawn_blocking(move || {
                started.send(()).unwrap();
                wait.recv_timeout(Duration::from_secs(5)).unwrap();
            });
            running.await.unwrap();
            let pool = lease::BytePool::new(64);
            let (released, retired) = tokio::sync::oneshot::channel();
            let owner =
                crate::lease::Owner::new((pool.try_acquire(64).unwrap(), Released(Some(released))));
            let mut inspection = Box::pin(inspect_transfer_request(
                transfer::Request {
                    path: "Cargo.toml".into(),
                },
                launch(),
                owner,
            ));
            let mut context = std::task::Context::from_waker(std::task::Waker::noop());
            assert!(inspection.as_mut().poll(&mut context).is_pending());
            drop(inspection);
            assert_eq!(pool.used(), 64);
            release.send(()).unwrap();
            blocker.await.unwrap();
            tokio::time::timeout(Duration::from_secs(5), retired)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(pool.used(), 0);
        });
    }
}

#[cfg(test)]
mod yolo_tests {
    use super::{
        TRANSFER_ADDR, check_listen_addr, check_routable_addr, output_mode_of, parse_bool_env,
        transfer_addr_of,
    };
    use crate::service::OutputMode;
    use std::ffi::OsStr;

    #[test]
    fn valid_and_invalid_values_match_contract() {
        assert_eq!(parse_bool_env("FASTEXEC_YOLO", None, false), Ok(false));
        assert_eq!(parse_bool_env("FASTEXEC_TRANSFER", None, false), Ok(false));
        assert_eq!(transfer_addr_of(None, false), Ok(None));
        assert_eq!(
            transfer_addr_of(Some(OsStr::new("127.0.0.1:0")), false),
            Ok(Some(std::net::SocketAddr::from(([127, 0, 0, 1], 0))))
        );
        assert!(transfer_addr_of(Some(OsStr::new("0.0.0.0:0")), false).is_err());
        assert!(transfer_addr_of(Some(OsStr::new("0.0.0.0:0")), true).is_ok());
        for unspecified in ["0.0.0.0:0", "[::]:0"] {
            let addr: std::net::SocketAddr = unspecified.parse().unwrap();
            assert_eq!(
                check_routable_addr(TRANSFER_ADDR, addr, false).unwrap_err(),
                format!(
                    "{TRANSFER_ADDR} {addr} is an unspecified address; set FASTEXEC_PUBLIC_URL to the URL clients reach"
                )
            );
            assert!(check_routable_addr(TRANSFER_ADDR, addr, true).is_ok());
        }
        for specific in ["127.0.0.1:0", "[::1]:0", "192.0.2.1:8080"] {
            let addr: std::net::SocketAddr = specific.parse().unwrap();
            assert!(check_routable_addr(TRANSFER_ADDR, addr, false).is_ok());
        }
        assert_eq!(
            check_listen_addr("--http", "0.0.0.0:0", false).unwrap_err(),
            "--http 0.0.0.0:0 is not a loopback address; set FASTEXEC_HTTP_ALLOW_REMOTE=1 only behind a TLS-terminating proxy"
        );
        assert_eq!(
            parse_bool_env("FASTEXEC_YOLO", Some(OsStr::new("false")), false),
            Ok(false)
        );
        assert_eq!(
            parse_bool_env("FASTEXEC_YOLO", Some(OsStr::new("true")), false),
            Ok(true)
        );
        assert_eq!(
            parse_bool_env("FASTEXEC_STRUCTURED_CONTENT", None, true),
            Ok(true)
        );
        assert_eq!(output_mode_of(None), Ok(OutputMode::Text));
        assert_eq!(
            output_mode_of(Some(OsStr::new("both"))),
            Ok(OutputMode::Both)
        );
        for bad in ["", "on", "off", "1", "0", "TRUE", "True", " true "] {
            let error = parse_bool_env("FASTEXEC_YOLO", Some(OsStr::new(bad)), false).unwrap_err();
            assert_eq!(
                error,
                format!("invalid FASTEXEC_YOLO {bad:?}; expected true or false")
            );
            let mode_error = output_mode_of(Some(OsStr::new(bad))).unwrap_err();
            assert_eq!(
                mode_error,
                format!("invalid FASTEXEC_OUTPUT_MODE {bad:?}; expected text, both, or structured")
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn non_utf8_value_is_refused() {
        use std::os::unix::ffi::OsStrExt;
        let bad = OsStr::from_bytes(b"\xff");
        let error = parse_bool_env("FASTEXEC_YOLO", Some(bad), false).unwrap_err();
        assert!(
            error.starts_with("invalid FASTEXEC_YOLO ")
                && error.ends_with("; expected true or false"),
            "{error}"
        );
        let mode_error = output_mode_of(Some(bad)).unwrap_err();
        assert!(
            mode_error.starts_with("invalid FASTEXEC_OUTPUT_MODE ")
                && mode_error.ends_with("; expected text, both, or structured"),
            "{mode_error}"
        );
    }

    #[cfg(windows)]
    #[test]
    fn unpaired_surrogate_value_is_refused() {
        use std::os::windows::ffi::OsStringExt;
        let value = std::ffi::OsString::from_wide(&[0xD800]);
        let error = parse_bool_env("FASTEXEC_YOLO", Some(&value), false).unwrap_err();
        assert!(
            error.starts_with("invalid FASTEXEC_YOLO ")
                && error.ends_with("; expected true or false"),
            "{error}"
        );
        let mode_error = output_mode_of(Some(&value)).unwrap_err();
        assert!(
            mode_error.starts_with("invalid FASTEXEC_OUTPUT_MODE ")
                && mode_error.ends_with("; expected text, both, or structured"),
            "{mode_error}"
        );
    }
}

#[cfg(test)]
mod portable_schema_tests {
    use super::*;
    use serde_json::{Value, json};

    /// Simulates Gemini web's MCP JSON Schema -> OpenAPI 3.0 declaration converter.
    fn gemini_convert(node: &Value) -> Value {
        let Some(obj) = node.as_object() else {
            return json!({});
        };
        let mut out = serde_json::Map::new();
        if let Some(desc) = obj.get("description") {
            out.insert("description".into(), desc.clone());
        }
        if let Some(ty) = obj.get("type").and_then(Value::as_str) {
            out.insert("type".into(), json!(ty.to_ascii_uppercase()));
        }
        if let Some(en) = obj.get("enum") {
            out.insert("enum".into(), en.clone());
        }
        if let Some(req) = obj.get("required") {
            out.insert("required".into(), req.clone());
        }
        if let Some(items) = obj.get("items") {
            out.insert("items".into(), gemini_convert(items));
        }
        if let Some(props) = obj.get("properties").and_then(Value::as_object) {
            let mut converted = serde_json::Map::new();
            for (k, v) in props {
                converted.insert(k.clone(), gemini_convert(v));
            }
            out.insert("properties".into(), Value::Object(converted));
        }
        if let Some(any_of) = obj.get("anyOf").and_then(Value::as_array) {
            out.insert(
                "any_of".into(),
                Value::Array(any_of.iter().map(gemini_convert).collect()),
            );
        }
        Value::Object(out)
    }

    fn assert_portable_and_gemini_typed(path: &str, schema: &Value, gemini: &Value) {
        let obj = schema
            .as_object()
            .unwrap_or_else(|| panic!("{path}: not an object"));
        for forbidden in [
            "$ref",
            "$defs",
            "definitions",
            "$schema",
            "anyOf",
            "oneOf",
            "allOf",
            "not",
            "prefixItems",
            "additionalItems",
            "patternProperties",
            "dependentSchemas",
            "format",
        ] {
            assert!(
                !obj.contains_key(forbidden),
                "{path} still contains {forbidden}: {schema}"
            );
        }
        assert!(
            obj.get("type").and_then(Value::as_str).is_some(),
            "{path} lacks scalar string type: {schema}"
        );
        assert!(
            gemini.get("type").and_then(Value::as_str).is_some(),
            "{path} lost its type under Gemini conversion: {gemini}"
        );
        if let Some(en) = obj.get("enum") {
            assert_eq!(
                obj.get("type"),
                Some(&json!("string")),
                "{path}: enum must be string"
            );
            assert_eq!(gemini.get("enum"), Some(en), "{path}: Gemini lost enum");
        }
        if let Some(items) = obj.get("items") {
            assert_portable_and_gemini_typed(&format!("{path}.items"), items, &gemini["items"]);
        }
        if let Some(props) = obj.get("properties").and_then(Value::as_object) {
            for (k, v) in props {
                assert_portable_and_gemini_typed(
                    &format!("{path}.{k}"),
                    v,
                    &gemini["properties"][k],
                );
            }
        }
    }

    #[tokio::test]
    async fn every_tool_schema_is_portable_and_preserves_types_in_a_modeled_gemini_conversion() {
        let tasks = Tasks::new(Limits::OFF).unwrap();
        let launch = Arc::new(LaunchContext::current().unwrap());
        let backend = Backend::Local {
            service: Arc::new(Service::new(tasks, true)),
            render: Render {
                output_mode: service::OutputMode::Text,
                structured_content: true,
            },
            session: Arc::new(OnceLock::new()),
            launch: Arc::clone(&launch),
            daemon: true,
        };
        let transfer = Some(TransferEndpoint::Fixed {
            table: Arc::new(transfer::Transfers::new()),
            base: Arc::from("http://127.0.0.1:1/"),
        });
        let server = Server::new(
            backend,
            &launch,
            transfer,
            Tools {
                codemode: true,
                transfer: true,
                yolo: false,
            },
            None,
            InstructionsToolMode::Off,
        );
        let tools = server.router.list_all();
        assert_eq!(tools.len(), 6);
        let mut by_name = std::collections::HashMap::new();
        for tool in tools {
            let schema = Value::Object((*tool.input_schema).clone());
            let gemini = gemini_convert(&schema);
            assert_portable_and_gemini_typed(&tool.name, &schema, &gemini);
            assert_eq!(
                schema["additionalProperties"],
                json!(false),
                "{}",
                tool.name
            );
            assert!(
                !tool
                    .description
                    .as_deref()
                    .unwrap_or_default()
                    .contains("libproc")
            );
            by_name.insert(tool.name.into_owned(), (schema, gemini));
        }
        let (exec_s, exec_g) = &by_name["exec"];
        let (code_s, _) = &by_name["codemode"];
        let (edit_s, edit_g) = &by_name["edit"];
        assert_eq!(
            exec_s["properties"]["action"]["enum"],
            json!(["start", "poll", "kill", "list", "transcript"])
        );
        assert_eq!(exec_g["properties"]["action"]["type"], json!("STRING"));
        assert_eq!(
            exec_g["properties"]["action"]["enum"],
            json!(["start", "poll", "kill", "list", "transcript"])
        );
        assert!(edit_s["properties"]["edits"]["items"]["properties"]["oldText"].is_object());
        assert!(edit_s["properties"]["edits"]["items"]["properties"]["newText"].is_object());
        assert_eq!(
            edit_g["properties"]["edits"]["items"]["properties"]["oldText"]["type"],
            json!("STRING")
        );
        // The wire constraints match. Each tool describes its own behavior and defaults.
        let constraints = |field: &Value| {
            let mut schema = field.as_object().unwrap().clone();
            schema.remove("description");
            schema
        };
        for shared in ["waitMs", "killAfterMs", "truncate", "maxBytes"] {
            assert_eq!(
                constraints(&code_s["properties"][shared]),
                constraints(&exec_s["properties"][shared]),
                "codemode.{shared} diverged from exec.{shared}"
            );
        }
        for rw_field in ["outputContains", "quietMs"] {
            assert_eq!(
                constraints(&code_s["properties"]["returnWhen"]["properties"][rw_field]),
                constraints(&exec_s["properties"]["returnWhen"]["properties"][rw_field]),
                "returnWhen.{rw_field} diverged"
            );
        }
        assert!(
            code_s["properties"]["killAfterMs"]["description"]
                .as_str()
                .unwrap()
                .contains("Tasks the script already started keep running")
        );
        assert_eq!(
            exec_s["properties"]["truncate"]["enum"],
            json!(["head_tail", "head", "tail"])
        );
        assert_eq!(
            exec_s["properties"]["maxBytes"]["maximum"],
            json!(4_194_304)
        );
        assert!(code_s["properties"].get("raw").is_none());

        // Chained $ref -> nullable anyOf -> oneOf inside $defs flattens cleanly while
        // preserving property names such as "format" and "type" inside `properties`.
        let tricky = portable_schema(json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$defs": {
                "Mode": {
                    "oneOf": [
                        {"const": "fast", "description": "quick"},
                        {"const": "full", "description": "complete"}
                    ]
                },
                "NullableMode": {
                    "anyOf": [{"$ref": "#/$defs/Mode"}, {"type": "null"}]
                }
            },
            "type": "object",
            "properties": {
                "mode": {"description": "Run mode.", "$ref": "#/$defs/NullableMode"},
                "format": {"type": ["string", "null"], "description": "Output format."}
            }
        }));
        let tricky_g = gemini_convert(&tricky);
        assert_portable_and_gemini_typed("tricky", &tricky, &tricky_g);
        assert_eq!(
            tricky["properties"]["mode"]["enum"],
            json!(["fast", "full"])
        );
        assert_eq!(
            tricky["properties"]["mode"]["description"],
            json!("Run mode. fast: quick full: complete")
        );
        assert_eq!(tricky["properties"]["format"]["type"], json!("string"));
    }
}

#[cfg(test)]
mod instructions_file_tests {
    use super::*;
    use std::ffi::OsString;
    use std::path::PathBuf;

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(label: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "fastexec-instructions-{label}-{}",
                tasks::random_chars(10).unwrap()
            ));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn launch_with(cwd: &std::path::Path, value: Option<OsString>) -> LaunchContext {
        let mut env = Vec::new();
        if let Some(value) = value {
            env.push((OsString::from(INSTRUCTIONS_FILE), value));
        }
        LaunchContext::new(cwd.to_path_buf(), env)
    }

    #[test]
    fn env_var_unset_empty_and_non_utf8() {
        let dir = TempDir::new("env");
        assert_eq!(instructions_file(&launch_with(&dir.0, None)), Ok(None));

        let empty_err =
            instructions_file(&launch_with(&dir.0, Some(OsString::from("")))).unwrap_err();
        assert_eq!(empty_err, "FASTEXEC_INSTRUCTIONS_FILE must not be empty");

        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStringExt;
            let bad = OsString::from_vec(vec![b'a', 0xff]);
            let err = instructions_file(&launch_with(&dir.0, Some(bad))).unwrap_err();
            assert_eq!(err, "FASTEXEC_INSTRUCTIONS_FILE is not UTF-8");
        }
    }

    #[test]
    fn rejects_missing_directory_nul_invalid_utf8_and_oversized_split_multibyte() {
        let dir = TempDir::new("rejects");

        let missing = instructions_file(&launch_with(
            &dir.0,
            Some(OsString::from("does-not-exist.txt")),
        ))
        .unwrap_err();
        assert!(
            missing.starts_with("cannot read FASTEXEC_INSTRUCTIONS_FILE "),
            "{missing}"
        );

        let subdir = dir.0.join("subdir");
        std::fs::create_dir(&subdir).unwrap();
        let dir_err =
            instructions_file(&launch_with(&dir.0, Some(OsString::from("subdir")))).unwrap_err();
        assert!(
            dir_err.starts_with("cannot read FASTEXEC_INSTRUCTIONS_FILE ")
                && dir_err.contains("it is a directory")
                && !dir_err.contains("list it with exec"),
            "{dir_err}"
        );

        let nul_path = dir.0.join("has-nul.txt");
        std::fs::write(&nul_path, b"hello\0world").unwrap();
        let nul_err = instructions_file(&launch_with(&dir.0, Some(OsString::from("has-nul.txt"))))
            .unwrap_err();
        assert!(nul_err.ends_with(" contains a NUL byte"), "{nul_err}");

        let bad_utf8 = dir.0.join("bad.txt");
        std::fs::write(&bad_utf8, b"hello\xff").unwrap();
        let utf8_err =
            instructions_file(&launch_with(&dir.0, Some(OsString::from("bad.txt")))).unwrap_err();
        assert!(utf8_err.ends_with(" is not valid UTF-8"), "{utf8_err}");

        // 16_383 ASCII bytes + 3-byte UTF-8 char = 16_386 bytes; truncated read splits '中'
        // at byte 16_385, so size check must run before UTF-8 validation.
        let oversized = dir.0.join("oversized.txt");
        let mut payload = vec![b'a'; 16_383];
        payload.extend_from_slice("中".as_bytes());
        assert_eq!(payload.len(), 16_386);
        std::fs::write(&oversized, &payload).unwrap();
        let size_err =
            instructions_file(&launch_with(&dir.0, Some(OsString::from("oversized.txt"))))
                .unwrap_err();
        assert!(size_err.ends_with(" exceeds 16384 bytes"), "{size_err}");
    }

    #[test]
    fn exact_cap_normalization_preservation_and_blank_files() {
        let dir = TempDir::new("valid");

        let exact = dir.0.join("exact.txt");
        let exact_text = "x".repeat(MAX_INSTRUCTIONS_BYTES);
        std::fs::write(&exact, &exact_text).unwrap();
        assert_eq!(
            instructions_file(&launch_with(&dir.0, Some(OsString::from("exact.txt")))),
            Ok(Some(exact_text))
        );

        let formatted = dir.0.join("formatted.txt");
        std::fs::write(&formatted, "\u{feff}    indented\r\nline 2\rline 3\n").unwrap();
        assert_eq!(
            instructions_file(&launch_with(&dir.0, Some(OsString::from("formatted.txt")))),
            Ok(Some("    indented\nline 2\nline 3\n".to_owned()))
        );

        let ws_only = dir.0.join("ws.txt");
        std::fs::write(&ws_only, "  \r\n\t \n").unwrap();
        assert_eq!(
            instructions_file(&launch_with(&dir.0, Some(OsString::from("ws.txt")))),
            Ok(None)
        );

        let bom_only = dir.0.join("bom.txt");
        std::fs::write(&bom_only, "\u{feff}\u{feff}  \r\n").unwrap();
        assert_eq!(
            instructions_file(&launch_with(&dir.0, Some(OsString::from("bom.txt")))),
            Ok(None)
        );
    }

    #[test]
    fn server_instructions_appends_extra_and_keeps_first_line() {
        for daemon_owned in [false, true] {
            for codemode in [false, true] {
                let base = server_instructions(daemon_owned, codemode, None);
                let with_extra = server_instructions(
                    daemon_owned,
                    codemode,
                    Some("    custom line\ntrailing\n"),
                );
                assert_eq!(base.lines().next(), with_extra.lines().next());
                assert_eq!(
                    with_extra.as_ref(),
                    format!("{base}\n\n    custom line\ntrailing\n")
                );
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn unix_fifo_fails_without_blocking() {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let dir = TempDir::new("fifo");
        let fifo = dir.0.join("pipe");
        let c_path = CString::new(fifo.as_os_str().as_bytes()).unwrap();
        // SAFETY: `c_path` is a valid NUL-terminated path string.
        assert_eq!(unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) }, 0);

        let started = std::time::Instant::now();
        let err =
            instructions_file(&launch_with(&dir.0, Some(OsString::from("pipe")))).unwrap_err();
        assert!(
            started.elapsed() < std::time::Duration::from_secs(2),
            "FIFO check took {:?}",
            started.elapsed()
        );
        assert!(
            err.contains("it is not a regular file"),
            "unexpected FIFO error: {err}"
        );
    }
}

#[cfg(test)]
mod instructions_tool_tests {
    use super::*;
    use serde_json::{Value, json};
    use std::ffi::OsStr;

    #[test]
    fn parser_accepts_strict_lowercase_values_and_rejects_invalid() {
        assert_eq!(
            instructions_tool_mode_of(None),
            Ok(InstructionsToolMode::Off)
        );
        assert_eq!(
            instructions_tool_mode_of(Some(OsStr::new("off"))),
            Ok(InstructionsToolMode::Off)
        );
        assert_eq!(
            instructions_tool_mode_of(Some(OsStr::new("move"))),
            Ok(InstructionsToolMode::Move)
        );
        assert_eq!(
            instructions_tool_mode_of(Some(OsStr::new("copy"))),
            Ok(InstructionsToolMode::Copy)
        );

        for bad in [
            "", "true", "false", "on", "OFF", "Move", "COPY", " off ", "move\n", "1", "0",
        ] {
            let err = instructions_tool_mode_of(Some(OsStr::new(bad))).unwrap_err();
            assert_eq!(
                err,
                format!("invalid {INSTRUCTIONS_TOOL} {bad:?}; expected off, move, or copy")
            );
        }

        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStrExt;
            let err = instructions_tool_mode_of(Some(OsStr::from_bytes(b"\xff"))).unwrap_err();
            assert!(
                err.starts_with(&format!("invalid {INSTRUCTIONS_TOOL} "))
                    && err.ends_with("; expected off, move, or copy"),
                "{err}"
            );
        }

        #[cfg(windows)]
        {
            use std::os::windows::ffi::OsStringExt;
            let value = std::ffi::OsString::from_wide(&[0xD800]);
            let err = instructions_tool_mode_of(Some(&value)).unwrap_err();
            assert!(
                err.starts_with(&format!("invalid {INSTRUCTIONS_TOOL} "))
                    && err.ends_with("; expected off, move, or copy"),
                "{err}"
            );
        }
    }

    #[test]
    fn compose_instructions_matches_spec_matrix() {
        let extra = "    host rule 1\n第二行\n";
        for daemon_owned in [false, true] {
            for codemode in [false, true] {
                let base = server_instructions(daemon_owned, codemode, None);
                let full = server_instructions(daemon_owned, codemode, Some(extra));

                let (proto, tool) =
                    compose_instructions(daemon_owned, codemode, None, InstructionsToolMode::Off);
                assert_eq!(proto, base);
                assert_eq!(tool, None);

                let (proto, tool) = compose_instructions(
                    daemon_owned,
                    codemode,
                    Some(extra),
                    InstructionsToolMode::Off,
                );
                assert_eq!(proto, full);
                assert_eq!(tool, None);

                let (proto, tool) =
                    compose_instructions(daemon_owned, codemode, None, InstructionsToolMode::Move);
                assert_eq!(proto, base);
                assert_eq!(tool.as_deref(), Some(base.as_ref()));

                let (proto, tool) = compose_instructions(
                    daemon_owned,
                    codemode,
                    Some(extra),
                    InstructionsToolMode::Move,
                );
                assert_eq!(
                    proto.as_ref(),
                    format!("{base}\n\n{MOVE_INSTRUCTIONS_POINTER}")
                );
                assert_eq!(tool.as_deref(), Some(extra));

                let (proto, tool) =
                    compose_instructions(daemon_owned, codemode, None, InstructionsToolMode::Copy);
                assert_eq!(proto, base);
                assert_eq!(tool.as_deref(), Some(base.as_ref()));

                let (proto, tool) = compose_instructions(
                    daemon_owned,
                    codemode,
                    Some(extra),
                    InstructionsToolMode::Copy,
                );
                assert_eq!(proto, full);
                assert_eq!(tool.as_deref(), Some(full.as_ref()));
            }
        }
    }

    #[test]
    fn build_router_registers_instructions_tool_with_exact_schema_and_annotations() {
        let launch = LaunchContext::current().unwrap();
        let expected_schema = json!({
            "type": "object",
            "properties": {},
            "additionalProperties": false,
        });
        let description = "    host-specific directive\n第二行\n";

        for yolo in [false, true] {
            let tools = Tools {
                codemode: false,
                transfer: false,
                yolo,
            };

            let absent = Server::build_router(&launch, tools, None);
            assert!(
                absent
                    .list_all()
                    .iter()
                    .all(|tool| tool.name != "instructions")
            );

            let present = Server::build_router(&launch, tools, Some(description));
            let tool = present
                .list_all()
                .into_iter()
                .find(|tool| tool.name == "instructions")
                .expect("instructions tool registered");
            assert_eq!(tool.description.as_deref(), Some(description));
            assert_eq!(Value::Object((*tool.input_schema).clone()), expected_schema);
            let annotations = tool.annotations.expect("instructions annotations");
            assert_eq!(annotations.title.as_deref(), Some("Server instructions"));
            assert_eq!(annotations.read_only_hint, Some(true));
            assert_eq!(annotations.destructive_hint, Some(false));
            assert_eq!(annotations.idempotent_hint, Some(!yolo));
            assert_eq!(annotations.open_world_hint, Some(false));
        }
    }
}

#[cfg(test)]
mod metadata_budget_tests {
    use super::*;

    #[test]
    fn application_input_admission_precedes_copies_and_keeps_control_for_management() {
        let args = serde_json::json!({"action":"sleep","path":"sample.txt"});
        let id = rmcp::model::RequestId::String("ordinary-id".into());
        let borrowed = (&args, &id);
        let needed = 4 * ipc::json_len(&borrowed).unwrap();
        for (limit, fits) in [(needed - 1, false), (needed, true)] {
            let pool = lease::BytePool::new(limit);
            let control = lease::BytePool::new(needed);
            let result = application_input_owner(&borrowed, None, &pool, &control, false);
            assert_eq!(result.is_ok(), fits);
            assert_eq!(control.used(), 0, "ordinary tools cannot use control space");
            drop(result);
            assert_eq!(pool.used(), 0);
        }
        let full = lease::BytePool::new(0);
        let control = lease::BytePool::new(needed);
        let held = application_input_owner(&borrowed, None, &full, &control, true).unwrap();
        assert_eq!(control.used(), needed);
        let inherited =
            application_input_owner(&borrowed, Some(&held), &full, &full, false).unwrap();
        drop(held);
        assert_eq!(
            control.used(),
            needed,
            "an existing owner is shared, not recharged"
        );
        drop(inherited);
        assert_eq!(control.used(), 0);
    }

    #[test]
    fn metadata_workspace_refuses_before_copy_and_releases_at_handoff() {
        let pool = lease::BytePool::new(32);
        let held = reserve_response_content(&pool, 32).unwrap();
        assert!(reserve_response_content(&pool, 1).is_err());
        let result = CallToolResult::success(vec![ContentBlock::text("ordinary metadata")]);
        assert_eq!(pool.used(), 32);
        // Returning this official SDK value does not transfer the application's
        // workspace. SDK-owned later allocations are explicitly out of scope.
        drop(held);
        assert_eq!(pool.used(), 0);
        assert_eq!(
            result.content[0].as_text().unwrap().text,
            "ordinary metadata"
        );
    }
}

#[cfg(test)]
mod cli_deadline_tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn pending_probe_uses_remaining_command_deadline() {
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(10);
        assert_eq!(
            within::<()>(deadline, std::future::pending())
                .await
                .unwrap_err(),
            NO_ANSWER
        );
    }

    #[test]
    fn slow_preflight_does_not_hold_runtime_shutdown() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let (release, wait) = std::sync::mpsc::channel();
        let (started, start) = std::sync::mpsc::channel();
        let (finished, done) = std::sync::mpsc::channel();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3600);
        let task = runtime.spawn(blocking_within(deadline, move || {
            started.send(()).unwrap();
            // A regression must fail instead of hanging runtime shutdown forever.
            wait.recv_timeout(std::time::Duration::from_secs(30))
                .unwrap();
            finished.send(()).unwrap();
            Ok(())
        }));
        runtime.block_on(tokio::task::yield_now());
        start
            .recv_timeout(std::time::Duration::from_secs(30))
            .unwrap();
        let result = runtime.block_on(async {
            tokio::time::pause();
            tokio::time::advance(std::time::Duration::from_secs(7200)).await;
            task.await.unwrap()
        });
        assert_eq!(result.unwrap_err(), NO_ANSWER);
        drop(runtime);
        release.send(()).unwrap();
        done.recv_timeout(std::time::Duration::from_secs(30))
            .unwrap();
    }
}

#[cfg(test)]
mod stdio_transport_tests {
    use super::*;
    use rmcp::transport::Transport;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn stdio_transport_isolates_progress_frees_cancelled_requests_and_shrinks_line_buf() {
        let pool = lease::BytePool::new(2000);
        let leases = Arc::new(RequestLeases::default());
        let (mut client_write, server_read) = tokio::io::duplex(256 * 1024);
        let (server_write, mut client_read) = tokio::io::duplex(64 * 1024);
        let mut transport = StdioTransport::new(server_read, server_write, Arc::clone(&leases));

        // 1. Oversized (> 64 KiB) incoming request line shrinks `line_buf` capacity back to 0.
        let big_arg = "A".repeat(80 * 1024);
        let req1_line = format!(
            "{{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{{\"name\":\"write\",\"arguments\":{{\"path\":\"a\",\"content\":\"{big_arg}\"}}}}}}\n"
        );
        client_write.write_all(req1_line.as_bytes()).await.unwrap();
        let msg1 = transport.receive().await.expect("request 1");
        assert!(matches!(msg1, rmcp::model::JsonRpcMessage::Request(_)));
        assert_eq!(
            transport.line_buf.capacity(),
            0,
            "line_buf over 64 KiB must drop its backing capacity after parsing"
        );

        // Stage 700 bytes for request 1 and 800 bytes for request 2.
        let cancel1 = CancellationToken::new();
        let mut h1 = pool.try_acquire(700).unwrap();
        let out1 = h1.split_off(400);
        leases.insert(rmcp::model::RequestId::Number(1), &cancel1, h1, out1);

        let cancel2 = CancellationToken::new();
        let mut h2 = pool.try_acquire(800).unwrap();
        let out2 = h2.split_off(500);
        leases.insert(rmcp::model::RequestId::Number(2), &cancel2, h2, out2);
        assert_eq!(pool.used(), 1500);

        // 2. Unqueued progress notifications and other responses must NOT touch request 1 or 2's leases.
        let progress = rmcp::model::ServerJsonRpcMessage::notification(
            rmcp::model::ServerNotification::ProgressNotification(
                rmcp::model::ProgressNotification::new(
                    rmcp::model::ProgressNotificationParam::new(
                        rmcp::model::ProgressToken(rmcp::model::NumberOrString::Number(9)),
                        1.0,
                    )
                    .with_message("still running"),
                ),
            ),
        );
        transport.send(progress).await.unwrap();
        assert_eq!(
            pool.used(),
            1500,
            "progress notification must not consume staged tool response leases"
        );

        // A completed response remains charged and deliverable after a late cancellation.
        client_write.write_all(concat!(
            "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/cancelled\",\"params\":{\"requestId\":1}}\n",
            "{\"jsonrpc\":\"2.0\",\"id\":11,\"method\":\"ping\"}\n",
        ).as_bytes()).await.unwrap();
        let next = transport.receive().await.unwrap();
        assert!(matches!(next, rmcp::model::JsonRpcMessage::Request(_)));
        assert_eq!(pool.used(), 1500);
        transport
            .send(rmcp::model::ServerJsonRpcMessage::response(
                rmcp::model::ServerResult::CallToolResult(CallToolResult::success(Vec::new())),
                rmcp::model::RequestId::Number(1),
            ))
            .await
            .unwrap();
        assert_eq!(pool.used(), 800);

        // Cancellation before a complete response is registered still reaches RMCP.
        client_write.write_all(
            b"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/cancelled\",\"params\":{\"requestId\":10}}\n"
        ).await.unwrap();
        let cancelled = transport.receive().await.unwrap();
        assert!(matches!(
            cancelled,
            rmcp::model::JsonRpcMessage::Notification(_)
        ));
        let late = pool.try_acquire(100).unwrap();
        leases.insert(
            rmcp::model::RequestId::Number(10),
            &cancel1,
            lease::Lease::empty(&pool),
            late,
        );
        assert_eq!(pool.used(), 800);

        // 4. Sending response for request 2 releases both staging and outbound leases once flushed.
        let resp2 = rmcp::model::ServerJsonRpcMessage::response(
            rmcp::model::ServerResult::CallToolResult(CallToolResult::success(vec![
                ContentBlock::text("done"),
            ])),
            rmcp::model::RequestId::Number(2),
        );
        transport.send(resp2).await.unwrap();
        assert_eq!(pool.used(), 0);

        let mut wire_out = vec![0u8; 4096];
        let n = client_read.read(&mut wire_out).await.unwrap();
        let text = std::str::from_utf8(&wire_out[..n]).unwrap();
        assert!(text.contains("\"notifications/progress\""), "{text}");
        assert!(text.contains("\"id\":2"), "{text}");

        // Closing rejects new work but retains queued leases until session owners end.
        let leftover = pool.try_acquire(300).unwrap();
        leases.insert(
            rmcp::model::RequestId::Number(3),
            &CancellationToken::new(),
            lease::Lease::empty(&pool),
            leftover,
        );
        assert_eq!(pool.used(), 300);
        transport.close().await.unwrap();
        assert_eq!(pool.used(), 300);
        drop(transport);
        assert_eq!(pool.used(), 300);
        drop(leases);
        assert_eq!(pool.used(), 0);
    }
    #[tokio::test]
    async fn stdio_send_claims_lease_before_first_poll() {
        use rmcp::transport::Transport;
        let pool = lease::BytePool::new(1 << 20);
        let leases = Arc::new(RequestLeases::default());
        let mut transport =
            StdioTransport::new(tokio::io::empty(), tokio::io::sink(), Arc::clone(&leases));
        let id = rmcp::model::RequestId::Number(7);
        let held = pool.try_acquire(128 << 10).unwrap();
        leases.insert(
            id.clone(),
            &CancellationToken::new(),
            lease::Lease::empty(&pool),
            held,
        );
        let result = CallToolResult::success(vec![ContentBlock::text("ordinary response")]);
        let sending = transport.send(rmcp::model::ServerJsonRpcMessage::response(
            rmcp::model::ServerResult::CallToolResult(result),
            id.clone(),
        ));
        leases.on_cancel(&id);
        assert_eq!(
            pool.used(),
            128 << 10,
            "the unpolled send still owns the response"
        );
        drop(sending);
        assert_eq!(pool.used(), 0);
    }

    #[tokio::test]
    async fn completed_stdio_response_ignores_late_cancellation() {
        use rmcp::transport::Transport;
        use tokio::io::AsyncWriteExt;
        let pool = lease::BytePool::new(4096);
        let leases = Arc::new(RequestLeases::default());
        let (mut input, read) = tokio::io::duplex(4096);
        let mut transport = StdioTransport::new(read, tokio::io::sink(), Arc::clone(&leases));
        let id = rmcp::model::RequestId::Number(7);
        leases.insert(
            id.clone(),
            &CancellationToken::new(),
            lease::Lease::empty(&pool),
            pool.try_acquire(1000).unwrap(),
        );
        input.write_all(concat!(
            "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/cancelled\",\"params\":{\"requestId\":7}}\n",
            "{\"jsonrpc\":\"2.0\",\"id\":8,\"method\":\"ping\"}\n"
        ).as_bytes()).await.unwrap();
        let next = transport.receive().await.unwrap();
        assert!(
            matches!(next, rmcp::model::JsonRpcMessage::Request(_)),
            "a completed result must stay queued instead of forwarding a late cancellation"
        );
        assert_eq!(pool.used(), 1000);
        drop(leases.take(&id));
        assert_eq!(pool.used(), 0);
    }

    #[tokio::test]
    async fn stdio_handoff_keeps_prepaid_payload_without_second_admission() {
        for send in [false, true] {
            let result = CallToolResult::success(vec![ContentBlock::text("already admitted")]);
            let id = rmcp::model::RequestId::Number(7);
            let payload_bytes = service::result_payload_capacity(&result);
            let wire_bytes = ipc::json_len(&result).unwrap() + ipc::json_len(&id).unwrap() + 256;
            let pool = lease::BytePool::new(payload_bytes + wire_bytes);
            let launch = Arc::new(
                LaunchContext::new(std::env::current_dir().unwrap(), Vec::new())
                    .with_memory(Arc::clone(&pool))
                    .with_control_memory(lease::BytePool::new(0)),
            );
            let tasks = Tasks::new(Limits::OFF).unwrap();
            let render = Render {
                output_mode: service::OutputMode::Both,
                structured_content: true,
            };
            let leases = Arc::new(RequestLeases::default());
            let mut server = Server::new(
                Backend::Local {
                    service: Arc::new(Service::new(tasks, false)),
                    render,
                    session: Arc::new(OnceLock::new()),
                    launch: Arc::clone(&launch),
                    daemon: false,
                },
                &launch,
                None,
                Tools::default(),
                None,
                InstructionsToolMode::Off,
            );
            server.stdio_leases = Some(Arc::clone(&leases));
            let (_input, read) = tokio::io::duplex(64);
            let mut peer = rmcp::service::serve_directly(
                server.clone(),
                StdioTransport::new(read, tokio::io::sink(), Arc::new(RequestLeases::default())),
                None,
            );
            let context = RequestContext::new(id.clone(), peer.peer().clone());
            let tracked = files::TrackedResult {
                result,
                overload: false,
                task_meta: None,
                lease: pool.try_acquire(wire_bytes),
                payload_owner: lease::Owner::new(pool.try_acquire(payload_bytes).unwrap()),
            };
            assert_eq!(pool.used(), pool.limit());
            let result = server.deliver_local(&context, tracked, &launch, render, false);
            assert_eq!(result.is_error, Some(false), "{result:?}");
            assert_eq!(
                result.content[0].as_text().unwrap().text,
                "already admitted"
            );
            assert_eq!(
                pool.used(),
                pool.limit(),
                "handoff transfers existing payment"
            );

            let mut transport =
                StdioTransport::new(tokio::io::empty(), tokio::io::sink(), Arc::clone(&leases));
            let sending = transport.send(rmcp::model::ServerJsonRpcMessage::response(
                rmcp::model::ServerResult::CallToolResult(result),
                id,
            ));
            assert_eq!(
                pool.used(),
                pool.limit(),
                "an unpolled send owns the payload"
            );
            if send {
                sending.await.unwrap();
            } else {
                drop(sending);
            }
            assert_eq!(pool.used(), 0);
            peer.close().await.unwrap();
        }
    }

    #[test]
    fn stdio_encoding_keeps_exact_capacity_charged() {
        let item = serde_json::json!({"text": "x".repeat(65_535)});
        let expected = ipc::json_len(&item).unwrap() + 1;
        let pool = lease::BytePool::new(expected);
        let mut lease = lease::Lease::empty(&pool);
        let bytes = encode_stdio_frame(&item, Some(&mut lease)).unwrap();
        assert_eq!(bytes.len(), expected);
        assert_eq!(bytes.capacity(), lease.bytes());
        assert_eq!(bytes.last(), Some(&b'\n'));
        drop(bytes);
        drop(lease);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn daemon_emit_flags_and_payload_parsing() {
        let flags = daemon_flags(
            "emit",
            &[
                "--namespace".into(),
                "custom-ns".into(),
                "--source".into(),
                "github".into(),
                "--topic".into(),
                "pr".into(),
                "--stream".into(),
            ],
        )
        .unwrap();
        assert_eq!(
            flags.namespace.as_deref(),
            Some(std::ffi::OsStr::new("custom-ns"))
        );
        assert_eq!(flags.source.as_deref(), Some("github"));
        assert_eq!(flags.topic.as_deref(), Some("pr"));
        assert!(flags.stream);
        assert!(daemon_flags("emit", &["--http".into(), "127.0.0.1:0".into()]).is_err());

        // Accepts mcpcli eventId/truncated aliases and fills source/topic from CLI flags.
        let parsed = parse_emit_payload(
            r#"{"eventId":"evt-42","name":"pr.merged","data":{"n":1},"truncated":true}"#,
            Some("github"),
            Some("pr"),
        )
        .unwrap();
        assert_eq!(parsed.source, "github");
        assert_eq!(parsed.topic.as_deref(), Some("pr"));
        assert_eq!(parsed.name, "pr.merged");
        assert_eq!(parsed.upstream_event_id, "evt-42");
        assert_eq!(parsed.upstream_truncated, Some(true));

        // Rejects oversized payloads (> 64 KiB).
        let huge = format!(
            r#"{{"source":"s","name":"n","upstreamEventId":"1","data":"{}"}}"#,
            "x".repeat(events::MAX_EXTERNAL_EVENT_BYTES)
        );
        assert!(parse_emit_payload(&huge, None, None).is_err());
    }

    #[test]
    fn mcp_name_header_decoding_handles_plain_and_base64() {
        use base64::Engine as _;
        assert_eq!(
            decode_mcp_header_value("external.event").as_deref(),
            Some("external.event")
        );
        let encoded = format!(
            "=?base64?{}?=",
            base64::engine::general_purpose::STANDARD.encode("external.event")
        );
        assert_eq!(
            decode_mcp_header_value(&encoded).as_deref(),
            Some("external.event")
        );
        assert!(decode_mcp_header_value("=?base64?!!!invalid!!!?=").is_none());
    }

    #[tokio::test]
    async fn read_bounded_line_skips_oversized_line_and_recovers_next() {
        let mut input = Vec::new();
        input.extend_from_slice(b"ok1\r\n");
        input.extend_from_slice(&[b'x'; 128]);
        input.push(b'\n');
        input.extend_from_slice(b"ok2\n");
        input.extend_from_slice(&[b'y'; 64]);
        let mut reader = tokio::io::BufReader::with_capacity(16, &input[..]);
        let mut buf = Vec::new();

        assert_eq!(
            read_bounded_line(&mut reader, &mut buf, 16).await.unwrap(),
            BoundedLine::Line(b"ok1\r\n".to_vec())
        );
        assert_eq!(
            read_bounded_line(&mut reader, &mut buf, 16).await.unwrap(),
            BoundedLine::TooLong
        );
        assert_eq!(
            read_bounded_line(&mut reader, &mut buf, 16).await.unwrap(),
            BoundedLine::Line(b"ok2\n".to_vec())
        );
        assert_eq!(
            read_bounded_line(&mut reader, &mut buf, 16).await.unwrap(),
            BoundedLine::TooLong
        );
        assert_eq!(
            read_bounded_line(&mut reader, &mut buf, 16).await.unwrap(),
            BoundedLine::Eof
        );
    }
}
