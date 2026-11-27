//! MCP server handler, tool routing, schema rewriting, and progress/event forwarding.

pub(crate) mod ingress;

use crate::codemode;
use crate::config::{InstructionsToolMode, Tools, compose_instructions};
use crate::endpoint;
use crate::env::LaunchContext;
use crate::events;
use crate::files;
use crate::id;
use crate::ipc;
use crate::lease;
use crate::render;
use crate::render::files::{self as render_files, publish_transfer, transfer_error};
use crate::render::types::{AgentDelivery, TrackedResult};
use crate::service::core::overload_task_message;
use crate::service::schema::{Action, Request};
use crate::service::{ExecContext, Service};
use crate::skills::{SkillCatalog, SkillContent, SkillEntry, SkillReadError};
use crate::tasks::session::Session;
use crate::transfer::{self, TransferEndpoint};
use crate::wait;
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{
    CacheScope, CallToolResult, ContentBlock, GetSkillRequestMethod, GetSkillRequestParams,
    GetSkillResult, Implementation, InitializeRequestParams, InitializeResult,
    ListResourceTemplatesRequestMethod, ListResourceTemplatesResult, ListResourcesRequestMethod,
    ListResourcesResult, ListSkillsRequestMethod, ListSkillsResult, PaginatedRequestParams,
    ProtocolVersion, ReadResourceDirectoryRequestMethod, ReadResourceDirectoryRequestParams,
    ReadResourceDirectoryResult, ReadResourceRequestMethod, ReadResourceRequestParams,
    ReadResourceResponse, ReadResourceResult, Resource, ResourceContents, ResultType,
    SKILLS_EXTENSION_ID, ServerCapabilities, ServerConfig, Skill, SkillFrontmatter, SkillResource,
    SkillResources, ToolAnnotations,
};
use rmcp::service::RequestContext;
use rmcp::{RoleServer, ServerHandler, tool, tool_handler};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

// Keep the advertised schema while returning invalid tool arguments as tool errors.
#[derive(serde::Deserialize)]
#[serde(transparent)]
struct CodemodeArguments(serde_json::Value);

impl schemars::JsonSchema for CodemodeArguments {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        codemode::control::Request::schema_name()
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        codemode::control::Request::json_schema(generator)
    }
}

/// The token position authenticated before rmcp receives a request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Principal(pub usize);

#[derive(Default)]
struct HttpResponseLeaseState {
    closed: bool,
    lease: Option<lease::Lease>,
}

/// Shared outbound response lease slot bridged from `Server` to HTTP `LeaseBody` across
/// `tokio::spawn` boundaries via `http::request::Parts::extensions`.
#[derive(Clone, Default)]
pub(crate) struct HttpResponseLease(Arc<std::sync::Mutex<HttpResponseLeaseState>>);

impl HttpResponseLease {
    pub(crate) fn attach(&self, outbound: lease::Lease) -> bool {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.closed {
            drop(state);
            drop(outbound);
            return false;
        }
        let replaced = state.lease.replace(outbound);
        drop(state);
        drop(replaced);
        true
    }

    pub(crate) fn close(&self) {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.closed = true;
        let dropped = state.lease.take();
        drop(state);
        drop(dropped);
    }
}

const SKILLS_PAGE_SIZE: usize = 64;
const DIR_PAGE_SIZE: usize = 256;

#[derive(Clone)]
pub(crate) struct Server {
    pub(crate) backend: Backend,
    instructions: Arc<str>,
    tool_instructions: Option<Arc<str>>,
    /// Connection instances share the immutable routing and tool metadata for their host.
    router: Arc<ToolRouter<Server>>,
    transfer: Option<TransferEndpoint>,
    tools: Tools,
    /// Sessionless HTTP calls select the session for their authenticated token.
    pub(crate) principals: Option<Arc<[Arc<Session>]>>,
    /// Outbound active-byte leases indexed by JSON-RPC request ID for stdio responses.
    pub(crate) stdio_leases: Option<Arc<RequestLeases>>,
    /// Optional read-only skills catalog exposed on MCP `2026-07-28+`.
    skills: Option<Arc<SkillCatalog>>,
    /// True once this session negotiated via legacy `initialize` or a `< 2026-07-28` HTTP request.
    legacy_session: Arc<AtomicBool>,
}

pub(crate) struct RegisteredSession {
    session: Arc<Session>,
    event_bus: Arc<events::EventBus>,
}

impl Drop for RegisteredSession {
    fn drop(&mut self) {
        self.event_bus.remove_session(&self.session.id);
    }
}

/// Where this server's calls run: in this process, or on the namespace's daemon.
#[derive(Clone)]
pub(crate) enum Backend {
    Local {
        service: Arc<Service>,
        /// Created by initialize, or the first tool call of a stdio client that skips it.
        session: Arc<OnceLock<Result<RegisteredSession, String>>>,
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

impl Server {
    /// A server over `backend`, whose `exec` tool says where its tasks run.
    pub(crate) fn new(
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
            skills: None,
            legacy_session: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) fn with_stdio_leases(mut self, stdio_leases: Arc<RequestLeases>) -> Self {
        self.stdio_leases = Some(stdio_leases);
        self
    }

    pub(crate) fn with_skills(mut self, skills: Option<Arc<SkillCatalog>>) -> Self {
        self.skills = skills;
        self
    }

    pub(crate) fn with_legacy_session(self, legacy: bool) -> Self {
        self.legacy_session.store(legacy, Ordering::Release);
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

    fn pools(&self) -> (&Arc<lease::BytePool>, &Arc<lease::BytePool>) {
        match &self.backend {
            Backend::Local { launch, .. } => (launch.memory(), launch.control_memory()),
            Backend::Daemon(endpoint) => (endpoint.memory(), endpoint.control_memory()),
        }
    }

    /// The principal session for sessionless HTTP, or this server's client-named session.
    fn session(&self, context: &RequestContext<RoleServer>) -> Result<Arc<Session>, String> {
        let Backend::Local {
            service,
            session,
            launch,
            ..
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
                .and_then(|parts| parts.extensions.get::<Principal>())
                .ok_or_else(|| "Internal error: HTTP request has no principal.".to_owned())?;
            return principals
                .get(principal.0)
                .cloned()
                .ok_or_else(|| "Internal error: HTTP principal is out of range.".to_owned());
        }
        let isolate_launch = self.principals.is_some();
        session
            .get_or_init(|| client_session(context, service, launch, isolate_launch))
            .as_ref()
            .map(|registered| Arc::clone(&registered.session))
            .map_err(Clone::clone)
    }
}

/// A session named for the client that initializes or first uses this server.
fn client_session(
    context: &RequestContext<RoleServer>,
    service: &Service,
    launch: &Arc<LaunchContext>,
    isolate_launch: bool,
) -> Result<RegisteredSession, String> {
    let short = id::random_chars(6).map_err(|error| format!("Cannot name the session: {error}"))?;
    let launch = if isolate_launch {
        Arc::new(launch.for_session())
    } else {
        Arc::clone(launch)
    };
    let session = Session::new(
        short.clone(),
        format!("{}-{short}", client_name(context)),
        launch,
    );
    Ok(RegisteredSession {
        session,
        event_bus: Arc::clone(service.event_bus()),
    })
}

/// The client's name from request metadata or the legacy handshake.
fn client_name(context: &RequestContext<RoleServer>) -> String {
    context
        .client_info()
        .map_or_else(|| "client".to_string(), |info| info.name)
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
pub(crate) fn host_line(launch: &LaunchContext) -> String {
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
    let home = files::path::resolve_home(launch).map_or_else(
        || "unknown".to_owned(),
        |home| files::path::display_path(&home),
    );
    format!(
        "{} {} · {hostname} · user {user} · home {home} · default cwd {}",
        std::env::consts::OS,
        std::env::consts::ARCH,
        files::path::display_path(&launch.cwd)
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
        self.legacy_session.store(true, Ordering::Release);
        let _response_workspace =
            reserve_response_content(self.pools().0, self.instructions.len().saturating_add(512))?;
        let mut result = self
            .negotiate_initialize(&request)?
            .with_instructions(self.instructions.as_ref());
        if let Some(extensions) = result.capabilities.extensions.as_mut() {
            extensions.remove(SKILLS_EXTENSION_ID);
            if extensions.is_empty() {
                result.capabilities.extensions = None;
            }
        }
        result.capabilities.resources = None;
        let mut peer = request;
        peer.protocol_version = result.protocol_version.clone();
        context.peer.set_peer_info(peer);
        if let Backend::Local {
            service,
            session,
            launch,
            ..
        } = &self.backend
        {
            let isolate_launch = self.principals.is_some();
            session
                .get_or_init(|| client_session(&context, service, launch, isolate_launch))
                .as_ref()
                .map_err(|message| rmcp::ErrorData::internal_error(message.clone(), None))?;
        }
        Ok(result)
    }

    async fn discover(
        &self,
        _context: RequestContext<RoleServer>,
    ) -> Result<rmcp::model::DiscoverResult, rmcp::ErrorData> {
        let (pool, control) = self.pools();
        let _response_workspace = reserve_control_response_content(
            pool,
            control,
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
        let (pool, control) = self.pools();
        let _response_workspace = reserve_control_response_content(pool, control, bytes)?;
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

    async fn list_skills(
        &self,
        request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListSkillsResult, rmcp::ErrorData> {
        let catalog = self.require_skills::<ListSkillsRequestMethod>(&context)?;
        let entries = catalog.skills();
        let cursor = request.and_then(|p| p.cursor);
        let start = parse_pagination_cursor(cursor.as_deref(), entries.len())?;
        let end = start.saturating_add(SKILLS_PAGE_SIZE).min(entries.len());
        let skills: Vec<Skill> = entries[start..end].iter().map(project_skill).collect();
        let next_cursor = (end < entries.len()).then(|| end.to_string());
        let staging = self.pools().0.try_acquire(0).expect("zero-byte lease");
        let result = ListSkillsResult {
            result_type: Some(ResultType::COMPLETE),
            ttl_ms: Some(0),
            cache_scope: Some(CacheScope::Public),
            skills,
            next_cursor,
            meta: None,
        };
        self.deliver_skill_response(&context, result, staging)
    }

    async fn get_skill(
        &self,
        request: GetSkillRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<GetSkillResult, rmcp::ErrorData> {
        let catalog = self.require_skills::<GetSkillRequestMethod>(&context)?;
        let entry = catalog.get_skill(&request.uri).ok_or_else(|| {
            rmcp::ErrorData::invalid_params(format!("skill not found: {}", request.uri), None)
        })?;
        let staging = self.pools().0.try_acquire(0).expect("zero-byte lease");
        let result = GetSkillResult::new(project_skill(entry))
            .with_ttl_ms(0)
            .with_cache_scope(CacheScope::Public);
        self.deliver_skill_response(&context, result, staging)
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResponse, rmcp::ErrorData> {
        use base64::Engine as _;

        let catalog = self.require_skills::<ReadResourceRequestMethod>(&context)?;
        let read = catalog
            .read_resource(&request.uri, self.pools().0, &context.ct)
            .await
            .map_err(skill_read_error_to_rmcp)?;
        let mut staging = read.lease;
        let contents = match read.content {
            SkillContent::Text(text) => ResourceContents::TextResourceContents {
                uri: read.uri,
                mime_type: Some(read.mime_type.to_owned()),
                text,
                meta: None,
            },
            SkillContent::Blob(bytes) => {
                let encoded_len = bytes.len().div_ceil(3).saturating_mul(4);
                let Some(peak) = staging.bytes().checked_add(encoded_len) else {
                    drop(staging);
                    drop(bytes);
                    return Err(rmcp::ErrorData::internal_error(
                        "Server active memory budget is full while preparing the response",
                        None,
                    ));
                };
                if !staging.try_resize(peak) {
                    drop(staging);
                    drop(bytes);
                    return Err(rmcp::ErrorData::internal_error(
                        "Server active memory budget is full while preparing the response",
                        None,
                    ));
                }
                let blob = base64::engine::general_purpose::STANDARD.encode(&bytes);
                drop(bytes);
                let _ = staging.try_resize(blob.len());
                ResourceContents::BlobResourceContents {
                    uri: read.uri,
                    mime_type: Some(read.mime_type.to_owned()),
                    blob,
                    meta: None,
                }
            }
        };
        let result = ReadResourceResult::new(vec![contents])
            .with_ttl_ms(0)
            .with_cache_scope(CacheScope::Public);
        self.deliver_skill_response(&context, result, staging)
            .map(ReadResourceResponse::Complete)
    }

    async fn read_resource_directory(
        &self,
        request: ReadResourceDirectoryRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceDirectoryResult, rmcp::ErrorData> {
        let catalog = self.require_skills::<ReadResourceDirectoryRequestMethod>(&context)?;
        let children = catalog.read_directory(&request.uri).ok_or_else(|| {
            rmcp::ErrorData::invalid_params(
                format!("skill directory not found: {}", request.uri),
                None,
            )
        })?;
        let start = parse_pagination_cursor(request.cursor.as_deref(), children.len())?;
        let end = start.saturating_add(DIR_PAGE_SIZE).min(children.len());
        let resources: Vec<Resource> = children[start..end]
            .iter()
            .map(|child| {
                let mut res = Resource::new(child.uri.clone(), child.name.clone())
                    .with_mime_type(child.mime_type);
                if let Some(size) = child.size {
                    res = res.with_size(size);
                }
                res
            })
            .collect();
        let next_cursor = (end < children.len()).then(|| end.to_string());
        let staging = self.pools().0.try_acquire(0).expect("zero-byte lease");
        let result = ListResourcesResult {
            result_type: Some(ResultType::COMPLETE),
            ttl_ms: Some(0),
            cache_scope: Some(CacheScope::Public),
            resources,
            next_cursor,
            meta: None,
        };
        self.deliver_skill_response(&context, result, staging)
    }

    async fn list_resources(
        &self,
        request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, rmcp::ErrorData> {
        let _ = self.require_skills::<ListResourcesRequestMethod>(&context)?;
        let cursor = request.and_then(|p| p.cursor);
        let _ = parse_pagination_cursor(cursor.as_deref(), 0)?;
        let staging = self.pools().0.try_acquire(0).expect("zero-byte lease");
        let result = ListResourcesResult {
            result_type: Some(ResultType::COMPLETE),
            ttl_ms: Some(0),
            cache_scope: Some(CacheScope::Public),
            resources: Vec::new(),
            next_cursor: None,
            meta: None,
        };
        self.deliver_skill_response(&context, result, staging)
    }

    async fn list_resource_templates(
        &self,
        request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListResourceTemplatesResult, rmcp::ErrorData> {
        let _ = self.require_skills::<ListResourceTemplatesRequestMethod>(&context)?;
        let cursor = request.and_then(|p| p.cursor);
        let _ = parse_pagination_cursor(cursor.as_deref(), 0)?;
        let staging = self.pools().0.try_acquire(0).expect("zero-byte lease");
        let result = ListResourceTemplatesResult {
            result_type: Some(ResultType::COMPLETE),
            ttl_ms: Some(0),
            cache_scope: Some(CacheScope::Public),
            resource_templates: Vec::new(),
            next_cursor: None,
            meta: None,
        };
        self.deliver_skill_response(&context, result, staging)
    }

    fn get_info(&self) -> ServerConfig {
        let capabilities = if self.skills.is_some() && !self.legacy_session.load(Ordering::Acquire)
        {
            ServerCapabilities::builder()
                .enable_tools()
                .enable_resources()
                .enable_skills_with_directory_read()
                .build()
        } else {
            ServerCapabilities::builder().enable_tools().build()
        };
        // Internal capability checks need no per-request instructions copy.
        ServerConfig::new(capabilities)
            .with_server_info(Implementation::new("fastexec", env!("CARGO_PKG_VERSION")))
    }
}

/// Stdio service wrapper that intercepts skills/resources requests for legacy or disabled sessions
/// before `rmcp`'s default `ServerHandler` dispatch runs URI validation.
pub(crate) struct McpService(pub(crate) Server);

impl rmcp::service::Service<RoleServer> for McpService {
    async fn handle_request(
        &self,
        request: <RoleServer as rmcp::service::ServiceRole>::PeerReq,
        context: RequestContext<RoleServer>,
    ) -> Result<<RoleServer as rmcp::service::ServiceRole>::Resp, rmcp::ErrorData> {
        let reject_as_legacy_or_disabled = self.0.skills.is_none()
            || self.0.legacy_session.load(Ordering::Acquire)
            || context
                .protocol_version()
                .is_some_and(|v| v < ProtocolVersion::V_2026_07_28);
        if reject_as_legacy_or_disabled {
            match &request {
                rmcp::model::ClientRequest::ListSkillsRequest(_) => {
                    return Err(rmcp::ErrorData::method_not_found::<ListSkillsRequestMethod>());
                }
                rmcp::model::ClientRequest::GetSkillRequest(_) => {
                    return Err(rmcp::ErrorData::method_not_found::<GetSkillRequestMethod>());
                }
                rmcp::model::ClientRequest::ReadResourceRequest(_) => {
                    return Err(rmcp::ErrorData::method_not_found::<ReadResourceRequestMethod>());
                }
                rmcp::model::ClientRequest::ReadResourceDirectoryRequest(_) => {
                    return Err(rmcp::ErrorData::method_not_found::<
                        ReadResourceDirectoryRequestMethod,
                    >());
                }
                rmcp::model::ClientRequest::ListResourcesRequest(_) => {
                    return Err(rmcp::ErrorData::method_not_found::<
                        ListResourcesRequestMethod,
                    >());
                }
                rmcp::model::ClientRequest::ListResourceTemplatesRequest(_) => {
                    return Err(rmcp::ErrorData::method_not_found::<
                        ListResourceTemplatesRequestMethod,
                    >());
                }
                _ => {}
            }
        }
        rmcp::service::Service::handle_request(&self.0, request, context).await
    }

    async fn handle_notification(
        &self,
        notification: <RoleServer as rmcp::service::ServiceRole>::PeerNot,
        context: rmcp::service::NotificationContext<RoleServer>,
    ) -> Result<(), rmcp::ErrorData> {
        rmcp::service::Service::handle_notification(&self.0, notification, context).await
    }

    fn get_info(&self) -> <RoleServer as rmcp::service::ServiceRole>::Info {
        rmcp::service::Service::get_info(&self.0)
    }

    fn supported_protocol_versions(&self) -> std::borrow::Cow<'static, [ProtocolVersion]> {
        rmcp::service::Service::supported_protocol_versions(&self.0)
    }
}

pub(crate) fn validate_skill_catalog(catalog: &SkillCatalog) {
    let projected: Vec<Skill> = catalog.skills().iter().map(project_skill).collect();
    if let Err(err) = rmcp::model::validate_skills(&projected) {
        panic!("invalid skill catalog projection: {err}");
    }
}

fn project_skill(entry: &SkillEntry) -> Skill {
    let mut frontmatter = SkillFrontmatter::new(entry.name.clone(), entry.description.clone());
    frontmatter.extra = entry.frontmatter_extra.clone();
    let resources = SkillResources::Files(
        entry
            .resources
            .iter()
            .map(|file| SkillResource::new(file.uri.clone(), file.digest.clone(), file.size))
            .collect(),
    );
    Skill::new(entry.uri.clone(), frontmatter, resources)
}

fn parse_pagination_cursor(
    cursor: Option<&str>,
    total_len: usize,
) -> Result<usize, rmcp::ErrorData> {
    let Some(raw) = cursor else {
        return Ok(0);
    };
    if raw.is_empty() || raw.starts_with('0') || !raw.bytes().all(|b| b.is_ascii_digit()) {
        return Err(rmcp::ErrorData::invalid_params(
            "invalid pagination cursor",
            None,
        ));
    }
    let offset = raw
        .parse::<usize>()
        .map_err(|_| rmcp::ErrorData::invalid_params("invalid pagination cursor", None))?;
    if offset == 0 || offset >= total_len {
        return Err(rmcp::ErrorData::invalid_params(
            "invalid pagination cursor",
            None,
        ));
    }
    Ok(offset)
}

fn skill_read_error_to_rmcp(err: SkillReadError) -> rmcp::ErrorData {
    match err {
        SkillReadError::NotFound(msg) => rmcp::ErrorData::invalid_params(msg, None),
        SkillReadError::MemoryFull(_) => rmcp::ErrorData::internal_error(
            "Server active memory budget is full while preparing the response",
            None,
        ),
        SkillReadError::Integrity(msg) | SkillReadError::Io(msg) => {
            rmcp::ErrorData::internal_error(msg, None)
        }
        SkillReadError::Cancelled => {
            rmcp::ErrorData::internal_error("Cancelled before the response was queued.", None)
        }
    }
}

impl Server {
    fn require_skills<M: rmcp::model::ConstString>(
        &self,
        context: &RequestContext<RoleServer>,
    ) -> Result<&Arc<SkillCatalog>, rmcp::ErrorData> {
        let Some(catalog) = &self.skills else {
            return Err(rmcp::ErrorData::method_not_found::<M>());
        };
        if self.legacy_session.load(Ordering::Acquire)
            || context
                .protocol_version()
                .is_some_and(|v| v < ProtocolVersion::V_2026_07_28)
        {
            return Err(rmcp::ErrorData::method_not_found::<M>());
        }
        Ok(catalog)
    }

    fn deliver_skill_response<T: serde::Serialize>(
        &self,
        context: &RequestContext<RoleServer>,
        result: T,
        mut staging: lease::Lease,
    ) -> Result<T, rmcp::ErrorData> {
        let outbound_bytes = lease::json_len(&result)
            .unwrap_or(usize::MAX)
            .saturating_add(lease::json_len(&context.id).unwrap_or(usize::MAX))
            .saturating_add(256);
        let Some(target) = staging.bytes().checked_add(outbound_bytes) else {
            drop(staging);
            drop(result);
            return Err(rmcp::ErrorData::internal_error(
                "Server active memory budget is full while preparing the response",
                None,
            ));
        };
        if !staging.try_resize(target) {
            drop(staging);
            drop(result);
            return Err(rmcp::ErrorData::internal_error(
                "Server active memory budget is full while preparing the response",
                None,
            ));
        }
        let outbound = staging.split_off(outbound_bytes);

        if let Some(slot) = context
            .extensions
            .get::<::http::request::Parts>()
            .and_then(|parts| parts.extensions.get::<HttpResponseLease>())
        {
            drop(staging);
            if !slot.attach(outbound) {
                drop(result);
                return Err(rmcp::ErrorData::internal_error(
                    "Cancelled before the response was queued.",
                    None,
                ));
            }
            return Ok(result);
        }

        if let Some(stdio) = self.stdio_leases.as_deref() {
            if !stdio.register_response(
                context.id.clone(),
                &context.ct,
                staging,
                outbound,
                Default::default(),
            ) {
                drop(result);
                return Err(rmcp::ErrorData::internal_error(
                    "Cancelled before the response was queued.",
                    None,
                ));
            }
            return Ok(result);
        }

        drop(staging);
        drop(outbound);
        Ok(result)
    }
}

fn event_error_to_rmcp(err: events::EventError, control: &Arc<lease::BytePool>) -> rmcp::ErrorData {
    let message_bytes = match &err {
        events::EventError::InvalidParams(message)
        | events::EventError::NotFound { message, .. }
        | events::EventError::Forbidden(message)
        | events::EventError::ResourceExhausted { message, .. } => message.len(),
    };
    let Some(held) = control.try_acquire(message_bytes.saturating_add(256)) else {
        return rmcp::ErrorData::new(
            rmcp::model::ErrorCode(-32025),
            "Server active memory budget is full while preparing the event error",
            None,
        );
    };
    lease::Owned::new(err, lease::Owner::new(held))
        .map(ipc::IpcEventError::from)
        .run(ipc_event_error_to_rmcp)
}

fn ipc_event_error_to_rmcp(err: ipc::IpcEventError) -> rmcp::ErrorData {
    rmcp::ErrorData::new(rmcp::model::ErrorCode(err.code), err.message, err.data)
}

pub(crate) fn decode_mcp_header_value(raw: &str) -> Option<String> {
    use base64::Engine as _;
    if let Some(rest) = raw.strip_prefix("=?base64?") {
        let encoded = rest.strip_suffix("?=")?;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .ok()?;
        return String::from_utf8(bytes).ok();
    }
    if raw.is_empty()
        || raw.starts_with(' ')
        || raw.ends_with(' ')
        || !raw.bytes().all(|b| (0x20..=0x7E).contains(&b))
    {
        return None;
    }
    Some(raw.to_owned())
}

fn reject_unexpected_mcp_name_header(
    context: &RequestContext<RoleServer>,
) -> Result<(), rmcp::ErrorData> {
    let Some(parts) = context.extensions.get::<::http::request::Parts>() else {
        return Ok(());
    };
    if parts.headers.get_all("mcp-name").iter().next().is_some() {
        return Err(rmcp::ErrorData::header_mismatch(
            "events/list does not accept Mcp-Name header",
            None,
        ));
    }
    Ok(())
}

/// Validates the HTTP `Mcp-Name` header against `params.name` (supporting `=?base64?...?=`
/// decoding). Missing header is allowed; duplicate, malformed, or mismatched headers return
/// `ErrorData::header_mismatch` (HTTP 400 / -32020).
fn validate_mcp_name_header(
    context: &RequestContext<RoleServer>,
    expected_name: &str,
) -> Result<(), rmcp::ErrorData> {
    let Some(parts) = context.extensions.get::<::http::request::Parts>() else {
        return Ok(());
    };
    let mut values = parts.headers.get_all("mcp-name").iter();
    let Some(raw_header) = values.next() else {
        return Ok(());
    };
    if values.next().is_some() {
        return Err(rmcp::ErrorData::header_mismatch(
            "duplicate Mcp-Name header is not allowed",
            None,
        ));
    }
    let raw_str = raw_header.to_str().map_err(|_| {
        rmcp::ErrorData::header_mismatch("Mcp-Name header is not valid ASCII", None)
    })?;
    let decoded = decode_mcp_header_value(raw_str)
        .ok_or_else(|| rmcp::ErrorData::header_mismatch("Mcp-Name header is malformed", None))?;
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

pub(crate) fn event_error_payload(error: ipc::IpcEventError) -> serde_json::Value {
    events::value_object([("error", error.into())])
}

async fn send_event_notification(
    context: &RequestContext<RoleServer>,
    method: &str,
    params: lease::Owned<serde_json::Value>,
    subscription_id: &rmcp::model::RequestId,
    metadata: &lease::Owner,
) -> bool {
    use rmcp::model::GetExtensions;

    let lease::Owned { value, owner } = params;
    let mut notification = build_event_notification(method, value, subscription_id);
    // The peer may keep this notification queued after the request is cancelled.
    notification
        .extensions_mut()
        .insert(lease::Owner::new((owner, metadata.clone())));
    tokio::select! {
        biased;
        _ = context.ct.cancelled() => false,
        sent = context.peer.send_notification(notification) => sent.is_ok(),
    }
}

fn stream_complete_result(subscription_id: &rmcp::model::RequestId) -> rmcp::model::CustomResult {
    let id_value = serde_json::to_value(subscription_id).unwrap_or(serde_json::Value::Null);
    rmcp::model::CustomResult::new(events::value_object([
        (
            "resultType",
            serde_json::Value::String("complete".to_owned()),
        ),
        (
            "_meta",
            events::value_object([("io.modelcontextprotocol/subscriptionId", id_value)]),
        ),
    ]))
}

impl Server {
    async fn handle_events_list(
        &self,
        params: Option<serde_json::Value>,
        context: RequestContext<RoleServer>,
    ) -> Result<rmcp::model::CustomResult, rmcp::ErrorData> {
        reject_unexpected_mcp_name_header(&context)?;
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
            Backend::Local { service, .. } => service
                .event_bus()
                .list_event_types()
                .map_err(|err| event_error_to_rmcp(err, self.pools().1))?,
            Backend::Daemon(endpoint) => endpoint
                .events_list(&client_name(&context), context.ct.clone())
                .await
                .map_err(|error| error.run(ipc_event_error_to_rmcp))?,
        };
        Ok(result
            .map(serde_json::Value::from)
            .run(rmcp::model::CustomResult::new))
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
                    .map_err(|err| event_error_to_rmcp(err, self.pools().1))?
            }
            Backend::Daemon(endpoint) => endpoint
                .events_poll(&client_name(&context), poll_req, context.ct.clone())
                .await
                .map_err(|error| error.run(ipc_event_error_to_rmcp))?,
        };
        Ok(response
            .map(serde_json::Value::from)
            .run(rmcp::model::CustomResult::new))
    }

    pub(crate) async fn handle_events_stream(
        &self,
        params: Option<serde_json::Value>,
        context: RequestContext<RoleServer>,
    ) -> Result<rmcp::model::CustomResult, rmcp::ErrorData> {
        let Some(raw) = params else {
            return Err(rmcp::ErrorData::invalid_params(
                "events/stream requires params with name",
                None,
            ));
        };
        let sub_req: events::SubscribeRequest = serde_json::from_value(raw)
            .map_err(|e| rmcp::ErrorData::invalid_params(format!("invalid params: {e}"), None))?;
        validate_mcp_name_header(&context, &sub_req.name)?;
        let (pool, control) = self.pools();
        // Fixed metadata covers subscription ID copies, cursors, and terminal error envelopes.
        let metadata_bytes = lease::json_len(&(&sub_req, &context.id))
            .unwrap_or(usize::MAX)
            .saturating_mul(3)
            .saturating_add(2048);
        let metadata = lease::Owner::new(
            pool.try_acquire(metadata_bytes)
                .or_else(|| control.try_acquire(metadata_bytes))
                .ok_or_else(|| {
                    ipc::IpcEventError::memory_full(
                        pool,
                        control,
                        "Server active memory budget is full while preparing event subscription",
                    )
                    .run(ipc_event_error_to_rmcp)
                })?,
        );
        let sub_id = context.id.clone();
        let terminated = |error| lease::Owned::new(event_error_payload(error), metadata.clone());
        match &self.backend {
            Backend::Local { service, .. } => {
                let _guard = service
                    .event_bus()
                    .try_acquire_stream()
                    .map_err(|err| event_error_to_rmcp(err, control))?;
                let caller_session = self.session(&context).ok().map(|s| s.id.clone());
                let mut sub = service
                    .event_bus()
                    .subscribe(sub_req, caller_session.as_deref())
                    .map_err(|err| event_error_to_rmcp(err, control))?;
                let initial = sub
                    .initial_active()
                    .map_err(|err| event_error_to_rmcp(err, control))?
                    .map(serde_json::Value::from);
                if !send_event_notification(
                    &context,
                    "notifications/events/active",
                    initial,
                    &sub_id,
                    &metadata,
                )
                .await
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
                                Ok(Some(lease::Owned { value, owner })) => {
                                    let (method, payload) = match value {
                                        events::StreamItem::Active(active) => ("notifications/events/active", active.into()),
                                        events::StreamItem::Event(event) => ("notifications/events/event", event.into()),
                                    };
                                    if !send_event_notification(&context, method, lease::Owned::new(payload, owner), &sub_id, &metadata).await {
                                        return Ok(stream_complete_result(&sub_id));
                                    }
                                    sleep.as_mut().reset(tokio::time::Instant::now() + heartbeat);
                                }
                                end => {
                                    let error = match end {
                                        Err(error) => error.into(),
                                        Ok(None) => ipc::IpcEventError {
                                            code: -32000,
                                            message: "event bus closed".to_owned(),
                                            data: None,
                                        },
                                        Ok(Some(_)) => unreachable!(),
                                    };
                                    send_event_notification(&context, "notifications/events/terminated", terminated(error), &sub_id, &metadata).await;
                                    return Ok(stream_complete_result(&sub_id));
                                }
                            }
                        }
                        _ = &mut sleep => {
                            let cursor = match sub.current_cursor() {
                                Ok(cursor) => cursor,
                                Err(error) => {
                                    send_event_notification(&context, "notifications/events/terminated", terminated(error.into()), &sub_id, &metadata).await;
                                    return Ok(stream_complete_result(&sub_id));
                                }
                            };
                            let payload = cursor.map(|cursor| events::value_object([("cursor", serde_json::Value::String(cursor))]));
                            if !send_event_notification(&context, "notifications/events/heartbeat", payload, &sub_id, &metadata).await {
                                return Ok(stream_complete_result(&sub_id));
                            }
                            sleep.as_mut().reset(tokio::time::Instant::now() + heartbeat);
                        }
                    }
                }
            }
            Backend::Daemon(endpoint) => {
                let client = client_name(&context);
                let (initial, mut stream) = endpoint
                    .events_stream(&client, sub_req, context.ct.clone())
                    .await
                    .map_err(|error| error.run(ipc_event_error_to_rmcp))?;

                if !send_event_notification(
                    &context,
                    "notifications/events/active",
                    initial.map(serde_json::Value::from),
                    &sub_id,
                    &metadata,
                )
                .await
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
                            let Some((frame, held)) = frame else {
                                if !terminated_sent {
                                    let error = if stream.overflowed() {
                                        ipc::IpcEventError {
                                            code: -32025,
                                            message:
                                                "endpoint event stream queue full due to slow consumer"
                                                    .to_owned(),
                                            data: None,
                                        }
                                    } else if stream.link_closed() {
                                        ipc::IpcEventError {
                                            code: -32000,
                                            message: "daemon connection closed".to_owned(),
                                            data: None,
                                        }
                                    } else {
                                        ipc::IpcEventError {
                                            code: -32025,
                                            message: "endpoint active memory budget full during event stream".to_owned(),
                                            data: None,
                                        }
                                    };
                                    send_event_notification(
                                        &context,
                                        "notifications/events/terminated",
                                        terminated(error),
                                        &sub_id,
                                        &metadata,
                                    )
                                    .await;
                                }
                                stream.cancel().await;
                                return Ok(stream_complete_result(&sub_id));
                            };
                            let owned = lease::Owned::new(frame, lease::Owner::new(held));
                            let (method, payload) = match owned.value {
                                ipc::EventWireFrame::Active(active) => {
                                    ("notifications/events/active", active.into())
                                }
                                ipc::EventWireFrame::Event(event) => {
                                    ("notifications/events/event", event.into())
                                }
                                ipc::EventWireFrame::Heartbeat { cursor } => {
                                    let payload = events::value_object([(
                                        "cursor",
                                        serde_json::Value::String(cursor),
                                    )]);
                                    ("notifications/events/heartbeat", payload)
                                }
                                ipc::EventWireFrame::Error(error) => {
                                    ("notifications/events/error", event_error_payload(error))
                                }
                                ipc::EventWireFrame::Terminated(error) => {
                                    terminated_sent = true;
                                    (
                                        "notifications/events/terminated",
                                        event_error_payload(error),
                                    )
                                }
                                ipc::EventWireFrame::Complete => {
                                    return Ok(stream_complete_result(&sub_id));
                                }
                                ipc::EventWireFrame::ListResult(_)
                                | ipc::EventWireFrame::PollResult(_) => continue,
                            };
                            if !send_event_notification(
                                &context,
                                method,
                                lease::Owned::new(payload, owned.owner),
                                &sub_id,
                                &metadata,
                            )
                            .await
                            {
                                stream.cancel().await;
                                return Ok(stream_complete_result(&sub_id));
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
        let (pool, control) = self.pools();
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
        Parameters(request): Parameters<Request>,
        context: RequestContext<RoleServer>,
    ) -> CallToolResult {
        let is_control = matches!(request.action, Action::List | Action::Kill);
        let mut context = context;
        if let Err(error) = self.admit_tool_input(&mut context, &request, is_control) {
            return error;
        }
        match &self.backend {
            Backend::Daemon(endpoint) => {
                let (progress, _forwarder) = self.progress(&context);
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
                session: _,
                launch,
                daemon: _,
            } => match self.session(&context) {
                Ok(session) => {
                    let (progress, _forwarder) = self.progress(&context);
                    let ctx = ExecContext {
                        session: &session,
                        launch: None,
                        cancel: context.ct.clone(),
                        progress,
                        call_id: None,
                        parent_task_id: None,
                        queued_guard: None,
                        on_started: None,
                        input_owner: request_input_owner(&context),
                    };
                    let delivery = AgentDelivery {
                        daemon_ipc: None,
                        http_response: http_context(&context),
                        rpc_id_bytes: lease::json_len(&context.id).unwrap_or(usize::MAX),
                    };
                    let tracked = render::exec_agent(service, request, ctx, delivery).await;
                    self.deliver_local(&context, tracked, launch, is_control)
                }
                Err(message) => {
                    let err = CallToolResult::error(vec![ContentBlock::text(message)]);
                    self.deliver_local(&context, TrackedResult::plain(err), launch, true)
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
        let request: codemode::control::Request = match serde_json::from_value(arguments.0) {
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
                service, launch, ..
            } => {
                let session = match self.session(&context) {
                    Ok(session) => session,
                    Err(message) => {
                        return CallToolResult::error(vec![ContentBlock::text(message)]);
                    }
                };
                let (progress, _forwarder) = self.progress(&context);
                let mut ctx = ExecContext {
                    session: &session,
                    launch: None,
                    cancel: context.ct.clone(),
                    progress,
                    call_id: None,
                    parent_task_id: None,
                    queued_guard: None,
                    on_started: None,
                    input_owner: request_input_owner(&context),
                };
                let delivery = AgentDelivery {
                    daemon_ipc: None,
                    http_response: http_context(&context),
                    rpc_id_bytes: lease::json_len(&context.id).unwrap_or(usize::MAX),
                };
                let outcome = crate::codemode::prepare_script(
                    service,
                    request,
                    &mut ctx,
                    None,
                    self.tools.transfer,
                )
                .await;
                let tracked = render::finish_exec_agent(service, outcome, &ctx, &delivery).await;
                self.deliver_local(&context, tracked, launch, false)
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
            Backend::Local { launch, .. } => {
                let tracked = render_files::read_tracked(
                    request,
                    Arc::clone(launch),
                    self.tools.transfer,
                    Some(context.ct.clone()),
                    request_input_owner(&context),
                )
                .await;
                self.deliver_local(&context, tracked, launch, false)
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
            Backend::Local { launch, .. } => {
                let res = render_files::write_tracked(
                    request,
                    Arc::clone(launch),
                    request_input_owner(&context),
                )
                .await;
                self.deliver_local(&context, res, launch, false)
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
            Backend::Local { launch, .. } => {
                let res = render_files::edit_tracked(
                    request,
                    Arc::clone(launch),
                    request_input_owner(&context),
                )
                .await;
                self.deliver_local(&context, res, launch, false)
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
            Backend::Local { launch, .. } => {
                let res = match &self.transfer {
                    None => TrackedResult::plain(transfer_error(
                        "transfer is disabled; set FASTEXEC_TRANSFER=true.".to_owned(),
                    )),
                    Some(TransferEndpoint::Fixed { table, base }) => {
                        publish_transfer(
                            request,
                            Arc::clone(launch),
                            table,
                            base,
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
                                .and_then(inferred_base),
                        };
                        match base {
                            Ok(base) => {
                                publish_transfer(
                                    request,
                                    Arc::clone(launch),
                                    table,
                                    &base,
                                    request_input_owner(&context),
                                )
                                .await
                            }
                            Err(message) => TrackedResult::plain(transfer_error(message)),
                        }
                    }
                };
                self.deliver_local(&context, res, launch, false)
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
                let _response_workspace = reserve_response_content(self.pools().0, text.len())?;
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
    pub(crate) fn deliver_local(
        &self,
        context: &RequestContext<RoleServer>,
        tracked: TrackedResult,
        launch: &LaunchContext,
        is_control: bool,
    ) -> CallToolResult {
        // Official rmcp owns HTTP response models and encodings after this
        // handoff. Its internal copies and queues are outside the app budget.
        if http_context(context) {
            return tracked.result;
        }
        let Some(stdio) = self.stdio_leases.as_deref() else {
            return tracked.result;
        };
        let limit = launch.memory().limit();
        deliver_tracked(
            launch.memory(),
            launch.control_memory(),
            stdio,
            context,
            tracked,
            is_control,
            |meta| match meta {
                Some(m) => overload_task_message(limit, m),
                None => format!(
                    "Server active memory budget ({limit} bytes) is full; retry once in-flight transfers complete.",
                ),
            },
        )
    }

    fn deliver_daemon(
        &self,
        context: &RequestContext<RoleServer>,
        tracked: TrackedResult,
        endpoint: &endpoint::Endpoint,
        is_control: bool,
    ) -> CallToolResult {
        let Some(stdio) = self.stdio_leases.as_deref() else {
            return tracked.result;
        };
        let pool = endpoint.memory();
        let limit = pool.limit();
        deliver_tracked(
            pool,
            endpoint.control_memory(),
            stdio,
            context,
            tracked,
            is_control,
            |_| {
                format!(
                    "Endpoint active memory budget ({limit} bytes) is full while writing stdio response; use action list or poll once in-flight transfers complete.",
                )
            },
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
        let (progress, _forwarder) = self.progress(context);
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
            root.remove("$defs")
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
            let name = reference.strip_prefix("#/$defs/").unwrap_or(&reference);
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

/// The base inferred from a request: its Host or HTTP/2 `:authority`, over the scheme the client saw.
fn inferred_base(parts: &::http::request::Parts) -> Result<String, String> {
    let mut hosts = parts.headers.get_all(::http::header::HOST).iter();
    let authority =
        match hosts.next() {
            Some(raw) => {
                if hosts.next().is_some() {
                    return Err(
                        "Cannot make a transfer URL: the request carries multiple Host headers."
                            .to_owned(),
                    );
                }
                let host = raw.to_str().map_err(|_| {
                    "Cannot make a transfer URL: Host header is not valid ASCII.".to_owned()
                })?;
                if host.contains('@') {
                    return Err(
                        "Cannot make a transfer URL: the request host carries userinfo.".to_owned(),
                    );
                }
                host.parse::<::http::uri::Authority>()
                    .map_err(|_| format!("Cannot make a transfer URL: {host} is not a host."))?
            }
            None => parts.uri.authority().cloned().ok_or_else(|| {
                "Cannot make a transfer URL: the request names no Host.".to_owned()
            })?,
        };
    if authority.as_str().contains('@') {
        return Err("Cannot make a transfer URL: the request host carries userinfo.".to_owned());
    }
    if authority.host().is_empty() {
        return Err("Cannot make a transfer URL: the request names no Host.".to_owned());
    }
    let scheme = forwarded_scheme(&parts.headers)
        .or_else(|| parts.uri.scheme_str().and_then(known_scheme))
        .unwrap_or("http");
    Ok(format!("{scheme}://{authority}/"))
}

/// The scheme the client saw: the first `X-Forwarded-Proto` token when it names `http` or `https`.
fn forwarded_scheme(headers: &::http::HeaderMap) -> Option<&'static str> {
    headers
        .get("x-forwarded-proto")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(',').next())
        .map(str::trim)
        .and_then(known_scheme)
}

fn known_scheme(token: &str) -> Option<&'static str> {
    if token.eq_ignore_ascii_case("https") {
        Some("https")
    } else if token.eq_ignore_ascii_case("http") {
        Some("http")
    } else {
        None
    }
}

fn deliver_tracked(
    pool: &Arc<lease::BytePool>,
    control_pool: &Arc<lease::BytePool>,
    stdio: &RequestLeases,
    context: &RequestContext<RoleServer>,
    mut tracked: TrackedResult,
    is_control: bool,
    overload_message: impl FnOnce(Option<&crate::service::core::TaskMeta>) -> String,
) -> CallToolResult {
    let use_control = is_control || tracked.result.is_error == Some(true);
    let b64_bytes =
        render::unleased_result_payload_capacity(&tracked.result, &tracked.payload_owner);
    let exact_json = lease::json_len(&tracked.result).unwrap_or(usize::MAX);
    let id_bytes = lease::json_len(&context.id).unwrap_or(usize::MAX);
    let outbound_bytes = exact_json.saturating_add(id_bytes).saturating_add(256);
    let admitted = b64_bytes
        .checked_add(outbound_bytes)
        .and_then(|peak| admit_delivery(&mut tracked.lease, pool, control_pool, peak, use_control));
    let Some(mut held) = admitted else {
        let meta = tracked.task_meta.take();
        drop(tracked);
        let err = CallToolResult::error(vec![ContentBlock::text(overload_message(meta.as_ref()))]);
        let err_outbound = lease::json_len(&err)
            .unwrap_or(usize::MAX)
            .saturating_add(id_bytes)
            .saturating_add(256);
        let err_peak = render::result_payload_capacity(&err).saturating_add(err_outbound);
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
        .saturating_add(lease::json_len(&tool.annotations).unwrap_or(usize::MAX))
        .saturating_add(lease::json_len(&tool.icons).unwrap_or(usize::MAX))
        .saturating_add(lease::json_len(&tool.meta).unwrap_or(usize::MAX))
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

fn reserve_control_response_content(
    pool: &Arc<lease::BytePool>,
    control: &Arc<lease::BytePool>,
    bytes: usize,
) -> Result<lease::Lease, rmcp::ErrorData> {
    pool.try_acquire(bytes)
        .or_else(|| {
            (bytes <= lease::CONTROL_LIMIT)
                .then(|| control.try_acquire(bytes))
                .flatten()
        })
        .ok_or_else(|| {
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
    let length = lease::json_len(parameters_and_id).map_err(|_| REFUSED)?;
    let bytes = length.checked_mul(4).ok_or(REFUSED)?;
    let held = pool
        .try_acquire(bytes)
        .or_else(|| {
            (is_control && length <= lease::SHORT_INPUT)
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
        .is_some_and(|parts| parts.extensions.get::<Principal>().is_some())
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
    pool: &Arc<lease::BytePool>,
    control: &Arc<lease::BytePool>,
) -> Option<rmcp::model::ServerNotification> {
    use rmcp::model::GetExtensions;
    // This owns the application's message allocation. SDK encodings are outside
    // the boundary; stdio acquires its own exact frame owner in Transport::send.
    let held = pool.try_acquire(message.len()).or_else(|| {
        (message.len() <= 64 * 1024)
            .then(|| control.try_acquire(message.len()))
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
    (pool, control): (Arc<lease::BytePool>, Arc<lease::BytePool>),
) {
    while latest.changed().await.is_ok() {
        let notification = {
            let latest = latest.borrow_and_update();
            let Some((seconds, message)) = latest.as_ref() else {
                continue;
            };
            progress_notification(&token, *seconds, message, &owner, &pool, &control)
        };
        let Some(notification) = notification else {
            continue;
        };
        if peer.send_notification(notification).await.is_err() {
            return;
        }
    }
}

impl Server {
    /// The sink the call replaces with its latest progress message, when the client asked for
    /// progress; the forwarder sends them.
    fn progress(
        &self,
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
        let (pool, control) = self.pools();
        let pools = (Arc::clone(pool), Arc::clone(control));
        let (sender, latest) = watch::channel(None);
        let future = forward_progress(context.peer.clone(), token, latest, owner.clone(), pools);
        let forwarder = Forwarder(tokio::spawn(owner.hold_future(future)));
        (Some(sender), Some(forwarder))
    }
}

/// Prepaid application payload, staging and wire bytes for each stdio response ID.
#[derive(Default)]
pub(crate) struct RequestLeases(std::sync::Mutex<RequestLeaseState>);

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
    pub(crate) fn on_request(&self, id: &rmcp::model::RequestId) {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.cancelled.retain(|(entry, _)| entry != id);
    }

    // A complete queued response remains deliverable after a late cancellation.
    #[cfg(test)]
    pub(crate) fn on_cancel(&self, id: &rmcp::model::RequestId) -> bool {
        self.on_cancel_owned(id, crate::lease::Owner::default())
    }

    pub(crate) fn on_cancel_owned(
        &self,
        id: &rmcp::model::RequestId,
        owner: crate::lease::Owner,
    ) -> bool {
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

    pub(crate) fn register_response(
        &self,
        id: rmcp::model::RequestId,
        cancel: &CancellationToken,
        staging: lease::Lease,
        outbound: lease::Lease,
        payload_owner: lease::Owner,
    ) -> bool {
        let mut state = self.0.lock().unwrap_or_else(|p| p.into_inner());
        if state.closed
            || cancel.is_cancelled()
            || state.cancelled.iter().any(|(entry, _)| entry == &id)
        {
            drop(state);
            drop((staging, outbound, payload_owner));
            return false;
        }
        let replaced = state.entries.insert(id, (staging, outbound, payload_owner));
        drop(state);
        drop(replaced);
        true
    }

    pub(crate) fn deliver(
        &self,
        id: rmcp::model::RequestId,
        cancel: &CancellationToken,
        result: CallToolResult,
        staging: lease::Lease,
        outbound: lease::Lease,
        payload_owner: lease::Owner,
    ) -> CallToolResult {
        if !self.register_response(id, cancel, staging, outbound, payload_owner) {
            drop(result);
            return CallToolResult::error(vec![ContentBlock::text(
                "Cancelled before the response was queued.",
            )]);
        }
        result
    }

    #[cfg(test)]
    pub(crate) fn insert(
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

    pub(crate) fn take(
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

    pub(crate) fn close(&self) {
        let mut state = self.0.lock().unwrap_or_else(|p| p.into_inner());
        state.closed = true;
        state.cancelled.clear();
        // The SDK may still own queued responses during shutdown. Their leases
        // remain with the session until every producer and transport owner ends.
    }
}

#[cfg(test)]
mod portable_schema_tests {
    use super::*;
    use crate::tasks::{Limits, Tasks};
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
mod instructions_tool_tests {
    use super::*;
    use crate::config::{
        INSTRUCTIONS_TOOL, MOVE_INSTRUCTIONS_POINTER, instructions_tool_mode_of,
        server_instructions,
    };
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
        let needed = 4 * lease::json_len(&borrowed).unwrap();
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
mod event_notification_tests {
    use super::*;
    use crate::tasks::{Limits, Tasks};
    use rmcp::model::{ClientJsonRpcMessage, ServerJsonRpcMessage};
    use rmcp::transport::Transport;
    use tokio::sync::{mpsc, oneshot};

    struct HeldNotification {
        message: ServerJsonRpcMessage,
        complete: oneshot::Sender<()>,
    }

    struct TestServer;

    impl ServerHandler for TestServer {}

    struct PausedTransport(mpsc::Sender<HeldNotification>);

    impl Transport<RoleServer> for PausedTransport {
        type Error = std::io::Error;

        fn send(
            &mut self,
            message: ServerJsonRpcMessage,
        ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'static {
            let outgoing = self.0.clone();
            async move {
                let (complete, waiting) = oneshot::channel();
                outgoing
                    .send(HeldNotification { message, complete })
                    .await
                    .map_err(|_| std::io::Error::other("test receiver closed"))?;
                waiting
                    .await
                    .map_err(|_| std::io::Error::other("test send dropped"))
            }
        }

        async fn receive(&mut self) -> Option<ClientJsonRpcMessage> {
            std::future::pending().await
        }

        async fn close(&mut self) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn event_notification_backpressure_does_not_delay_stream_cancel() {
        let tasks = Tasks::new(Limits::OFF).unwrap();
        let service = Arc::new(Service::new(tasks, false));
        let bus = Arc::clone(service.event_bus());
        let launch = Arc::new(LaunchContext::new(std::env::temp_dir(), Vec::new()));
        let server = Server::new(
            Backend::Local {
                service,
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
        let (outgoing, mut queued) = mpsc::channel(1);
        let mut peer =
            rmcp::service::serve_directly(server.clone(), PausedTransport(outgoing), None);
        let context = RequestContext::new(
            rmcp::model::RequestId::String("stream-cancel".into()),
            peer.peer().clone(),
        );
        let cancel = context.ct.clone();
        let mut streaming = tokio::spawn(async move {
            server
                .handle_events_stream(
                    Some(serde_json::json!({"name": events::EVENT_EXTERNAL})),
                    context,
                )
                .await
        });
        let pending = tokio::time::timeout(std::time::Duration::from_secs(5), queued.recv())
            .await
            .unwrap()
            .unwrap();
        let value = serde_json::to_value(&pending.message).unwrap();
        assert_eq!(value["method"], "notifications/events/active");
        assert_eq!(bus.active_streams(), 1);

        cancel.cancel();
        let completed = tokio::time::timeout(std::time::Duration::from_secs(5), &mut streaming)
            .await
            .expect("cancel must release the subscription before the notification can be written")
            .unwrap()
            .unwrap();
        assert_eq!(completed.0["resultType"], "complete");
        assert_eq!(bus.active_streams(), 0);
        drop(pending.message);
        pending.complete.send(()).unwrap();
        peer.close().await.unwrap();
    }
    #[tokio::test]
    async fn event_notification_cancel_keeps_owners_until_transport_drops_it() {
        let (outgoing, mut queued) = mpsc::channel(1);
        let mut peer = rmcp::service::serve_directly(TestServer, PausedTransport(outgoing), None);
        let context = RequestContext::new(
            rmcp::model::RequestId::String("stream-owner".into()),
            peer.peer().clone(),
        );
        let cancel = context.ct.clone();
        let payload_pool = lease::BytePool::new(64);
        let metadata_pool = lease::BytePool::new(64);
        let payload = serde_json::json!({"cursor": "current"});
        let payload_bytes = lease::json_len(&payload).unwrap();
        let metadata_bytes = lease::json_len(&context.id).unwrap();
        let params = lease::Owned::new(
            payload,
            lease::Owner::new(payload_pool.try_acquire(payload_bytes).unwrap()),
        );
        let metadata = lease::Owner::new(metadata_pool.try_acquire(metadata_bytes).unwrap());
        let mut sending = tokio::spawn(async move {
            send_event_notification(
                &context,
                "notifications/events/heartbeat",
                params,
                &context.id,
                &metadata,
            )
            .await
        });
        let pending = tokio::time::timeout(std::time::Duration::from_secs(5), queued.recv())
            .await
            .unwrap()
            .unwrap();

        cancel.cancel();
        assert!(
            !tokio::time::timeout(std::time::Duration::from_secs(5), &mut sending)
                .await
                .expect("notification send must observe cancellation while transport is paused")
                .unwrap()
        );
        assert_eq!(payload_pool.used(), payload_bytes);
        assert_eq!(metadata_pool.used(), metadata_bytes);
        drop(pending.message);
        assert_eq!(payload_pool.used(), 0);
        assert_eq!(metadata_pool.used(), 0);
        pending.complete.send(()).unwrap();
        peer.close().await.unwrap();
    }

    #[tokio::test]
    async fn event_notification_pre_cancel_sends_no_notification() {
        let (outgoing, mut queued) = mpsc::channel(1);
        let mut peer = rmcp::service::serve_directly(TestServer, PausedTransport(outgoing), None);
        let context = RequestContext::new(
            rmcp::model::RequestId::String("stream-pre-cancel".into()),
            peer.peer().clone(),
        );
        context.ct.cancel();
        assert!(
            !tokio::time::timeout(
                std::time::Duration::from_secs(5),
                send_event_notification(
                    &context,
                    "notifications/events/active",
                    lease::Owned::new(serde_json::json!({}), lease::Owner::default()),
                    &context.id,
                    &lease::Owner::default(),
                ),
            )
            .await
            .expect("a pre-cancelled request must not wait for the transport")
        );

        let after_cancel = tokio::spawn(async move {
            context
                .peer
                .send_notification(build_event_notification(
                    "notifications/events/heartbeat",
                    serde_json::json!({"cursor": "after-cancel"}),
                    &context.id,
                ))
                .await
        });
        let pending = tokio::time::timeout(std::time::Duration::from_secs(5), queued.recv())
            .await
            .unwrap()
            .unwrap();
        let value = serde_json::to_value(&pending.message).unwrap();
        assert_eq!(value["method"], "notifications/events/heartbeat");
        assert_eq!(value["params"]["cursor"], "after-cancel");
        drop(pending.message);
        pending.complete.send(()).unwrap();
        after_cancel.await.unwrap().unwrap();
        peer.close().await.unwrap();
    }

    #[tokio::test]
    async fn http_stateless_requests_share_launch_while_stateful_sessions_isolate_caches() {
        let tasks = Tasks::new(Limits::OFF).unwrap();
        let service = Arc::new(Service::new(tasks, true));
        let shared_launch = Arc::new(LaunchContext::new(std::env::temp_dir(), Vec::new()));
        let principal_session = Session::new(
            "t1".into(),
            "http-t1".into(),
            Arc::new(shared_launch.for_session()),
        );
        let principals: Arc<[Arc<Session>]> = Arc::from([Arc::clone(&principal_session)]);
        let make_http_server = || {
            let mut server = Server::new(
                Backend::Local {
                    service: Arc::clone(&service),
                    session: Arc::new(OnceLock::new()),
                    launch: Arc::clone(&shared_launch),
                    daemon: true,
                },
                &shared_launch,
                None,
                Tools::default(),
                None,
                InstructionsToolMode::Off,
            );
            server.principals = Some(Arc::clone(&principals));
            server
        };

        // Stateless HTTP call uses the pre-built principal session without initializing OnceLock.
        let stateless_server = make_http_server();
        let (tx0, _rx0) = mpsc::channel(1);
        let mut peer0 = rmcp::service::serve_directly(TestServer, PausedTransport(tx0), None);
        let mut stateless_ctx = RequestContext::new(
            rmcp::model::RequestId::String("stateless".into()),
            peer0.peer().clone(),
        );
        let (mut parts, ()) = ::http::Request::new(()).into_parts();
        parts.extensions.insert(Principal(0));
        stateless_ctx.extensions.insert(parts);
        let resolved = stateless_server.session(&stateless_ctx).unwrap();
        assert!(Arc::ptr_eq(&resolved, &principal_session));
        if let Backend::Local {
            session, launch, ..
        } = &stateless_server.backend
        {
            assert!(session.get().is_none());
            assert!(Arc::ptr_eq(launch, &shared_launch));
        }
        peer0.close().await.unwrap();

        // Stateful HTTP initialize isolates each session's LaunchContext from shared_launch and peers.
        let init_params: InitializeRequestParams = serde_json::from_value(serde_json::json!({
            "protocolVersion": "2025-03-26",
            "capabilities": {},
            "clientInfo": { "name": "http-stateful", "version": "1.0" }
        }))
        .unwrap();
        let stateful_a = make_http_server();
        let (txa, _rxa) = mpsc::channel(1);
        let mut peera = rmcp::service::serve_directly(TestServer, PausedTransport(txa), None);
        stateful_a
            .initialize(
                init_params.clone(),
                RequestContext::new(
                    rmcp::model::RequestId::String("init-a".into()),
                    peera.peer().clone(),
                ),
            )
            .await
            .unwrap();
        let sess_a = stateful_a
            .session(&RequestContext::new(
                rmcp::model::RequestId::String("call-a".into()),
                peera.peer().clone(),
            ))
            .unwrap();
        peera.close().await.unwrap();

        let stateful_b = make_http_server();
        let (txb, _rxb) = mpsc::channel(1);
        let mut peerb = rmcp::service::serve_directly(TestServer, PausedTransport(txb), None);
        stateful_b
            .initialize(
                init_params,
                RequestContext::new(
                    rmcp::model::RequestId::String("init-b".into()),
                    peerb.peer().clone(),
                ),
            )
            .await
            .unwrap();
        let sess_b = stateful_b
            .session(&RequestContext::new(
                rmcp::model::RequestId::String("call-b".into()),
                peerb.peer().clone(),
            ))
            .unwrap();
        peerb.close().await.unwrap();

        assert!(!Arc::ptr_eq(&sess_a.launch, &shared_launch));
        assert!(!Arc::ptr_eq(&sess_b.launch, &shared_launch));
        assert!(!Arc::ptr_eq(&sess_a.launch, &sess_b.launch));

        // Stdio server (principals: None) shares its single LaunchContext directly.
        let stdio_server = Server::new(
            Backend::Local {
                service: Arc::clone(&service),
                session: Arc::new(OnceLock::new()),
                launch: Arc::clone(&shared_launch),
                daemon: false,
            },
            &shared_launch,
            None,
            Tools::default(),
            None,
            InstructionsToolMode::Off,
        );
        let (txs, _rxs) = mpsc::channel(1);
        let mut peers = rmcp::service::serve_directly(TestServer, PausedTransport(txs), None);
        let stdio_sess = stdio_server
            .session(&RequestContext::new(
                rmcp::model::RequestId::String("stdio".into()),
                peers.peer().clone(),
            ))
            .unwrap();
        assert!(Arc::ptr_eq(&stdio_sess.launch, &shared_launch));
        peers.close().await.unwrap();
    }

    #[test]
    fn instance_pools_never_touch_globals_and_control_plane_falls_back_to_control_memory() {
        const EXACT_NAME: &str = "mcp::event_notification_tests::instance_pools_never_touch_globals_and_control_plane_falls_back_to_control_memory";
        if !std::env::args().any(|arg| arg == "--exact") {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", EXACT_NAME, "--nocapture"])
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(verify_instance_pool_isolation());
    }

    async fn verify_instance_pool_isolation() {
        assert_eq!(lease::process_pool().used(), 0);
        assert_eq!(lease::control_pool().used(), 0);

        let init_params: InitializeRequestParams = serde_json::from_value(serde_json::json!({
            "protocolVersion": "2025-03-26",
            "capabilities": {},
            "clientInfo": { "name": "pool-isolation", "version": "1.0" }
        }))
        .unwrap();

        let make_server = |main_limit: usize, control_limit: usize| {
            let pool = lease::BytePool::new(main_limit);
            let control = lease::BytePool::new(control_limit);
            let launch = Arc::new(
                LaunchContext::new(std::env::temp_dir(), Vec::new())
                    .with_memory(Arc::clone(&pool))
                    .with_control_memory(Arc::clone(&control)),
            );
            let tasks = Tasks::new(Limits::OFF).unwrap();
            let server = Server::new(
                Backend::Local {
                    service: Arc::new(Service::new(tasks, false)),
                    session: Arc::new(OnceLock::new()),
                    launch: Arc::clone(&launch),
                    daemon: false,
                },
                &launch,
                None,
                Tools::default(),
                Some("extra directive"),
                InstructionsToolMode::Copy,
            );
            (server, pool, control)
        };

        // 1. Both isolated pools saturated (0 bytes): all 6 paths refuse/drop without touching globals.
        {
            let (server, pool, control) = make_server(0, 0);
            let (outgoing, mut queued) = mpsc::channel(4);
            let mut peer =
                rmcp::service::serve_directly(TestServer, PausedTransport(outgoing), None);
            let ctx = |id: &str| {
                RequestContext::new(
                    rmcp::model::RequestId::String(id.into()),
                    peer.peer().clone(),
                )
            };

            let init_err = server
                .initialize(init_params.clone(), ctx("init-full"))
                .await
                .unwrap_err();
            assert!(
                init_err
                    .message
                    .contains("Server active memory budget is full")
            );
            let instr_err = server
                .instructions(Parameters(serde_json::Map::new()), ctx("instr-full"))
                .await
                .unwrap_err();
            assert!(
                instr_err
                    .message
                    .contains("Server active memory budget is full")
            );
            let disc_err = server.discover(ctx("disc-full")).await.unwrap_err();
            assert!(
                disc_err
                    .message
                    .contains("Server active memory budget is full")
            );
            let list_err = server.list_tools(None, ctx("list-full")).await.unwrap_err();
            assert!(
                list_err
                    .message
                    .contains("Server active memory budget is full")
            );
            let ev_err = server
                .handle_events_poll(
                    Some(serde_json::json!({"name": "no.such.event"})),
                    ctx("poll-full"),
                )
                .await
                .unwrap_err();
            assert_eq!(ev_err.code.0, -32025);
            assert!(
                ev_err.message.contains(
                    "Server active memory budget is full while preparing the event error"
                )
            );

            let mut prog_ctx = ctx("prog-full");
            prog_ctx.meta.set_progress_token(rmcp::model::ProgressToken(
                rmcp::model::NumberOrString::Number(1),
            ));
            let (sender, forwarder) = server.progress(&prog_ctx);
            sender
                .as_ref()
                .unwrap()
                .send(Some((1.0, "dropped-progress".to_owned())))
                .unwrap();
            tokio::task::yield_now().await;
            drop(sender);
            drop(forwarder);
            assert!(queued.try_recv().is_err());

            assert_eq!(pool.used(), 0);
            assert_eq!(control.used(), 0);
            assert_eq!(lease::process_pool().used(), 0);
            assert_eq!(lease::control_pool().used(), 0);
            peer.close().await.unwrap();
        }

        // 2. Main pool saturated (0 bytes), control pool ample: initialize and instructions stay
        // refused, while discover, list_tools, event_error_to_rmcp, and progress fall back to control.
        {
            let (server, pool, control) = make_server(0, 64 * 1024);
            let (outgoing, mut queued) = mpsc::channel(4);
            let mut peer =
                rmcp::service::serve_directly(TestServer, PausedTransport(outgoing), None);
            let ctx = |id: &str| {
                RequestContext::new(
                    rmcp::model::RequestId::String(id.into()),
                    peer.peer().clone(),
                )
            };

            assert!(
                server
                    .initialize(init_params.clone(), ctx("init-ctrl"))
                    .await
                    .is_err()
            );
            assert!(
                server
                    .instructions(Parameters(serde_json::Map::new()), ctx("instr-ctrl"))
                    .await
                    .is_err()
            );
            assert_eq!(control.used(), 0);

            let discovered = server.discover(ctx("disc-ctrl")).await.unwrap();
            assert!(!discovered.supported_versions.is_empty());
            let listed = server.list_tools(None, ctx("list-ctrl")).await.unwrap();
            assert!(!listed.tools.is_empty());
            let ev_err = server
                .handle_events_poll(
                    Some(serde_json::json!({"name": "no.such.event"})),
                    ctx("poll-ctrl"),
                )
                .await
                .unwrap_err();
            assert_eq!(ev_err.code.0, -32023);

            let mut prog_ctx = ctx("prog-ctrl");
            prog_ctx.meta.set_progress_token(rmcp::model::ProgressToken(
                rmcp::model::NumberOrString::Number(2),
            ));
            let (sender, _forwarder) = server.progress(&prog_ctx);
            let msg = "control-progress-payload";
            sender
                .as_ref()
                .unwrap()
                .send(Some((2.0, msg.to_owned())))
                .unwrap();
            let pending = tokio::time::timeout(std::time::Duration::from_secs(5), queued.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(pool.used(), 0);
            assert_eq!(control.used(), msg.len());
            assert_eq!(lease::process_pool().used(), 0);
            assert_eq!(lease::control_pool().used(), 0);
            drop(pending.message);
            assert_eq!(control.used(), 0);
            pending.complete.send(()).unwrap();
            peer.close().await.unwrap();
        }

        // 3. Main pool ample, control pool zero: initialize and instructions succeed on instance main pool.
        {
            let (server, pool, control) = make_server(64 * 1024, 0);
            let (outgoing, _queued) = mpsc::channel(1);
            let mut peer =
                rmcp::service::serve_directly(TestServer, PausedTransport(outgoing), None);
            let ctx = |id: &str| {
                RequestContext::new(
                    rmcp::model::RequestId::String(id.into()),
                    peer.peer().clone(),
                )
            };

            server
                .initialize(init_params, ctx("init-main"))
                .await
                .unwrap();
            let instr = server
                .instructions(Parameters(serde_json::Map::new()), ctx("instr-main"))
                .await
                .unwrap();
            assert_eq!(instr.is_error, Some(false));
            assert_eq!(pool.used(), 0);
            assert_eq!(control.used(), 0);
            assert_eq!(lease::process_pool().used(), 0);
            assert_eq!(lease::control_pool().used(), 0);
            peer.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn inferred_base_and_transfer_support_http2_authority_and_reject_userinfo_or_duplicate_hosts()
     {
        let dir = std::env::temp_dir().join(format!(
            "fastexec-mcp-transfer-{}",
            id::random_chars(10).unwrap()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let sample = dir.join("item.txt");
        std::fs::write(&sample, b"hello").unwrap();

        let tasks = Tasks::new(Limits::OFF).unwrap();
        let service = Arc::new(Service::new(tasks, true));
        let launch = Arc::new(LaunchContext::new(dir.clone(), Vec::new()));
        let table = Arc::new(transfer::Transfers::new());
        let server = Server::new(
            Backend::Local {
                service,
                session: Arc::new(OnceLock::new()),
                launch: Arc::clone(&launch),
                daemon: true,
            },
            &launch,
            Some(TransferEndpoint::Http {
                table: Arc::clone(&table),
                public_root: None,
            }),
            Tools {
                codemode: false,
                transfer: true,
                yolo: false,
            },
            None,
            InstructionsToolMode::Off,
        );
        let (tx, _rx) = mpsc::channel(1);
        let mut peer = rmcp::service::serve_directly(TestServer, PausedTransport(tx), None);

        let call_transfer = |parts: ::http::request::Parts| {
            let mut ctx =
                RequestContext::new(rmcp::model::RequestId::Number(1), peer.peer().clone());
            ctx.extensions.insert(parts);
            let path = sample.to_string_lossy().into_owned();
            server.transfer(Parameters(transfer::Request { path }), ctx)
        };

        // HTTP/2 request without Host header uses :authority and :scheme.
        let (h2_https_parts, ()) = ::http::Request::builder()
            .uri("https://127.0.0.1:8787/mcp")
            .body(())
            .unwrap()
            .into_parts();
        assert_eq!(
            inferred_base(&h2_https_parts).unwrap(),
            "https://127.0.0.1:8787/"
        );
        let ok_res = call_transfer(h2_https_parts).await;
        assert_eq!(ok_res.is_error, Some(false));
        let ok_text = &ok_res.content[0].as_text().unwrap().text;
        assert!(
            ok_text.starts_with("https://127.0.0.1:8787/t/"),
            "{ok_text}"
        );

        // X-Forwarded-Proto overrides :scheme in both directions.
        let (xfp_https, ()) = ::http::Request::builder()
            .uri("http://[::1]:8787/mcp")
            .header("x-forwarded-proto", "https, http")
            .body(())
            .unwrap()
            .into_parts();
        assert_eq!(inferred_base(&xfp_https).unwrap(), "https://[::1]:8787/");
        let (xfp_http, ()) = ::http::Request::builder()
            .uri("https://localhost:8787/mcp")
            .header("x-forwarded-proto", "http")
            .body(())
            .unwrap()
            .into_parts();
        assert_eq!(inferred_base(&xfp_http).unwrap(), "http://localhost:8787/");

        // Missing both Host and :authority fails with the canonical message.
        let (no_host, ()) = ::http::Request::builder()
            .uri("/mcp")
            .body(())
            .unwrap()
            .into_parts();
        assert_eq!(
            inferred_base(&no_host).unwrap_err(),
            "Cannot make a transfer URL: the request names no Host."
        );

        // Empty host (`:8787`) is rejected.
        let (empty_host, ()) = ::http::Request::builder()
            .uri("/mcp")
            .header(::http::header::HOST, ":8787")
            .body(())
            .unwrap()
            .into_parts();
        assert!(inferred_base(&empty_host).is_err());

        // Userinfo in Host or HTTP/2 :authority is rejected without echoing credentials.
        let (evil_header, ()) = ::http::Request::builder()
            .uri("/mcp")
            .header(::http::header::HOST, "evil@127.0.0.1:8787")
            .body(())
            .unwrap()
            .into_parts();
        let err = inferred_base(&evil_header).unwrap_err();
        assert!(!err.contains("evil"), "{err}");
        let bad_res = call_transfer(evil_header).await;
        assert_eq!(bad_res.is_error, Some(true));

        let evil_uri = ::http::Uri::builder()
            .scheme("http")
            .authority("evil@127.0.0.1:8787")
            .path_and_query("/mcp")
            .build()
            .unwrap();
        let (evil_h2, ()) = ::http::Request::builder()
            .uri(evil_uri)
            .body(())
            .unwrap()
            .into_parts();
        let err = inferred_base(&evil_h2).unwrap_err();
        assert!(!err.contains("evil"), "{err}");
        let bad_h2_res = call_transfer(evil_h2).await;
        assert_eq!(bad_h2_res.is_error, Some(true));

        // Duplicate Host headers are rejected even when identical.
        for second in ["127.0.0.1:8787", "localhost:8787"] {
            let (mut dup_parts, ()) = ::http::Request::builder()
                .uri("http://127.0.0.1:8787/mcp")
                .body(())
                .unwrap()
                .into_parts();
            dup_parts
                .headers
                .append(::http::header::HOST, "127.0.0.1:8787".parse().unwrap());
            dup_parts
                .headers
                .append(::http::header::HOST, second.parse().unwrap());
            assert!(inferred_base(&dup_parts).is_err());
            let dup_res = call_transfer(dup_parts).await;
            assert_eq!(dup_res.is_error, Some(true));
        }

        peer.close().await.unwrap();
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn concurrent_backend_local_drops_always_remove_event_bus_session_cursor() {
        let tasks = Tasks::new(Limits::OFF).unwrap();
        let service = Arc::new(Service::new(tasks, false));
        let bus = Arc::clone(service.event_bus());
        bus.publish_external(events::ExternalEventInput {
            event_type: None,
            source: "ci".to_owned(),
            topic: None,
            name: "done".to_owned(),
            event_id: "ev-1".to_owned(),
            timestamp: None,
            data: serde_json::json!({}),
        })
        .unwrap();

        let launch = Arc::new(LaunchContext::new(std::env::temp_dir(), Vec::new()));
        let (tx, _rx) = mpsc::channel(1);
        let mut peer = rmcp::service::serve_directly(TestServer, PausedTransport(tx), None);

        for round in 0..128 {
            let backend = Backend::Local {
                service: Arc::clone(&service),
                session: Arc::new(OnceLock::new()),
                launch: Arc::clone(&launch),
                daemon: false,
            };
            let server = Server::new(
                backend.clone(),
                &launch,
                None,
                Tools::default(),
                None,
                InstructionsToolMode::Off,
            );
            let sess = server
                .session(&RequestContext::new(
                    rmcp::model::RequestId::Number(round),
                    peer.peer().clone(),
                ))
                .unwrap();
            let sess_id = sess.id.clone();
            drop(sess);
            drop(server);

            assert!(bus.external_background_summary(&sess_id).is_some());
            assert!(bus.external_background_summary(&sess_id).is_none());

            let clones: Vec<Backend> = (0..8).map(|_| backend.clone()).collect();
            drop(backend);
            let barrier = std::sync::Barrier::new(clones.len());
            std::thread::scope(|scope| {
                for clone in clones {
                    let barrier = &barrier;
                    scope.spawn(move || {
                        barrier.wait();
                        drop(clone);
                    });
                }
            });

            assert!(
                bus.external_background_summary(&sess_id).is_some(),
                "round {round}: session cursor for {sess_id} was not removed on concurrent drop"
            );
            bus.remove_session(&sess_id);
        }

        peer.close().await.unwrap();
    }
}
