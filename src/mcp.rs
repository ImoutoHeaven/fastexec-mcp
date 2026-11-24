//! MCP server handler, tool routing, schema rewriting, and progress/event forwarding.

use crate::codemode;
use crate::config::{InstructionsToolMode, Tools, compose_instructions};
use crate::endpoint;
use crate::events;
use crate::files;
use crate::http;
use crate::ingress;
use crate::ipc;
use crate::lease;
use crate::service::render::{self as service_render, Render};
use crate::service::schema::{Action, Request};
use crate::service::{Call, Service};
use crate::session::{LaunchContext, Session};
use crate::tasks;
use crate::transfer::{self, TransferEndpoint, publish_transfer, transfer_error};
use crate::wait;
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{
    CallToolResult, ContentBlock, Implementation, ProtocolVersion, ServerCapabilities,
    ServerConfig, ToolAnnotations,
};
use rmcp::service::RequestContext;
use rmcp::{RoleServer, ServerHandler, tool, tool_handler};
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
}

/// Where this server's calls run: in this process, or on the namespace's daemon.
#[derive(Clone)]
pub(crate) enum Backend {
    Local {
        service: Arc<Service>,
        render: Render,
        /// Created by the stdio client's first tool call.
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
        }
    }

    pub(crate) fn with_stdio_leases(mut self, stdio_leases: Arc<RequestLeases>) -> Self {
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
        if let Some(principals) = &self.principals {
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

/// A session named for the client that first uses this server.
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

/// The client's name from request metadata.
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
    let home = launch.home().map_or_else(
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
    fn supported_protocol_versions(&self) -> std::borrow::Cow<'static, [ProtocolVersion]> {
        const VERSIONS: &[ProtocolVersion] = &[ProtocolVersion::V_2026_07_28];
        std::borrow::Cow::Borrowed(VERSIONS)
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
        _context: RequestContext<RoleServer>,
    ) -> Result<rmcp::model::ListToolsResult, rmcp::ErrorData> {
        let bytes = self.router.map.values().fold(512usize, |bytes, route| {
            bytes.saturating_add(tool_metadata_copy_bytes(&route.attr))
        });
        let _response_workspace = reserve_response_content(lease::process_pool(), bytes)?;
        Ok(rmcp::model::ListToolsResult {
            result_type: Some(rmcp::model::ResultType::COMPLETE),
            tools: self.router.list_all(),
            ttl_ms: Some(0),
            cache_scope: Some(rmcp::model::CacheScope::Public),
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
    let message_bytes = match &err {
        events::EventError::InvalidParams(message)
        | events::EventError::NotFound { message, .. }
        | events::EventError::Forbidden(message)
        | events::EventError::ResourceExhausted { message, .. } => message.len(),
    };
    let Some(held) = lease::control_pool().try_acquire(message_bytes.saturating_add(256)) else {
        return rmcp::ErrorData::new(
            rmcp::model::ErrorCode(-32025),
            "Server active memory budget is full while preparing the event error",
            None,
        );
    };
    ipc::Owned::new(err, lease::Owner::new(held))
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
    Some(raw.to_owned())
}

/// Checks the HTTP `Mcp-Name` header for custom event methods the SDK does not validate.
fn validate_mcp_name_header(
    context: &RequestContext<RoleServer>,
    expected_name: &str,
) -> Result<(), rmcp::ErrorData> {
    let Some(parts) = context.extensions.get::<::http::request::Parts>() else {
        return Ok(());
    };
    let raw_header = parts
        .headers
        .get("mcp-name")
        .ok_or_else(|| rmcp::ErrorData::header_mismatch("Mcp-Name header is required", None))?;
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

pub(crate) fn event_error_payload(error: ipc::IpcEventError) -> serde_json::Value {
    events::value_object([("error", error.into())])
}

async fn send_event_notification(
    context: &RequestContext<RoleServer>,
    method: &str,
    params: ipc::Owned<serde_json::Value>,
    subscription_id: &rmcp::model::RequestId,
    metadata: &lease::Owner,
) -> bool {
    use rmcp::model::GetExtensions;

    let ipc::Owned { value, owner } = params;
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
                .map_err(event_error_to_rmcp)?,
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
                    .map_err(event_error_to_rmcp)?
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
        let (pool, control) = match &self.backend {
            Backend::Local { launch, .. } => (launch.memory(), launch.control_memory()),
            Backend::Daemon(endpoint) => (endpoint.memory(), endpoint.control_memory()),
        };
        // The pending notification and a reconnect request may hold independent ID/input copies.
        // Fixed metadata covers fastexec cursors and terminal error envelopes.
        let metadata_bytes = ipc::json_len(&(&sub_req, &context.id))
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
        let terminated = |error| ipc::Owned::new(event_error_payload(error), metadata.clone());
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
                let initial = sub
                    .initial_active()
                    .map_err(event_error_to_rmcp)?
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
                                Ok(Some(ipc::Owned { value, owner })) => {
                                    let (method, payload) = match value {
                                        events::StreamItem::Active(active) => ("notifications/events/active", active.into()),
                                        events::StreamItem::Event(event) => ("notifications/events/event", event.into()),
                                    };
                                    if !send_event_notification(&context, method, ipc::Owned::new(payload, owner), &sub_id, &metadata).await {
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
                let mut request = sub_req;
                let mut activated = false;
                loop {
                    let (initial, mut stream) = match endpoint
                        .events_stream(&client, request.clone(), context.ct.clone())
                        .await
                    {
                        Ok(stream) => stream,
                        Err(error) if !activated => return Err(error.run(ipc_event_error_to_rmcp)),
                        Err(error) => {
                            if !context.ct.is_cancelled() {
                                send_event_notification(
                                    &context,
                                    "notifications/events/terminated",
                                    error.map(event_error_payload),
                                    &sub_id,
                                    &metadata,
                                )
                                .await;
                            }
                            return Ok(stream_complete_result(&sub_id));
                        }
                    };

                    request.cursor = Some(initial.value.cursor.clone());
                    request.max_age_ms = None;
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

                    activated = true;
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
                                    if !terminated_sent && stream.is_lagged() {
                                        stream.cancel().await;
                                        break;
                                    }
                                    if !terminated_sent {
                                        send_event_notification(
                                            &context,
                                            "notifications/events/terminated",
                                            terminated(ipc::IpcEventError {
                                                code: -32000,
                                                message: "daemon connection closed".to_owned(),
                                                data: None,
                                            }),
                                            &sub_id,
                                            &metadata,
                                        ).await;
                                    }
                                    return Ok(stream_complete_result(&sub_id));
                                };
                                let owned = ipc::Owned::new(frame, lease::Owner::new(held));
                                let (method, payload, cursor) = match owned.value {
                                    ipc::EventWireFrame::Active(active) => {
                                        let cursor = active.cursor.clone();
                                        ("notifications/events/active", active.into(), Some(cursor))
                                    }
                                    ipc::EventWireFrame::Event(event) => {
                                        let cursor = event.cursor.clone();
                                        ("notifications/events/event", event.into(), Some(cursor))
                                    }
                                    ipc::EventWireFrame::Heartbeat { cursor } => {
                                        let payload = events::value_object([("cursor", serde_json::Value::String(cursor.clone()))]);
                                        ("notifications/events/heartbeat", payload, Some(cursor))
                                    }
                                    ipc::EventWireFrame::Error(error) => ("notifications/events/error", event_error_payload(error), None),
                                    ipc::EventWireFrame::Terminated(error) => {
                                        terminated_sent = true;
                                        ("notifications/events/terminated", event_error_payload(error), None)
                                    }
                                    ipc::EventWireFrame::Complete => return Ok(stream_complete_result(&sub_id)),
                                    ipc::EventWireFrame::ListResult(_) | ipc::EventWireFrame::PollResult(_) => continue,
                                };
                                if !send_event_notification(&context, method, ipc::Owned::new(payload, owned.owner), &sub_id, &metadata).await {
                                    stream.cancel().await;
                                    return Ok(stream_complete_result(&sub_id));
                                }
                                if let Some(cursor) = cursor {
                                    request.cursor = Some(cursor);
                                }
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
                        queued_guard: None,
                        render_slot: None,
                        http_response: http_context(&context),
                        rpc_id_bytes: ipc::json_len(&context.id).unwrap_or(usize::MAX),
                        input_owner: request_input_owner(&context),
                    };
                    let tracked = service.call_tracked(request, call).await;
                    self.deliver_local(&context, tracked, launch, *render, is_control)
                }
                Err(message) => {
                    let err = service_render::suppress(
                        service_render::error_result(request.action, &message),
                        *render,
                    );
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
                service,
                render,
                launch,
                ..
            } => {
                let session = match self.session(&context) {
                    Ok(session) => session,
                    Err(message) => {
                        return service_render::suppress(
                            service_render::codemode_error(&message),
                            *render,
                        );
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
                    queued_guard: None,
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
    pub(crate) fn deliver_local(
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
        let b64_bytes = service_render::unleased_result_payload_capacity(
            &tracked.result,
            &tracked.payload_owner,
        );
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
                Some(m) => service_render::overload_task_message(launch.memory().limit(), m),
                None => format!(
                    "Server active memory budget ({} bytes) is full; retry once in-flight transfers complete.",
                    launch.memory().limit()
                ),
            };
            let effective = render.effective();
            let mut err = CallToolResult::error(vec![ContentBlock::text(msg.clone())]);
            err.structured_content = Some(serde_json::json!({ "ok": false, "error": msg }));
            let suppressed = service_render::suppress(err, effective);
            let err_outbound = ipc::json_len(&suppressed)
                .unwrap_or(usize::MAX)
                .saturating_add(id_bytes)
                .saturating_add(256);
            let err_peak =
                service_render::result_payload_capacity(&suppressed).saturating_add(err_outbound);
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
        let b64_bytes = service_render::unleased_result_payload_capacity(
            &tracked.result,
            &tracked.payload_owner,
        );
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
            let err_peak =
                service_render::result_payload_capacity(&err).saturating_add(err_outbound);
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

    pub(crate) fn deliver(
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
            render: Render {
                output_mode: service_render::OutputMode::Text,
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
                render: Render {
                    output_mode: service_render::OutputMode::Text,
                    structured_content: true,
                },
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
        let payload_bytes = ipc::json_len(&payload).unwrap();
        let metadata_bytes = ipc::json_len(&context.id).unwrap();
        let params = ipc::Owned::new(
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
                    ipc::Owned::new(serde_json::json!({}), lease::Owner::default()),
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
}
