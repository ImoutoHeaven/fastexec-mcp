use super::leases::{
    Principal, RegisteredSession, RequestLeases, application_input_owner, deliver_tracked,
    http_context, inferred_base, request_input_owner, reserve_response_content,
};
use super::schema::{CodemodeArguments, exec_description, prepare_tool};
use crate::codemode;
use crate::config::{InstructionsToolMode, Tools, compose_instructions};
use crate::endpoint;
use crate::env::LaunchContext;
use crate::files;
use crate::id;
use crate::lease;
use crate::render;
use crate::render::files::{self as render_files, publish_transfer, transfer_error};
use crate::render::types::{AgentDelivery, TrackedResult};
use crate::service::core::overload_task_message;
use crate::service::schema::{Action, Request};
use crate::service::{ExecContext, Service};
use crate::skills::SkillCatalog;
use crate::tasks::session::Session;
use crate::transfer::{self, TransferEndpoint};
use crate::wait;
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, ContentBlock};
use rmcp::service::RequestContext;
use rmcp::{RoleServer, tool};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use tokio::sync::watch;

#[derive(Clone)]
pub(crate) struct Server {
    pub(crate) backend: Backend,
    pub(super) instructions: Arc<str>,
    tool_instructions: Option<Arc<str>>,
    /// Connection instances share the immutable routing and tool metadata for their host.
    pub(super) router: Arc<ToolRouter<Server>>,
    transfer: Option<TransferEndpoint>,
    tools: Tools,
    /// Sessionless HTTP calls select the session for their authenticated token.
    pub(crate) principals: Option<Arc<[Arc<Session>]>>,
    /// Outbound active-byte leases indexed by JSON-RPC request ID for stdio responses.
    pub(crate) stdio_leases: Option<Arc<RequestLeases>>,
    /// Optional read-only skills catalog exposed on MCP `2026-07-28+`.
    pub(super) skills: Option<Arc<SkillCatalog>>,
    /// True once this session negotiated via legacy `initialize` or a `< 2026-07-28` HTTP request.
    pub(super) legacy_session: Arc<AtomicBool>,
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

    pub(super) fn pools(&self) -> (&Arc<lease::BytePool>, &Arc<lease::BytePool>) {
        match &self.backend {
            Backend::Local { launch, .. } => (launch.memory(), launch.control_memory()),
            Backend::Daemon(endpoint) => (endpoint.memory(), endpoint.control_memory()),
        }
    }

    /// The principal session for sessionless HTTP, or this server's client-named session.
    pub(super) fn session(
        &self,
        context: &RequestContext<RoleServer>,
    ) -> Result<Arc<Session>, String> {
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
pub(super) fn client_session(
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
pub(super) fn client_name(context: &RequestContext<RoleServer>) -> String {
    context
        .client_info()
        .map_or_else(|| "client".to_string(), |info| info.name)
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

    #[doc = include_str!("../description.md")]
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

    #[doc = include_str!("../description-codemode.md")]
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
    pub(super) async fn transfer(
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
    pub(super) async fn instructions(
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

/// Aborts the progress forwarder however the call returns, including through `?`.
pub(super) struct Forwarder(tokio::task::JoinHandle<()>);

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
    pub(super) fn progress(
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
