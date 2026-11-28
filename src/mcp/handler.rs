use super::leases::{reserve_control_response_content, reserve_response_content};
use super::schema::tool_metadata_copy_bytes;
use super::server::{Backend, Server, client_session};
use rmcp::model::{
    GetSkillRequestMethod, GetSkillRequestParams, GetSkillResult, Implementation,
    InitializeRequestParams, InitializeResult, ListResourceTemplatesRequestMethod,
    ListResourceTemplatesResult, ListResourcesRequestMethod, ListResourcesResult,
    ListSkillsRequestMethod, ListSkillsResult, PaginatedRequestParams, ProtocolVersion,
    ReadResourceDirectoryRequestMethod, ReadResourceDirectoryRequestParams,
    ReadResourceDirectoryResult, ReadResourceRequestMethod, ReadResourceRequestParams,
    ReadResourceResponse, SKILLS_EXTENSION_ID, ServerCapabilities, ServerConfig,
};
use rmcp::service::RequestContext;
use rmcp::{RoleServer, ServerHandler, tool_handler};
use std::sync::atomic::Ordering;

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
        self.handle_list_skills(request, context).await
    }

    async fn get_skill(
        &self,
        request: GetSkillRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<GetSkillResult, rmcp::ErrorData> {
        self.handle_get_skill(request, context).await
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResponse, rmcp::ErrorData> {
        self.handle_read_resource(request, context).await
    }

    async fn read_resource_directory(
        &self,
        request: ReadResourceDirectoryRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceDirectoryResult, rmcp::ErrorData> {
        self.handle_read_resource_directory(request, context).await
    }

    async fn list_resources(
        &self,
        request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, rmcp::ErrorData> {
        self.handle_list_resources(request, context).await
    }

    async fn list_resource_templates(
        &self,
        request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListResourceTemplatesResult, rmcp::ErrorData> {
        self.handle_list_resource_templates(request, context).await
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
