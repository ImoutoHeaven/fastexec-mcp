use super::leases::HttpResponseLease;
use super::server::Server;
use crate::lease;
use crate::skills::{SkillCatalog, SkillContent, SkillEntry, SkillReadError};
use rmcp::RoleServer;
use rmcp::model::{
    CacheScope, GetSkillRequestMethod, GetSkillRequestParams, GetSkillResult,
    ListResourceTemplatesRequestMethod, ListResourceTemplatesResult, ListResourcesRequestMethod,
    ListResourcesResult, ListSkillsRequestMethod, ListSkillsResult, PaginatedRequestParams,
    ProtocolVersion, ReadResourceDirectoryRequestMethod, ReadResourceDirectoryRequestParams,
    ReadResourceDirectoryResult, ReadResourceRequestMethod, ReadResourceRequestParams,
    ReadResourceResponse, ReadResourceResult, Resource, ResourceContents, ResultType, Skill,
    SkillFrontmatter, SkillResource, SkillResources,
};
use rmcp::service::RequestContext;
use std::sync::Arc;
use std::sync::atomic::Ordering;

const SKILLS_PAGE_SIZE: usize = 64;
const DIR_PAGE_SIZE: usize = 256;

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

impl Server {
    pub(super) async fn handle_list_skills(
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

    pub(super) async fn handle_get_skill(
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

    pub(super) async fn handle_read_resource(
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

    pub(super) async fn handle_read_resource_directory(
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

    pub(super) async fn handle_list_resources(
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

    pub(super) async fn handle_list_resource_templates(
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
}
