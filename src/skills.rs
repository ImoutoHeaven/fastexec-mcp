//! Read-only Skills catalog domain: anchored `DirFd` scanning, YAML frontmatter validation,
//! main-pool metadata leasing, and TOCTOU-verified resource reads.

use crate::files::path::is_windows_reserved_device_name;
use crate::lease::{BytePool, Lease};
use crate::media;
use crate::platform::dirfd::{DirEntry, DirFd, EntryKind};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

pub const MAX_SKILLS: usize = 64;
pub const MAX_FILES_PER_SKILL: usize = 512;
pub const MAX_SKILL_DEPTH: usize = 16;
pub const MAX_SKILL_MD_BYTES: usize = 64 * 1024;
pub const MAX_SKILL_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_TOTAL_SKILLS_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_SKILL_READ_WORKERS: usize = 8;
pub const DIRECTORY_MIME_TYPE: &str = "inode/directory";

const DIR_ENUM_LIMIT: usize = 4096;

#[derive(Clone, Debug, PartialEq)]
pub struct SkillEntry {
    pub uri: String,
    pub root_uri: String,
    pub name: String,
    pub description: String,
    pub frontmatter_extra: Map<String, Value>,
    pub resources: Vec<SkillFileEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SkillFileEntry {
    pub uri: String,
    pub rel_path: String,
    pub name: String,
    pub size: u64,
    pub digest: String,
    pub mime_type: &'static str,
    pub is_text: bool,
    segments: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SkillDirChild {
    pub uri: String,
    pub name: String,
    pub mime_type: &'static str,
    pub size: Option<u64>,
    pub is_dir: bool,
}

#[derive(Debug)]
pub enum SkillContent {
    Text(String),
    Blob(Vec<u8>),
}

#[derive(Debug)]
pub struct ReadSkillResource {
    pub uri: String,
    pub mime_type: &'static str,
    pub content: SkillContent,
    pub lease: Lease,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SkillReadError {
    NotFound(String),
    MemoryFull(String),
    Integrity(String),
    Cancelled,
    Io(String),
}

impl fmt::Display for SkillReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound(msg) | Self::MemoryFull(msg) | Self::Integrity(msg) | Self::Io(msg) => {
                f.write_str(msg)
            }
            Self::Cancelled => f.write_str("skill resource read cancelled"),
        }
    }
}

#[derive(Debug)]
pub struct SkillCatalog {
    root: Arc<DirFd>,
    skills: Vec<SkillEntry>,
    skill_by_uri: HashMap<String, usize>,
    files_by_uri: HashMap<String, SkillFileEntry>,
    dirs_by_uri: HashMap<String, Vec<SkillDirChild>>,
    read_permits: Arc<Semaphore>,
    _metadata_lease: Lease,
}

impl SkillCatalog {
    pub fn skills(&self) -> &[SkillEntry] {
        &self.skills
    }

    pub fn get_skill(&self, uri: &str) -> Option<&SkillEntry> {
        let &index = self.skill_by_uri.get(uri)?;
        self.skills.get(index)
    }

    #[cfg(test)]
    pub fn get_file(&self, uri: &str) -> Option<&SkillFileEntry> {
        self.files_by_uri.get(uri)
    }

    pub fn read_directory(&self, uri: &str) -> Option<&[SkillDirChild]> {
        self.dirs_by_uri.get(uri).map(Vec::as_slice)
    }

    #[cfg(test)]
    pub fn read_permits(&self) -> &Arc<Semaphore> {
        &self.read_permits
    }

    #[cfg(test)]
    pub fn metadata_lease_bytes(&self) -> usize {
        self._metadata_lease.bytes()
    }

    /// Re-opens a cataloged resource through the anchored `DirFd` inside a bounded blocking pool
    /// and verifies its raw byte length and `sha256:` digest before returning it.
    pub async fn read_resource(
        &self,
        uri: &str,
        memory: &Arc<BytePool>,
        cancel: &CancellationToken,
    ) -> Result<ReadSkillResource, SkillReadError> {
        let entry = self
            .files_by_uri
            .get(uri)
            .ok_or_else(|| SkillReadError::NotFound(format!("skill resource not found: {uri}")))?;
        let expected_size = usize::try_from(entry.size)
            .map_err(|_| SkillReadError::Integrity(format!("invalid size for {uri}")))?;
        let lease = memory.try_acquire(expected_size).ok_or_else(|| {
            SkillReadError::MemoryFull(format!(
                "Server active memory budget ({} bytes) is full while reading skill resource {uri}",
                memory.limit()
            ))
        })?;

        let permit = tokio::select! {
            biased;
            () = cancel.cancelled() => return Err(SkillReadError::Cancelled),
            permit = Arc::clone(&self.read_permits).acquire_owned() => {
                permit.map_err(|_| SkillReadError::Cancelled)?
            }
        };

        let root = Arc::clone(&self.root);
        let target = entry.clone();

        let handle = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            read_verified_file(&root, &target, lease)
        });

        handle
            .await
            .map_err(|err| SkillReadError::Io(format!("skill read worker failed: {err}")))?
    }
}

/// Validates a skill root or parent directory without allocating from any `BytePool`.
pub fn preflight_check(root: &Path) -> Result<(), String> {
    let _ = scan_root(root)?;
    Ok(())
}

/// Anchors `root` with `DirFd`, scans and validates all skills, and leases metadata bytes from
/// the caller's main `BytePool`.
pub fn load(root: &Path, memory: &Arc<BytePool>) -> Result<SkillCatalog, String> {
    let scanned = scan_root(root)?;
    let lease_bytes = estimate_metadata_bytes(&scanned);
    let metadata_lease = memory.try_acquire(lease_bytes).ok_or_else(|| {
        format!(
            "Server active memory budget ({} bytes) is full while loading skills metadata",
            memory.limit()
        )
    })?;
    Ok(SkillCatalog {
        root: Arc::new(scanned.root),
        skills: scanned.skills,
        skill_by_uri: scanned.skill_by_uri,
        files_by_uri: scanned.files_by_uri,
        dirs_by_uri: scanned.dirs_by_uri,
        read_permits: Arc::new(Semaphore::new(MAX_SKILL_READ_WORKERS)),
        _metadata_lease: metadata_lease,
    })
}

struct ScannedCatalog {
    root: DirFd,
    skills: Vec<SkillEntry>,
    skill_by_uri: HashMap<String, usize>,
    files_by_uri: HashMap<String, SkillFileEntry>,
    dirs_by_uri: HashMap<String, Vec<SkillDirChild>>,
}

fn scan_root(root: &Path) -> Result<ScannedCatalog, String> {
    let root_dirfd = DirFd::open_root(root)
        .map_err(|err| format!("cannot open skill root {}: {err}", root.display()))?;
    let root_entries = read_checked_entries(&root_dirfd, root.display().to_string().as_str())?;

    let mut skills = Vec::new();
    let mut files_by_uri = HashMap::new();
    let mut dirs_by_uri = HashMap::new();
    let mut total_bytes: u64 = 0;

    if let Some(skill_md) = root_entries.iter().find(|entry| entry.name == "SKILL.md") {
        match skill_md.kind {
            EntryKind::Link => {
                return Err(format!(
                    "skill root {} contains a symlink SKILL.md",
                    root.display()
                ));
            }
            EntryKind::File => {
                let dir_name =
                    root.file_name()
                        .and_then(|name| name.to_str())
                        .ok_or_else(|| {
                            format!(
                                "single-skill root {} has no valid directory name",
                                root.display()
                            )
                        })?;
                validate_segment(dir_name)?;
                let skill = scan_skill(
                    &root_dirfd,
                    dir_name,
                    &[],
                    &mut total_bytes,
                    &mut files_by_uri,
                    &mut dirs_by_uri,
                )?;
                skills.push(skill);
            }
            EntryKind::Other => {
                return Err(format!(
                    "skill root {} has non-regular SKILL.md",
                    root.display()
                ));
            }
            EntryKind::Directory => {
                scan_multi_skill_root(
                    &root_dirfd,
                    root,
                    &root_entries,
                    &mut total_bytes,
                    &mut skills,
                    &mut files_by_uri,
                    &mut dirs_by_uri,
                )?;
            }
        }
    } else {
        scan_multi_skill_root(
            &root_dirfd,
            root,
            &root_entries,
            &mut total_bytes,
            &mut skills,
            &mut files_by_uri,
            &mut dirs_by_uri,
        )?;
    }

    skills.sort_by(|a, b| a.name.cmp(&b.name));
    let mut skill_by_uri = HashMap::with_capacity(skills.len());
    for (idx, skill) in skills.iter().enumerate() {
        skill_by_uri.insert(skill.uri.clone(), idx);
    }

    Ok(ScannedCatalog {
        root: root_dirfd,
        skills,
        skill_by_uri,
        files_by_uri,
        dirs_by_uri,
    })
}

fn scan_multi_skill_root(
    root_dirfd: &DirFd,
    root_path: &Path,
    root_entries: &[CheckedEntry],
    total_bytes: &mut u64,
    skills: &mut Vec<SkillEntry>,
    files_by_uri: &mut HashMap<String, SkillFileEntry>,
    dirs_by_uri: &mut HashMap<String, Vec<SkillDirChild>>,
) -> Result<(), String> {
    for entry in root_entries {
        if entry.kind == EntryKind::Link {
            return Err(format!(
                "skill root {} contains forbidden symlink or reparse point {:?}",
                root_path.display(),
                entry.name
            ));
        }
    }

    for entry in root_entries {
        if entry.kind != EntryKind::Directory {
            continue;
        }
        let child_dir = root_dirfd.open_subdir(&entry.name).map_err(|err| {
            format!(
                "cannot open skill directory {:?} under {}: {err}",
                entry.name,
                root_path.display()
            )
        })?;
        let child_entries = child_dir
            .read_entries(DIR_ENUM_LIMIT)
            .map_err(|err| format!("cannot list {:?}: {err}", entry.name))?;
        let Some(skill_md) = child_entries.iter().find(|e| e.name == "SKILL.md") else {
            // Plain directories without SKILL.md (such as .git) are ignored.
            continue;
        };
        match skill_md.kind {
            EntryKind::Link => {
                return Err(format!(
                    "skill {:?} contains a symlink or reparse point SKILL.md",
                    entry.name
                ));
            }
            EntryKind::File => {}
            EntryKind::Directory | EntryKind::Other => continue,
        }

        validate_segment(&entry.name)?;
        if skills.len() >= MAX_SKILLS {
            return Err(format!(
                "skill root {} exceeds maximum of {MAX_SKILLS} skills",
                root_path.display()
            ));
        }
        let prefix = [entry.name.clone()];
        let skill = scan_skill(
            &child_dir,
            &entry.name,
            &prefix,
            total_bytes,
            files_by_uri,
            dirs_by_uri,
        )?;
        skills.push(skill);
    }
    Ok(())
}

struct SkillScanState<'a> {
    skill_name: &'a str,
    root_uri: &'a str,
    file_count: usize,
    skill_bytes: u64,
    total_bytes: &'a mut u64,
    resources: Vec<SkillFileEntry>,
    files_by_uri: &'a mut HashMap<String, SkillFileEntry>,
    dirs_by_uri: &'a mut HashMap<String, Vec<SkillDirChild>>,
    frontmatter: Option<ParsedFrontmatter>,
}

fn scan_skill(
    skill_dir: &DirFd,
    mounted_name: &str,
    root_prefix: &[String],
    total_bytes: &mut u64,
    files_by_uri: &mut HashMap<String, SkillFileEntry>,
    dirs_by_uri: &mut HashMap<String, Vec<SkillDirChild>>,
) -> Result<SkillEntry, String> {
    let root_uri = format!("skill://{mounted_name}");
    let mut state = SkillScanState {
        skill_name: mounted_name,
        root_uri: &root_uri,
        file_count: 0,
        skill_bytes: 0,
        total_bytes,
        resources: Vec::new(),
        files_by_uri,
        dirs_by_uri,
        frontmatter: None,
    };

    let mut rel_segments = Vec::new();
    let mut root_segments = root_prefix.to_vec();
    walk_skill_dir(skill_dir, &mut rel_segments, &mut root_segments, &mut state)?;

    let frontmatter = state
        .frontmatter
        .ok_or_else(|| format!("skill {mounted_name:?} is missing SKILL.md"))?;
    let mut resources = state.resources;
    resources.sort_by(|a, b| a.uri.cmp(&b.uri));
    Ok(SkillEntry {
        uri: format!("{root_uri}/SKILL.md"),
        root_uri,
        name: frontmatter.name,
        description: frontmatter.description,
        frontmatter_extra: frontmatter.extra,
        resources,
    })
}

fn walk_skill_dir(
    dir: &DirFd,
    rel_segments: &mut Vec<String>,
    root_segments: &mut Vec<String>,
    state: &mut SkillScanState<'_>,
) -> Result<(), String> {
    let dir_label = if rel_segments.is_empty() {
        state.skill_name.to_owned()
    } else {
        format!("{}/{}", state.skill_name, rel_segments.join("/"))
    };
    let entries = read_checked_entries(dir, &dir_label)?;

    let mut children = Vec::with_capacity(entries.len());
    for entry in entries {
        validate_segment(&entry.name)?;
        let next_depth = rel_segments.len() + 1;
        if next_depth > MAX_SKILL_DEPTH {
            return Err(format!(
                "skill {:?} exceeds maximum directory depth {MAX_SKILL_DEPTH} at {dir_label}/{}",
                state.skill_name, entry.name
            ));
        }

        match entry.kind {
            EntryKind::Link => {
                return Err(format!(
                    "skill {:?} contains forbidden symlink or reparse point {dir_label}/{}",
                    state.skill_name, entry.name
                ));
            }
            EntryKind::Other => {
                return Err(format!(
                    "skill {:?} contains non-regular file {dir_label}/{}",
                    state.skill_name, entry.name
                ));
            }
            EntryKind::Directory => {
                let sub = dir.open_subdir(&entry.name).map_err(|err| {
                    format!(
                        "cannot open directory {dir_label}/{} in skill {:?}: {err}",
                        entry.name, state.skill_name
                    )
                })?;
                rel_segments.push(entry.name.clone());
                root_segments.push(entry.name.clone());
                let child_uri = format!("{}/{}", state.root_uri, rel_segments.join("/"));
                walk_skill_dir(&sub, rel_segments, root_segments, state)?;
                root_segments.pop();
                rel_segments.pop();
                children.push(SkillDirChild {
                    uri: child_uri,
                    name: entry.name,
                    mime_type: DIRECTORY_MIME_TYPE,
                    size: None,
                    is_dir: true,
                });
            }
            EntryKind::File => {
                state.file_count += 1;
                if state.file_count > MAX_FILES_PER_SKILL {
                    return Err(format!(
                        "skill {:?} exceeds maximum of {MAX_FILES_PER_SKILL} files",
                        state.skill_name
                    ));
                }

                let is_skill_md = rel_segments.is_empty() && entry.name == "SKILL.md";
                let (file, stat_size) = dir.open_file_segment(&entry.name).map_err(|err| {
                    format!(
                        "cannot open file {dir_label}/{} in skill {:?}: {err}",
                        entry.name, state.skill_name
                    )
                })?;
                let file_cap = if is_skill_md {
                    MAX_SKILL_MD_BYTES as u64
                } else {
                    MAX_SKILL_BYTES as u64
                };
                if stat_size > file_cap {
                    return Err(format!(
                        "file {dir_label}/{} in skill {:?} exceeds {file_cap} bytes",
                        entry.name, state.skill_name
                    ));
                }
                let mut raw = Vec::new();
                file.take(file_cap + 1)
                    .read_to_end(&mut raw)
                    .map_err(|err| {
                        format!(
                            "cannot read file {dir_label}/{} in skill {:?}: {err}",
                            entry.name, state.skill_name
                        )
                    })?;
                let actual_size = raw.len() as u64;
                if actual_size > file_cap {
                    return Err(format!(
                        "file {dir_label}/{} in skill {:?} exceeds {file_cap} bytes",
                        entry.name, state.skill_name
                    ));
                }
                if actual_size != stat_size {
                    return Err(format!(
                        "file {dir_label}/{} in skill {:?} changed size while scanning",
                        entry.name, state.skill_name
                    ));
                }

                state.skill_bytes = state.skill_bytes.saturating_add(actual_size);
                if state.skill_bytes > MAX_SKILL_BYTES as u64 {
                    return Err(format!(
                        "skill {:?} exceeds maximum total size of {MAX_SKILL_BYTES} bytes",
                        state.skill_name
                    ));
                }
                *state.total_bytes = state.total_bytes.saturating_add(actual_size);
                if *state.total_bytes > MAX_TOTAL_SKILLS_BYTES as u64 {
                    return Err(format!(
                        "skills catalog exceeds maximum total size of {MAX_TOTAL_SKILLS_BYTES} bytes"
                    ));
                }

                if is_skill_md {
                    let parsed = parse_skill_md_frontmatter(&raw, state.skill_name)?;
                    state.frontmatter = Some(parsed);
                }

                rel_segments.push(entry.name.clone());
                root_segments.push(entry.name.clone());
                let rel_path = rel_segments.join("/");
                let uri = format!("{}/{rel_path}", state.root_uri);
                let digest = sha256_digest(&raw);
                let (mime_type, is_text) = detect_file_mime(&raw, &rel_path);
                let file_entry = SkillFileEntry {
                    uri: uri.clone(),
                    rel_path,
                    name: entry.name.clone(),
                    size: actual_size,
                    digest,
                    mime_type,
                    is_text,
                    segments: root_segments.clone(),
                };
                state.resources.push(file_entry.clone());
                state.files_by_uri.insert(uri.clone(), file_entry);
                root_segments.pop();
                rel_segments.pop();

                children.push(SkillDirChild {
                    uri,
                    name: entry.name,
                    mime_type,
                    size: Some(actual_size),
                    is_dir: false,
                });
            }
        }
    }

    children.sort_by(|a, b| a.name.cmp(&b.name));
    let dir_uri = if rel_segments.is_empty() {
        state.root_uri.to_owned()
    } else {
        format!("{}/{}", state.root_uri, rel_segments.join("/"))
    };
    state.dirs_by_uri.insert(dir_uri, children);
    Ok(())
}

struct CheckedEntry {
    name: String,
    kind: EntryKind,
}

fn read_checked_entries(dir: &DirFd, label: &str) -> Result<Vec<CheckedEntry>, String> {
    let raw = dir
        .read_entries(DIR_ENUM_LIMIT)
        .map_err(|err| format!("cannot enumerate {label}: {err}"))?;
    let mut entries = Vec::with_capacity(raw.len());
    let mut seen_lower = BTreeSet::new();
    for DirEntry { name, kind } in raw {
        let name = name
            .into_string()
            .map_err(|bad| format!("non-UTF-8 entry {bad:?} in {label}"))?;
        let folded = name.to_ascii_lowercase();
        if !seen_lower.insert(folded) {
            return Err(format!(
                "case-colliding duplicate entry {name:?} in {label}"
            ));
        }
        entries.push(CheckedEntry { name, kind });
    }
    entries.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(entries)
}

// shortcut: 技能内部相对路径段仅允许 ASCII [A-Za-z0-9._-] 字符集并强制同目录大小写唯一, 未来若 SEP-2640 明确非 ASCII 资源路径的规范化编码规则再按 RFC 3986 扩展
fn validate_segment(segment: &str) -> Result<(), String> {
    if segment.is_empty() || segment == "." || segment == ".." {
        return Err(format!("invalid skill path segment {segment:?}"));
    }
    if segment.ends_with('.') || segment.ends_with(' ') {
        return Err(format!(
            "skill path segment {segment:?} must not end with a dot or space"
        ));
    }
    if !segment
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
    {
        return Err(format!(
            "skill path segment {segment:?} contains disallowed characters"
        ));
    }
    if is_windows_reserved_device_name(segment) {
        return Err(format!(
            "skill path segment {segment:?} uses a Windows reserved device name"
        ));
    }
    Ok(())
}

struct ParsedFrontmatter {
    name: String,
    description: String,
    extra: Map<String, Value>,
}

fn yaml_frontmatter_options() -> serde_saphyr::Options {
    serde_saphyr::options! {
        strict_booleans: true,
        ignore_binary_tag_for_string: true,
        reject_unsupported_tags: true,
        merge_keys: serde_saphyr::MergeKeyPolicy::Error,
        with_snippet: false,
        budget: serde_saphyr::budget! {
            max_documents: 1,
            max_depth: 16,
            flow_nesting_limit: 16,
            max_nodes: 1024,
            max_events: 4096,
            max_aliases: 0,
            max_anchors: 0,
            max_recorded_anchor_events: 0,
            max_recorded_anchor_bytes: 0,
            max_total_scalar_bytes: 64 * 1024,
        },
        alias_limits: serde_saphyr::alias_limits! {
            max_total_replayed_events: 0,
            max_replay_stack_depth: 0,
            max_alias_expansions_per_anchor: 0,
        },
    }
}

fn parse_skill_md_frontmatter(raw: &[u8], mounted_name: &str) -> Result<ParsedFrontmatter, String> {
    if raw.len() > MAX_SKILL_MD_BYTES {
        return Err(format!(
            "SKILL.md for {mounted_name:?} exceeds {MAX_SKILL_MD_BYTES} bytes"
        ));
    }
    if raw.starts_with(&[0xEF, 0xBB, 0xBF]) {
        return Err(format!(
            "SKILL.md for {mounted_name:?} must not start with a UTF-8 BOM"
        ));
    }
    if raw.contains(&0) {
        return Err(format!(
            "SKILL.md for {mounted_name:?} must not contain NUL bytes"
        ));
    }
    let text = std::str::from_utf8(raw)
        .map_err(|_| format!("SKILL.md for {mounted_name:?} is not valid UTF-8"))?;
    let yaml = extract_frontmatter_yaml(text)
        .ok_or_else(|| format!("SKILL.md for {mounted_name:?} lacks valid YAML frontmatter"))?;

    let value: Value = serde_saphyr::from_str_with_options(yaml, yaml_frontmatter_options())
        .map_err(|err| format!("invalid YAML frontmatter in {mounted_name:?}: {err}"))?;
    let Value::Object(mut map) = value else {
        return Err(format!(
            "SKILL.md frontmatter in {mounted_name:?} must be a non-empty YAML mapping"
        ));
    };

    let name = match map.remove("name") {
        Some(Value::String(name)) => name,
        _ => {
            return Err(format!(
                "SKILL.md frontmatter in {mounted_name:?} must define string field `name`"
            ));
        }
    };
    validate_skill_name(&name)?;
    if name != mounted_name {
        return Err(format!(
            "SKILL.md frontmatter name {name:?} does not match directory name {mounted_name:?}"
        ));
    }

    let description = match map.remove("description") {
        Some(Value::String(desc)) => desc,
        _ => {
            return Err(format!(
                "SKILL.md frontmatter in {mounted_name:?} must define string field `description`"
            ));
        }
    };
    if description.trim().is_empty() || description.chars().count() > 1024 {
        return Err(format!(
            "SKILL.md frontmatter description in {mounted_name:?} must be non-empty and <= 1024 chars"
        ));
    }

    Ok(ParsedFrontmatter {
        name,
        description,
        extra: map,
    })
}

fn extract_frontmatter_yaml(text: &str) -> Option<&str> {
    let mut lines = text.split_inclusive('\n');
    let first = lines.next()?;
    if strip_line_ending(first) != "---" {
        return None;
    }
    let yaml_start = first.len();
    let mut offset = yaml_start;
    for line in lines {
        let stripped = strip_line_ending(line);
        if stripped == "---" || stripped == "..." {
            return Some(&text[yaml_start..offset]);
        }
        offset += line.len();
    }
    None
}

fn strip_line_ending(line: &str) -> &str {
    let line = line.strip_suffix('\n').unwrap_or(line);
    line.strip_suffix('\r').unwrap_or(line)
}

fn validate_skill_name(name: &str) -> Result<(), String> {
    if name.is_empty() || name.len() > 64 {
        return Err(format!(
            "skill name {name:?} must be between 1 and 64 bytes"
        ));
    }
    if name.starts_with('-') || name.ends_with('-') || name.contains("--") {
        return Err(format!(
            "skill name {name:?} must not start or end with '-' or contain '--'"
        ));
    }
    if !name
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    {
        return Err(format!("skill name {name:?} may only contain [a-z0-9-]"));
    }
    Ok(())
}

fn detect_file_mime(raw: &[u8], rel_path: &str) -> (&'static str, bool) {
    let ext = Path::new(rel_path)
        .extension()
        .and_then(|e| e.to_str())
        .map(str::to_ascii_lowercase);
    if !raw.contains(&0) && std::str::from_utf8(raw).is_ok() {
        let mime = match ext.as_deref() {
            Some("md") => "text/markdown",
            Some("json") => "application/json",
            Some("yaml" | "yml") => "application/yaml",
            Some("toml") => "application/toml",
            Some("js" | "mjs") => "text/javascript",
            Some("ts") => "text/typescript",
            Some("py") => "text/x-python",
            Some("sh") => "text/x-shellscript",
            Some("txt") => "text/plain",
            _ => "text/plain",
        };
        return (mime, true);
    }

    let prefix = &raw[..raw.len().min(media::SNIFF)];
    if let Some(detected) = media::detect_path(prefix, Some(Path::new(rel_path))) {
        return (detected.mime, false);
    }
    if ext.as_deref() == Some("zip") && prefix.starts_with(b"PK\x03\x04") {
        return ("application/zip", false);
    }
    if ext.as_deref() == Some("wasm") && prefix.starts_with(b"\0asm") {
        return ("application/wasm", false);
    }
    ("application/octet-stream", false)
}

pub fn sha256_digest(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let hash = Sha256::digest(bytes);
    let mut out = String::with_capacity(7 + 64);
    out.push_str("sha256:");
    for byte in hash {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

fn read_verified_file(
    root: &DirFd,
    entry: &SkillFileEntry,
    lease: Lease,
) -> Result<ReadSkillResource, SkillReadError> {
    let seg_refs: Vec<&str> = entry.segments.iter().map(String::as_str).collect();
    let (file, opened_size) = root.open_file(&seg_refs).map_err(|err| {
        SkillReadError::Io(format!(
            "cannot open skill resource {} through anchored DirFd: {err}",
            entry.uri
        ))
    })?;
    if opened_size != entry.size {
        return Err(SkillReadError::Integrity(format!(
            "skill resource {} changed size from {} to {opened_size} bytes",
            entry.uri, entry.size
        )));
    }
    let mut bytes = Vec::with_capacity(usize::try_from(entry.size).unwrap_or(0));
    file.take(entry.size.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|err| {
            SkillReadError::Io(format!("cannot read skill resource {}: {err}", entry.uri))
        })?;
    if bytes.len() as u64 != entry.size {
        return Err(SkillReadError::Integrity(format!(
            "skill resource {} read {} bytes, expected {}",
            entry.uri,
            bytes.len(),
            entry.size
        )));
    }
    let actual_digest = sha256_digest(&bytes);
    if actual_digest != entry.digest {
        return Err(SkillReadError::Integrity(format!(
            "skill resource {} digest mismatch: expected {}, got {actual_digest}",
            entry.uri, entry.digest
        )));
    }
    let content = if entry.is_text {
        let text = String::from_utf8(bytes).map_err(|_| {
            SkillReadError::Integrity(format!(
                "skill resource {} is no longer valid UTF-8",
                entry.uri
            ))
        })?;
        SkillContent::Text(text)
    } else {
        SkillContent::Blob(bytes)
    };
    Ok(ReadSkillResource {
        uri: entry.uri.clone(),
        mime_type: entry.mime_type,
        content,
        lease,
    })
}

fn estimate_metadata_bytes(scanned: &ScannedCatalog) -> usize {
    let mut total = 0usize;
    for skill in &scanned.skills {
        total = total
            .saturating_add(skill.uri.len())
            .saturating_add(skill.root_uri.len())
            .saturating_add(skill.name.len())
            .saturating_add(skill.description.len())
            .saturating_add(crate::lease::json_len(&skill.frontmatter_extra).unwrap_or(0))
            .saturating_add(128);
        for res in &skill.resources {
            let seg_bytes: usize = res.segments.iter().map(String::len).sum();
            total = total
                .saturating_add(res.uri.len())
                .saturating_add(res.rel_path.len())
                .saturating_add(res.name.len())
                .saturating_add(res.digest.len())
                .saturating_add(seg_bytes)
                .saturating_add(128);
        }
    }
    for (dir_uri, children) in &scanned.dirs_by_uri {
        total = total.saturating_add(dir_uri.len()).saturating_add(64);
        for child in children {
            total = total
                .saturating_add(child.uri.len())
                .saturating_add(child.name.len())
                .saturating_add(64);
        }
    }
    total
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(label: &str) -> Self {
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let n = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "fastexec-skills-{label}-{}-{n}",
                std::process::id()
            ));
            let _ = std::fs::remove_dir_all(&path);
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn write_file(path: &Path, content: &[u8]) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(path, content).unwrap();
    }

    fn valid_skill_md(name: &str, desc: &str) -> String {
        format!("---\nname: {name}\ndescription: {desc}\n---\n# {name}\n")
    }

    #[test]
    fn mounts_single_skill_and_multi_skill_parent_and_empty_dir() {
        let pool = BytePool::new(1 << 20);

        // Empty directory -> empty catalog.
        let empty = TempDir::new("empty");
        preflight_check(&empty.0).unwrap();
        let cat = load(&empty.0, &pool).unwrap();
        assert!(cat.skills().is_empty());
        assert_eq!(pool.used(), 0);
        drop(cat);

        // Single-skill root.
        let single_parent = TempDir::new("single");
        let single = single_parent.0.join("alpha-skill");
        write_file(
            &single.join("SKILL.md"),
            valid_skill_md("alpha-skill", "Alpha description").as_bytes(),
        );
        write_file(&single.join("refs/guide.md"), b"# Guide\n");
        preflight_check(&single).unwrap();
        assert_eq!(pool.used(), 0, "preflight_check must not touch BytePool");

        let cat = load(&single, &pool).unwrap();
        assert_eq!(cat.skills().len(), 1);
        assert_eq!(cat.skills()[0].name, "alpha-skill");
        assert_eq!(cat.skills()[0].uri, "skill://alpha-skill/SKILL.md");
        assert!(cat.get_skill("skill://alpha-skill/SKILL.md").is_some());
        assert!(cat.get_file("skill://alpha-skill/refs/guide.md").is_some());
        assert_eq!(cat.read_directory("skill://alpha-skill").unwrap().len(), 2);
        assert_eq!(
            cat.read_permits().available_permits(),
            MAX_SKILL_READ_WORKERS
        );
        assert!(cat.metadata_lease_bytes() > 0);
        assert!(pool.used() > 0);
        drop(cat);
        assert_eq!(pool.used(), 0);

        // Multi-skill parent with skipped .git directory.
        let multi = TempDir::new("multi");
        write_file(
            &multi.0.join("beta/SKILL.md"),
            valid_skill_md("beta", "Beta skill").as_bytes(),
        );
        write_file(
            &multi.0.join("alpha/SKILL.md"),
            valid_skill_md("alpha", "Alpha skill").as_bytes(),
        );
        write_file(&multi.0.join(".git/HEAD"), b"ref: refs/heads/main\n");
        let cat = load(&multi.0, &pool).unwrap();
        let names: Vec<&str> = cat.skills().iter().map(|s| s.name.as_str()).collect();
        assert_eq!(names, vec!["alpha", "beta"]);
    }

    #[cfg(unix)]
    #[test]
    fn allows_ancestor_symlink_but_rejects_leaf_and_child_symlinks() {
        use std::os::unix::fs::symlink;

        let base = TempDir::new("symlinks");
        let real_parent = base.0.join("real");
        let skill_dir = real_parent.join("demo-skill");
        write_file(
            &skill_dir.join("SKILL.md"),
            valid_skill_md("demo-skill", "Demo").as_bytes(),
        );

        // Ancestor symlink is allowed (like /var -> /private/var).
        let ancestor_link = base.0.join("via-ancestor");
        symlink(&real_parent, &ancestor_link).unwrap();
        let via_ancestor_skill = ancestor_link.join("demo-skill");
        preflight_check(&via_ancestor_skill).unwrap();

        // Final-component symlink root is rejected (with or without a trailing slash).
        let leaf_link = base.0.join("leaf-link");
        symlink(&skill_dir, &leaf_link).unwrap();
        let err = preflight_check(&leaf_link).unwrap_err();
        assert!(err.contains("symlink"), "{err}");

        let leaf_link_trailing = PathBuf::from(format!("{}/", leaf_link.display()));
        let err = preflight_check(&leaf_link_trailing).unwrap_err();
        assert!(err.contains("symlink"), "{err}");
        let pool = BytePool::new(1 << 20);
        let err = load(&leaf_link_trailing, &pool).unwrap_err();
        assert!(err.contains("symlink"), "{err}");

        // Internal symlink inside skill is rejected.
        let outside = base.0.join("secret.txt");
        write_file(&outside, b"secret");
        symlink(&outside, skill_dir.join("leak.txt")).unwrap();
        let err = preflight_check(&skill_dir).unwrap_err();
        assert!(err.contains("symlink"), "{err}");

        // Multi-skill parent containing a symlink child is rejected.
        let multi = TempDir::new("multi-symlink");
        symlink(&skill_dir, multi.0.join("linked-skill")).unwrap();
        let err = preflight_check(&multi.0).unwrap_err();
        assert!(err.contains("symlink"), "{err}");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn runtime_toctou_subdir_replaced_with_symlink_is_rejected() {
        use std::os::unix::fs::symlink;

        let base = TempDir::new("toctou");
        let skill = base.0.join("toctou-skill");
        write_file(
            &skill.join("SKILL.md"),
            valid_skill_md("toctou-skill", "TOCTOU test").as_bytes(),
        );
        write_file(&skill.join("refs/note.txt"), b"original payload");

        let pool = BytePool::new(1 << 20);
        let cat = load(&skill, &pool).unwrap();
        let cancel = CancellationToken::new();

        // Baseline read succeeds.
        let read = cat
            .read_resource("skill://toctou-skill/refs/note.txt", &pool, &cancel)
            .await
            .unwrap();
        assert!(matches!(read.content, SkillContent::Text(ref s) if s == "original payload"));
        drop(read);

        // Swap refs/ with a symlink pointing to a decoy directory holding identical bytes.
        let decoy = base.0.join("decoy");
        write_file(&decoy.join("note.txt"), b"original payload");
        std::fs::remove_dir_all(skill.join("refs")).unwrap();
        symlink(&decoy, skill.join("refs")).unwrap();

        let err = cat
            .read_resource("skill://toctou-skill/refs/note.txt", &pool, &cancel)
            .await
            .unwrap_err();
        assert!(
            matches!(err, SkillReadError::Io(_)),
            "expected DirFd openat(O_NOFOLLOW) failure, got {err:?}"
        );
    }

    #[test]
    fn rejects_case_colliding_duplicates_reserved_names_and_trailing_dot_or_space() {
        // Case-folding duplicate in same directory.
        let dup = TempDir::new("case-dup");
        let skill = dup.0.join("case-skill");
        write_file(
            &skill.join("SKILL.md"),
            valid_skill_md("case-skill", "Case test").as_bytes(),
        );
        write_file(&skill.join("refs/Guide.md"), b"one");
        write_file(&skill.join("refs/guide.md"), b"two");
        // On case-insensitive filesystems both writes hit the same file; only assert when 2 exist.
        if std::fs::read_dir(skill.join("refs")).unwrap().count() == 2 {
            let err = preflight_check(&skill).unwrap_err();
            assert!(err.contains("case-colliding"), "{err}");
        }

        // Windows reserved device names rejected cross-platform.
        for bad in ["aux.py", "CON", "COM1", "LPT9.txt", "nul.md", "PRN"] {
            let err = validate_segment(bad).unwrap_err();
            assert!(err.contains("reserved device name"), "{bad}: {err}");
        }

        // Trailing dot or space rejected cross-platform.
        for bad in ["file.", "file ", "a.."] {
            let err = validate_segment(bad).unwrap_err();
            assert!(err.contains("dot or space"), "{bad}: {err}");
        }

        // Reserved device name inside skill directory fails preflight_check.
        let res = TempDir::new("reserved");
        let rskill = res.0.join("res-skill");
        write_file(
            &rskill.join("SKILL.md"),
            valid_skill_md("res-skill", "Reserved test").as_bytes(),
        );
        write_file(&rskill.join("aux.py"), b"print(1)\n");
        let err = preflight_check(&rskill).unwrap_err();
        assert!(err.contains("reserved device name"), "{err}");
    }

    #[test]
    fn sniffs_media_containers_zip_wasm_and_text_mimes() {
        let base = TempDir::new("mime");
        let skill = base.0.join("mime-skill");
        write_file(
            &skill.join("SKILL.md"),
            valid_skill_md("mime-skill", "MIME test").as_bytes(),
        );
        // Minimal PNG header + IHDR bytes (contains non-UTF-8 0x89 and NUL).
        let png = [
            0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00, 0x00, 0x0d, b'I', b'H',
            b'D', b'R',
        ];
        write_file(&skill.join("icon.png"), &png);
        // Binary PDF header (standard PDF binary marker comment).
        write_file(&skill.join("doc.pdf"), b"%PDF-1.7\n%\xe2\xe3\xcf\xd3\n");
        // ZIP magic with NUL byte.
        write_file(&skill.join("bundle.zip"), b"PK\x03\x04\x14\x00\x00\x00");
        // WASM magic (\0asm + version 1).
        write_file(&skill.join("mod.wasm"), b"\x00asm\x01\x00\x00\x00");
        // Unknown binary.
        write_file(&skill.join("raw.bin"), b"\x00\xff\xfe\xfd");
        // Text files.
        write_file(&skill.join("data.json"), b"{\"ok\":true}\n");
        write_file(&skill.join("notes.custom"), b"plain text\n");

        let pool = BytePool::new(1 << 20);
        let cat = load(&skill, &pool).unwrap();
        assert_eq!(
            cat.get_file("skill://mime-skill/SKILL.md")
                .unwrap()
                .mime_type,
            "text/markdown"
        );
        assert_eq!(
            cat.get_file("skill://mime-skill/icon.png")
                .unwrap()
                .mime_type,
            "image/png"
        );
        assert_eq!(
            cat.get_file("skill://mime-skill/doc.pdf")
                .unwrap()
                .mime_type,
            "application/pdf"
        );
        assert_eq!(
            cat.get_file("skill://mime-skill/bundle.zip")
                .unwrap()
                .mime_type,
            "application/zip"
        );
        assert_eq!(
            cat.get_file("skill://mime-skill/mod.wasm")
                .unwrap()
                .mime_type,
            "application/wasm"
        );
        assert_eq!(
            cat.get_file("skill://mime-skill/raw.bin")
                .unwrap()
                .mime_type,
            "application/octet-stream"
        );
        assert_eq!(
            cat.get_file("skill://mime-skill/data.json")
                .unwrap()
                .mime_type,
            "application/json"
        );
        assert_eq!(
            cat.get_file("skill://mime-skill/notes.custom")
                .unwrap()
                .mime_type,
            "text/plain"
        );
    }

    #[test]
    fn rejects_bom_nul_empty_header_anchors_aliases_merge_keys_and_duplicate_keys() {
        let cases: &[(&str, &[u8])] = &[
            (
                "bom",
                b"\xef\xbb\xbf---\nname: test-skill\ndescription: hi\n---\nbody\n",
            ),
            (
                "nul",
                b"---\nname: test-skill\ndescription: hi\x00there\n---\nbody\n",
            ),
            ("empty-header", b"---\n---\nbody\n"),
            (
                "anchor-alias",
                b"---\nname: &a test-skill\ndescription: *a\n---\nbody\n",
            ),
            (
                "merge-key",
                b"---\nbase: { a: 1 }\n<<: { b: 2 }\nname: test-skill\ndescription: hi\n---\n",
            ),
            (
                "duplicate-key",
                b"---\nname: test-skill\nname: test-skill\ndescription: hi\n---\n",
            ),
            (
                "non-finite-float",
                b"---\nname: test-skill\ndescription: hi\nscore: .nan\n---\n",
            ),
            (
                "non-string-key",
                b"---\nname: test-skill\ndescription: hi\n[a, b]: value\n---\n",
            ),
        ];

        for (label, raw) in cases {
            assert!(
                parse_skill_md_frontmatter(raw, "test-skill").is_err(),
                "expected case {label} to be rejected"
            );
        }
    }

    #[tokio::test]
    async fn supports_dashes_and_dots_closing_delimiters_and_preserves_crlf_sha256() {
        let base = TempDir::new("crlf");
        let skill_a = base.0.join("dash-skill");
        let crlf_dash = b"---\r\nname: dash-skill\r\ndescription: CRLF dash\r\nextra_num: 42\r\n---\r\nLine 1\r\nLine 2\r\n";
        write_file(&skill_a.join("SKILL.md"), crlf_dash);

        let skill_b = base.0.join("dots-skill");
        let crlf_dots = b"---\r\nname: dots-skill\r\ndescription: CRLF dots\r\n...\r\nBody\r\n";
        write_file(&skill_b.join("SKILL.md"), crlf_dots);

        let pool = BytePool::new(1 << 20);
        let cat = load(&base.0, &pool).unwrap();
        let entry_a = cat.get_file("skill://dash-skill/SKILL.md").unwrap();
        assert_eq!(entry_a.size, crlf_dash.len() as u64);
        assert_eq!(entry_a.digest, sha256_digest(crlf_dash));
        assert_eq!(
            cat.get_skill("skill://dash-skill/SKILL.md")
                .unwrap()
                .frontmatter_extra
                .get("extra_num"),
            Some(&Value::from(42))
        );

        let entry_b = cat.get_file("skill://dots-skill/SKILL.md").unwrap();
        assert_eq!(entry_b.size, crlf_dots.len() as u64);
        assert_eq!(entry_b.digest, sha256_digest(crlf_dots));

        let cancel = CancellationToken::new();
        let read_a = cat
            .read_resource("skill://dash-skill/SKILL.md", &pool, &cancel)
            .await
            .unwrap();
        assert_eq!(read_a.uri, "skill://dash-skill/SKILL.md");
        assert_eq!(read_a.mime_type, "text/markdown");
        assert_eq!(read_a.lease.bytes(), crlf_dash.len());
        match read_a.content {
            SkillContent::Text(text) => assert_eq!(text.as_bytes(), crlf_dash),
            SkillContent::Blob(_) => panic!("expected text content"),
        }

        // Also verify binary blob read and integrity mismatch detection.
        let bin_dir = TempDir::new("blob-read");
        let bin_skill = bin_dir.0.join("bin-skill");
        write_file(
            &bin_skill.join("SKILL.md"),
            valid_skill_md("bin-skill", "Binary read test").as_bytes(),
        );
        let wasm = b"\x00asm\x01\x00\x00\x00";
        write_file(&bin_skill.join("mod.wasm"), wasm);
        let bin_cat = load(&bin_skill, &pool).unwrap();
        let read_bin = bin_cat
            .read_resource("skill://bin-skill/mod.wasm", &pool, &cancel)
            .await
            .unwrap();
        assert_eq!(read_bin.mime_type, "application/wasm");
        match read_bin.content {
            SkillContent::Blob(ref bytes) => assert_eq!(bytes, wasm),
            SkillContent::Text(_) => panic!("expected blob content"),
        }
        drop(read_bin);

        // Mutating file contents after load triggers digest integrity failure.
        write_file(&bin_skill.join("mod.wasm"), b"\x00asm\x02\x00\x00\x00");
        let err = bin_cat
            .read_resource("skill://bin-skill/mod.wasm", &pool, &cancel)
            .await
            .unwrap_err();
        assert!(matches!(err, SkillReadError::Integrity(_)), "{err:?}");
    }
}
