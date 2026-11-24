use super::path::{
    canonical_target, display_path, open_regular, reserve_target_workspace, resolve,
    with_path_workspace,
};
use super::read::TEXT_BLOCK;
use super::write::save_target;
#[cfg(windows)]
use super::write::windows_file_identity;
use super::{EditRequest, FileError, lock};
use crate::lease::{BytePool, Lease};
use crate::session::LaunchContext;
use serde::Serialize;
use std::fmt;
use std::fs::File;
use std::io::{self, Read, Seek};
use std::path::PathBuf;
use std::sync::Arc;
use unicode_normalization::UnicodeNormalization;
use unicode_normalization::char::canonical_combining_class;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EditMatch {
    pub line: usize,
    pub fuzzy: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileEditData {
    pub path: String,
    pub replacements: usize,
    pub edits: Vec<EditMatch>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EditProblem {
    Ambiguous,
    NotFound,
    Overlap,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EditFailure {
    pub index: usize,
    pub problem: EditProblem,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lines: Option<Vec<usize>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub with: Option<Vec<usize>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub more: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EditError {
    pub error: FileError,
    pub path: Option<String>,
    pub failures: Option<Vec<EditFailure>>,
}

impl EditError {
    pub fn message(&self) -> &str {
        self.error.message()
    }

    pub fn into_message(self) -> String {
        self.error.into_message()
    }
}

impl fmt::Display for EditError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message())
    }
}

impl From<FileError> for EditError {
    fn from(error: FileError) -> Self {
        Self {
            error,
            path: None,
            failures: None,
        }
    }
}

/// Applies targeted replacements to one file, returning pure domain results without MCP or JSON wrappers.
pub async fn edit_core(
    request: EditRequest,
    launch: Arc<LaunchContext>,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
    input_owner: crate::lease::Owner,
) -> crate::ipc::Owned<Result<FileEditData, EditError>> {
    let input = crate::ipc::Owned::new((request, permit), input_owner);
    match tokio::task::spawn_blocking(move || {
        input.flat_map(|(request, _permit)| edit_blocking(&request, &launch))
    })
    .await
    {
        Ok(outcome) => outcome,
        Err(error) => crate::ipc::Owned::new(
            Err(FileError::Io(format!("the file operation did not finish: {error}")).into()),
            Default::default(),
        ),
    }
}

fn edit_sum(parts: &[usize]) -> Option<usize> {
    parts
        .iter()
        .try_fold(0usize, |sum, &part| sum.checked_add(part))
}

/// Four logical lengths cover geometric capacity growth and the old allocation
/// while its replacement is live. The floor covers small Vec/String capacities.
pub(super) fn edit_growth(items: usize, item_bytes: usize) -> Option<usize> {
    items
        .checked_add(8)?
        .checked_mul(item_bytes)?
        .checked_mul(4)
}

fn edit_memory_error(limit: usize) -> EditError {
    FileError::Overload(format!(
        "Server active memory budget ({limit} bytes) is full while editing the file; nothing was changed."
    ))
    .into()
}

pub(super) struct EditWorkspace<'a> {
    lease: &'a mut Lease,
    retained: usize,
    scratch: usize,
    limit: usize,
}

impl<'a> EditWorkspace<'a> {
    pub(super) fn new(lease: &'a mut Lease, limit: usize) -> Self {
        let retained = lease.bytes();
        Self {
            lease,
            retained,
            scratch: 0,
            limit,
        }
    }

    fn retain(&mut self, bytes: Option<usize>) -> Result<(), EditError> {
        let retained = bytes
            .and_then(|bytes| self.retained.checked_add(bytes))
            .ok_or_else(|| edit_memory_error(self.limit))?;
        let total = retained
            .checked_add(self.scratch)
            .ok_or_else(|| edit_memory_error(self.limit))?;
        if !self.lease.try_resize(total) {
            return Err(edit_memory_error(self.limit));
        }
        self.retained = retained;
        Ok(())
    }

    fn temporary(&mut self, bytes: Option<usize>) -> Result<(), EditError> {
        let scratch = bytes
            .ok_or_else(|| edit_memory_error(self.limit))?
            .max(self.scratch);
        let total = self
            .retained
            .checked_add(scratch)
            .ok_or_else(|| edit_memory_error(self.limit))?;
        if !self.lease.try_resize(total) {
            return Err(edit_memory_error(self.limit));
        }
        self.scratch = scratch;
        Ok(())
    }

    fn diagnostics(&mut self, edits: usize, path: &str, failed: bool) -> Result<(), EditError> {
        // Each failure shows at most 20 line/entry numbers. This covers its
        // bullet, joined message, JSON copy and temporary number/label strings.
        let per_edit = if failed { 16 * 1024 } else { 512 };
        self.retain(edits.checked_mul(per_edit).and_then(|bytes| {
            edit_sum(&[
                bytes,
                path.len().checked_mul(16)?,
                4096,
                if failed { 0 } else { TEXT_BLOCK },
            ])
        }))
    }
}

/// Count the dependency's fully decomposed characters without constructing text.
/// NFKC composition cannot increase that character count; UTF-8 needs at most 4
/// bytes per character. ASCII stays unchanged.
#[derive(Clone, Copy)]
pub(super) struct EditNormalization {
    chars: usize,
    pub(super) decomposed: usize,
    pub(super) bytes: usize,
    lines: usize,
    segment: usize,
}

impl EditNormalization {
    pub(super) fn count(text: &str) -> Option<Self> {
        let mut chars = Some(0usize);
        let mut decomposed = Some(0usize);
        let mut lines = Some(1usize);
        let mut run = Some(0usize);
        let mut longest = 0usize;
        let mut burst = 0usize;
        for ch in text.chars() {
            chars = chars.and_then(|n| n.checked_add(1));
            let mut parts = Some(0usize);
            unicode_normalization::char::decompose_compatible(ch, |part| {
                decomposed = decomposed.and_then(|n| n.checked_add(1));
                parts = parts.and_then(|n| n.checked_add(1));
                if matches!(part, '\r' | '\n') {
                    lines = lines.and_then(|n| n.checked_add(1));
                }
                run = if canonical_combining_class(part) == 0 {
                    Some(1)
                } else {
                    run.and_then(|n| n.checked_add(1))
                };
                if let Some(run) = run {
                    longest = longest.max(run);
                }
            });
            burst = burst.max(parts?);
        }
        let decomposed = decomposed?;
        Some(Self {
            chars: chars?,
            decomposed,
            bytes: if text.is_ascii() {
                text.len()
            } else {
                decomposed.checked_mul(4)?
            },
            lines: lines?,
            segment: longest.checked_add(burst)?,
        })
    }

    fn normalizer(self) -> Option<usize> {
        // unicode-normalization 0.1.26: decomposition TinyVec<(u8,char)>,
        // recomposition TinyVec<char>, and stable-sort scratch.
        edit_sum(&[
            edit_growth(self.segment, std::mem::size_of::<(u8, char)>())?,
            edit_growth(self.segment, std::mem::size_of::<char>())?,
            self.segment
                .checked_mul(std::mem::size_of::<(u8, char)>())?,
        ])
    }

    fn form(self) -> Option<usize> {
        edit_sum(&[
            self.normalizer()?,
            edit_growth(self.bytes, 1)?.checked_mul(3)?,
            edit_growth(self.lines, std::mem::size_of::<&str>())?,
        ])
    }

    fn mapping(self) -> Option<usize> {
        edit_sum(&[
            edit_growth(self.chars, std::mem::size_of::<(usize, usize)>())?,
            edit_growth(
                self.chars,
                std::mem::size_of::<(usize, usize, usize, usize)>(),
            )?,
            edit_growth(self.bytes, 1)?.checked_mul(2)?,
            edit_growth(self.bytes, std::mem::size_of::<usize>())?,
        ])
    }

    fn tagged(self) -> Option<usize> {
        edit_sum(&[
            self.normalizer()?,
            edit_growth(self.decomposed, std::mem::size_of::<(char, bool)>())?.checked_mul(2)?,
            edit_growth(self.decomposed, std::mem::size_of::<(char, usize)>())?,
            self.decomposed.checked_mul(
                std::mem::size_of::<(char, bool)>() + std::mem::size_of::<(char, usize)>(),
            )?,
            edit_growth(self.bytes, 1)?,
        ])
    }
}

fn prepare_edit_workspace(
    content: &str,
    request: &EditRequest,
    workspace: &mut EditWorkspace<'_>,
) -> Result<EditNormalization, EditError> {
    let size =
        EditNormalization::count(content).ok_or_else(|| edit_memory_error(workspace.limit))?;
    let mut input_bytes = 0usize;
    let mut new_bytes = 0usize;
    let mut needle_bytes = 0usize;
    let mut search_bytes = 0usize;
    let mut scratch = size
        .form()
        .ok_or_else(|| edit_memory_error(workspace.limit))?;
    for edit in &request.edits {
        input_bytes = edit_sum(&[input_bytes, edit.old_text.len(), edit.new_text.len()])
            .ok_or_else(|| edit_memory_error(workspace.limit))?;
        new_bytes = new_bytes
            .checked_add(edit.new_text.len())
            .ok_or_else(|| edit_memory_error(workspace.limit))?;
        let old = EditNormalization::count(&edit.old_text)
            .ok_or_else(|| edit_memory_error(workspace.limit))?;
        needle_bytes = needle_bytes.max(old.bytes);
        search_bytes = search_bytes.max(
            edit.old_text
                .len()
                .checked_add(old.bytes)
                .ok_or_else(|| edit_memory_error(workspace.limit))?,
        );
        scratch = scratch.max(
            old.form()
                .ok_or_else(|| edit_memory_error(workspace.limit))?,
        );
    }
    let crlf = content
        .match_indices("\r\n")
        .try_fold(0usize, |n, _| n.checked_add(1));
    let raw_lines = content
        .bytes()
        .filter(|&ch| ch == b'\n')
        .try_fold(0usize, |n, _| n.checked_add(1));
    let entry_bytes = std::mem::size_of::<(String, String)>()
        + std::mem::size_of::<String>()
        + std::mem::size_of::<Option<EntryProblem>>()
        + std::mem::size_of::<Matched<'_>>()
        + std::mem::size_of::<usize>()
        + std::mem::size_of::<bool>();
    let retained = (|| {
        edit_sum(&[
            edit_growth(content.len(), 1)?.checked_mul(2)?,
            edit_growth(crlf?, std::mem::size_of::<usize>())?,
            edit_growth(raw_lines?, std::mem::size_of::<usize>())?,
            edit_growth(input_bytes, 1)?.checked_mul(2)?,
            edit_growth(new_bytes.checked_mul(2)?, 1)?,
            edit_growth(request.edits.len(), entry_bytes)?,
            edit_growth(
                request.edits.len().checked_mul(MAX_REPORTED_ITEMS)?,
                std::mem::size_of::<usize>(),
            )?,
            edit_growth(size.bytes, 1)?,
            edit_growth(
                size.lines,
                std::mem::size_of::<usize>() + std::mem::size_of::<bool>(),
            )?,
            edit_growth(needle_bytes, 1)?,
        ])
    })();
    workspace.retain(retained)?;
    workspace.temporary((|| {
        edit_sum(&[
            scratch,
            edit_growth(search_bytes, std::mem::size_of::<usize>())?,
            edit_growth(MAX_REPORTED_ITEMS * 8, std::mem::size_of::<usize>())?,
        ])
    })())?;
    Ok(size)
}

fn read_uninterrupted(reader: &mut impl Read, buffer: &mut [u8]) -> io::Result<usize> {
    loop {
        match reader.read(buffer) {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            result => return result,
        }
    }
}

pub(super) fn verify_edit_content(current: &mut impl Read, content: &str) -> io::Result<()> {
    let mut offset = 0;
    let mut buffer = [0; TEXT_BLOCK];
    loop {
        let read = read_uninterrupted(current, &mut buffer)?;
        if read == 0 {
            return if offset == content.len() {
                Ok(())
            } else {
                Err(EditSource::changed())
            };
        }
        if content.as_bytes().get(offset..offset + read) != Some(&buffer[..read]) {
            return Err(EditSource::changed());
        }
        offset += read;
    }
}

pub(super) fn read_edit_content(
    file: &mut impl Read,
    pool: &Arc<BytePool>,
) -> io::Result<(String, Lease)> {
    let memory = || {
        io::Error::new(
            io::ErrorKind::OutOfMemory,
            edit_memory_error(pool.limit()).into_message(),
        )
    };
    let mut lease = pool.try_acquire(TEXT_BLOCK).ok_or_else(memory)?;
    let mut bytes = Vec::new();
    let mut block = [0u8; TEXT_BLOCK];
    loop {
        let count = read_uninterrupted(file, &mut block)?;
        if count == 0 {
            break;
        }
        let needed = bytes.len().checked_add(count).ok_or_else(memory)?;
        if needed > bytes.capacity() {
            let preferred = bytes
                .capacity()
                .checked_mul(2)
                .ok_or_else(memory)?
                .max(needed);
            let mut capacity = preferred;
            let peak = |capacity| edit_sum(&[TEXT_BLOCK, bytes.capacity(), capacity]);
            if !peak(capacity).is_some_and(|peak| lease.try_resize(peak)) {
                capacity = needed;
                if !peak(capacity).is_some_and(|peak| lease.try_resize(peak)) {
                    return Err(memory());
                }
            }
            let mut replacement = Vec::new();
            replacement
                .try_reserve_exact(capacity)
                .map_err(|_| memory())?;
            if !peak(replacement.capacity()).is_some_and(|peak| lease.try_resize(peak)) {
                return Err(memory());
            }
            replacement.extend_from_slice(&bytes);
            bytes = replacement;
            lease.shrink_to(
                TEXT_BLOCK
                    .checked_add(bytes.capacity())
                    .ok_or_else(memory)?,
            );
        }
        bytes.extend_from_slice(&block[..count]);
    }
    let content = String::from_utf8(bytes)
        .map_err(|_| io::Error::other("the file is not valid UTF-8; edit it with exec"))?;
    Ok((content, lease))
}

pub(super) struct EditSource {
    path: PathBuf,
    pub(super) target: crate::ipc::Owned<PathBuf>,
    file: File,
    pub(super) content: String,
    _content_lease: Lease,
}

impl EditSource {
    pub(super) fn read(path: PathBuf, pool: &Arc<BytePool>) -> io::Result<Self> {
        // A canonicalizer can discard directory requirements that the native open enforces.
        let (mut file, _) = open_regular(&path)?;
        let target = canonical_target(&path, pool)?;
        let (content, mut lease) = read_edit_content(&mut file, pool)?;
        // The reader's fixed block is gone; retain the actual String allocation.
        lease.shrink_to(content.capacity());
        Ok(Self {
            path,
            target,
            file,
            content,
            _content_lease: lease,
        })
    }

    pub(super) fn changed() -> io::Error {
        io::Error::other("the file changed after it was read; read it again and retry the edit")
    }

    pub(super) fn verify_target(
        &self,
        current: Option<&File>,
        pool: &Arc<BytePool>,
    ) -> io::Result<()> {
        let current = current.ok_or_else(Self::changed)?;
        let (named, _) = open_regular(&self.path)?;
        if canonical_target(&self.path, pool)?.value != self.target.value {
            return Err(Self::changed());
        }
        #[cfg(unix)]
        let identity = |file: &File| {
            use std::os::unix::fs::MetadataExt;
            file.metadata()
                .map(|metadata| (metadata.dev(), metadata.ino()))
        };
        #[cfg(windows)]
        let identity = windows_file_identity;
        let original = identity(&self.file)?;
        if original != identity(current)? || original != identity(&named)? {
            return Err(Self::changed());
        }
        Ok(())
    }

    pub(super) fn verify_content(&self, current: &mut File) -> io::Result<()> {
        current.rewind()?;
        verify_edit_content(current, &self.content)
    }

    pub(super) fn save(&self, bytes: &[u8], pool: &Arc<BytePool>) -> io::Result<()> {
        save_target(&self.target.value, bytes, Some(self), pool)
    }
}

/// One replacement matched against the content it is applied to.
struct Matched<'a> {
    /// The edit's position in the request, which names it in errors and in the result.
    index: usize,
    /// Where it matched in the content, and how long the matched text there is.
    at: usize,
    len: usize,
    /// Whether only the fuzzy form matched.
    fuzzy: bool,
    new_text: &'a str,
}

const MAX_REPORTED_ITEMS: usize = 20;

enum EntryProblem {
    Ambiguous { count: usize, lines: Vec<usize> },
    NotFound { lines: Vec<usize>, more: usize },
}

fn edit_retained_bytes(result: &Result<FileEditData, EditError>) -> Option<usize> {
    match result {
        Ok(data) => edit_sum(&[
            data.path.capacity(),
            data.edits
                .capacity()
                .checked_mul(std::mem::size_of::<EditMatch>())?,
        ]),
        Err(err) => {
            let failures_bytes = err.failures.as_ref().map_or(Some(0), |failures| {
                let items = failures
                    .capacity()
                    .checked_mul(std::mem::size_of::<EditFailure>())?;
                let vecs = failures.iter().try_fold(0usize, |sum, item| {
                    let lines = item.lines.as_ref().map_or(Some(0), |v| {
                        v.capacity().checked_mul(std::mem::size_of::<usize>())
                    })?;
                    let with = item.with.as_ref().map_or(Some(0), |v| {
                        v.capacity().checked_mul(std::mem::size_of::<usize>())
                    })?;
                    edit_sum(&[sum, lines, with])
                })?;
                items.checked_add(vecs)
            })?;
            edit_sum(&[
                err.message().len(),
                err.path.as_ref().map_or(0, String::capacity),
                failures_bytes,
            ])
        }
    }
}

pub(crate) fn edit_blocking(
    request: &EditRequest,
    launch: &LaunchContext,
) -> crate::ipc::Owned<Result<FileEditData, EditError>> {
    with_path_workspace(&request.path, launch, 0, |lease| {
        let path_bytes = lease.bytes();
        let result = edit_outcome_reserved(request, launch, lease);
        if let Some(retained) = edit_retained_bytes(&result) {
            debug_assert!(retained <= lease.bytes());
            lease.shrink_to(path_bytes.max(retained));
        }
        result
    })
    .unwrap_or_else(|message| {
        crate::ipc::Owned::new(Err(FileError::Overload(message).into()), Default::default())
    })
}

/// Applies every edit to one file: exact matches first, and the fuzzy form when any edit needs it.
pub(super) fn edit_outcome_reserved(
    request: &EditRequest,
    launch: &LaunchContext,
    lease: &mut Lease,
) -> Result<FileEditData, EditError> {
    if request.edits.is_empty() {
        return Err(FileError::Validation(
            "Edit tool input is invalid. edits must contain at least one replacement.".to_owned(),
        )
        .into());
    }
    for (index, edit) in request.edits.iter().enumerate() {
        if edit.old_text.is_empty() {
            return Err(FileError::Validation(format!(
                "edits[{index}].oldText must not be empty."
            ))
            .into());
        }
        if edit.start_line == Some(0) {
            return Err(FileError::Validation(format!(
                "edits[{index}].startLine must be at least 1."
            ))
            .into());
        }
        if edit.end_line == Some(0) {
            return Err(FileError::Validation(format!(
                "edits[{index}].endLine must be at least 1."
            ))
            .into());
        }
        if let (Some(start), Some(end)) = (edit.start_line, edit.end_line)
            && start > end
        {
            return Err(FileError::Validation(format!(
                "edits[{index}].startLine ({start}) must not exceed endLine ({end})."
            ))
            .into());
        }
    }
    let resolved = resolve(&request.path, launch).map_err(FileError::Validation)?;
    let shown_path = display_path(&resolved);
    let map_io = |error: io::Error| -> EditError {
        let message = format!("Could not edit file: {shown_path}. {error}.");
        match error.kind() {
            io::ErrorKind::NotFound => FileError::NotFound(message).into(),
            io::ErrorKind::OutOfMemory => FileError::Overload(message).into(),
            _ => FileError::Io(message).into(),
        }
    };
    let _guard = lock();
    let source = EditSource::read(resolved, launch.memory()).map_err(&map_io)?;
    reserve_target_workspace(&source.target.value, lease, launch.memory()).map_err(&map_io)?;
    let mut workspace = EditWorkspace::new(lease, launch.memory().limit());
    let (mut written, starts, fuzzy_flags) =
        apply_edits_budgeted(&source.content, request, &shown_path, &mut workspace)?;
    if source.content.starts_with('\u{feff}') && !written.starts_with('\u{feff}') {
        written.insert(0, '\u{feff}');
    }
    if written == source.content {
        return Err(edit_no_change(&shown_path));
    }
    workspace.diagnostics(request.edits.len(), &shown_path, false)?;
    let mut edits = Vec::with_capacity(request.edits.len());
    for (&at, &fuzzy) in starts.iter().zip(&fuzzy_flags) {
        let line = 1 + line_breaks(&source.content[..at]);
        edits.push(EditMatch { line, fuzzy });
    }
    source
        .save(written.as_bytes(), launch.memory())
        .map_err(&map_io)?;
    Ok(FileEditData {
        path: shown_path,
        replacements: request.edits.len(),
        edits,
    })
}

fn edit_no_change(path: &str) -> EditError {
    let error = format!(
        "Cannot edit {path}; nothing was changed: the replacement produced identical content."
    );
    EditError {
        error: FileError::Validation(error),
        path: Some(path.to_owned()),
        failures: Some(Vec::new()),
    }
}

/// Matches in LF-normalized text, collects every entry failure (ambiguous, not_found, overlap),
/// and replaces the corresponding original byte ranges when every entry succeeds.
#[allow(clippy::type_complexity)]
pub(super) fn apply_edits_budgeted(
    content: &str,
    request: &EditRequest,
    path: &str,
    workspace: &mut EditWorkspace<'_>,
) -> Result<(String, Vec<usize>, Vec<bool>), EditError> {
    let size = prepare_edit_workspace(content, request, workspace)?;
    let total = request.edits.len();
    let normalized = LfText::new(content);
    let ending = detect_line_ending(content);
    let raw_newlines: Vec<usize> = content
        .bytes()
        .enumerate()
        .filter_map(|(i, b)| (b == b'\n').then_some(i))
        .collect();
    let line_at_raw = |raw_pos: usize| 1 + raw_newlines.partition_point(|&nl| nl < raw_pos);
    let line_at_norm = |norm_pos: usize| line_at_raw(normalized.original(norm_pos));

    let mut lazy_fuzzy: Option<(String, Vec<usize>, std::cell::OnceCell<Option<FuzzyMap>>)> = None;
    let edits: Vec<(String, String)> = request
        .edits
        .iter()
        .map(|edit| {
            (
                normalize_to_lf(&edit.old_text),
                normalize_to_lf(&edit.new_text),
            )
        })
        .collect();
    let inserted: Vec<String> = edits
        .iter()
        .map(|(_, new)| restore_line_endings(new, ending))
        .collect();

    let mut problems: Vec<Option<EntryProblem>> = (0..total).map(|_| None).collect();
    let mut matched: Vec<Matched<'_>> = Vec::new();
    let mut fuzzy_flags = vec![false; total];

    for (index, (old, new)) in edits.iter().enumerate() {
        let edit_spec = &request.edits[index];
        let min_line = edit_spec.start_line.unwrap_or(1);
        let max_line = edit_spec.end_line.unwrap_or(u64::MAX);
        let in_line_range = |line: usize| (line as u64) >= min_line && (line as u64) <= max_line;

        let exact =
            starts_positions_filtered(&normalized.text, old, |at| in_line_range(line_at_norm(at)));
        if exact.in_count > 1 {
            let lines = exact.in_starts.iter().map(|&at| line_at_norm(at)).collect();
            problems[index] = Some(EntryProblem::Ambiguous {
                count: exact.in_count,
                lines,
            });
            continue;
        }
        let needle = normalize_for_fuzzy_match(old);
        if needle.is_empty() {
            if let Some(&at) = exact.in_starts.first() {
                let (raw_at, raw_end) =
                    (normalized.original(at), normalized.original(at + old.len()));
                let new_text = if normalized.text[at..at + old.len()] == *new {
                    &content[raw_at..raw_end]
                } else {
                    &inserted[index]
                };
                matched.push(Matched {
                    index,
                    at: raw_at,
                    len: raw_end - raw_at,
                    fuzzy: false,
                    new_text,
                });
            } else {
                let out_lines = exact
                    .out_starts
                    .iter()
                    .map(|&at| line_at_norm(at))
                    .collect();
                problems[index] = Some(EntryProblem::NotFound {
                    lines: out_lines,
                    more: exact.out_count.saturating_sub(MAX_REPORTED_ITEMS),
                });
            }
            continue;
        }

        let (haystack, haystack_newlines, map) = lazy_fuzzy.get_or_insert_with(|| {
            let h = normalize_for_fuzzy_match(&normalized.text);
            // Every `\n` in `normalized.text` maps 1-to-1 in order to `h`; keep only those
            // that came from a real `\n` (or `\r\n`) rather than a lone `\r` in `content`.
            let norm_real_flags: Vec<bool> = normalized
                .text
                .bytes()
                .enumerate()
                .filter(|&(_, b)| b == b'\n')
                .map(|(norm_pos, _)| {
                    let raw = normalized.original(norm_pos);
                    content.as_bytes().get(raw) == Some(&b'\n')
                        || content.as_bytes().get(raw..raw + 2) == Some(b"\r\n")
                })
                .collect();
            let nls = h
                .bytes()
                .enumerate()
                .filter(|&(_, b)| b == b'\n')
                .zip(norm_real_flags)
                .filter_map(|((i, _), is_real)| is_real.then_some(i))
                .collect();
            (h, nls, std::cell::OnceCell::new())
        });
        let line_at_fuzzy =
            |fuzzy_pos: usize| 1 + haystack_newlines.partition_point(|&nl| nl < fuzzy_pos);

        let same_form = *haystack == normalized.text && needle == *old;
        let fuzzy = if same_form {
            exact.clone()
        } else {
            starts_positions_filtered(haystack, &needle, |pos| in_line_range(line_at_fuzzy(pos)))
        };

        if let Some(&exact_at) = exact.in_starts.first() {
            if fuzzy.in_count > 0
                && !same_form
                && let Some((_, context)) =
                    fuzzy_boundary_context(&normalized.text, exact_at, old.len())
            {
                workspace.temporary(
                    EditNormalization::count(context).and_then(EditNormalization::tagged),
                )?;
            }
            let separate = fuzzy.in_count > 0
                && !same_form
                && !exact_survives_fuzzy(&normalized.text, exact_at, old.len(), &needle);
            let count = fuzzy.in_count + usize::from(separate);
            if count > 1 {
                let mut lines: Vec<_> = fuzzy
                    .in_starts
                    .iter()
                    .map(|&pos| line_at_fuzzy(pos))
                    .collect();
                if separate {
                    let line = line_at_norm(exact_at);
                    lines.insert(lines.partition_point(|&other| other < line), line);
                    lines.truncate(MAX_REPORTED_ITEMS);
                }
                problems[index] = Some(EntryProblem::Ambiguous { count, lines });
                continue;
            }
            let (raw_at, raw_end) = (
                normalized.original(exact_at),
                normalized.original(exact_at + old.len()),
            );
            let new_text = if normalized.text[exact_at..exact_at + old.len()] == *new {
                &content[raw_at..raw_end]
            } else {
                &inserted[index]
            };
            matched.push(Matched {
                index,
                at: raw_at,
                len: raw_end - raw_at,
                fuzzy: false,
                new_text,
            });
            continue;
        }

        if map.get().is_none()
            && (fuzzy.in_count == 1 || (fuzzy.in_count == 0 && !same_form && fuzzy.out_count != 0))
        {
            workspace.retain(size.mapping())?;
        }

        let not_found_outside = || {
            let mut lines: Vec<_> = exact
                .out_starts
                .iter()
                .map(|&pos| line_at_norm(pos))
                .collect();
            let mut count = exact.out_count;
            if !same_form
                && fuzzy.out_count != 0
                && let Some(m) = map.get_or_init(|| FuzzyMap::new(&normalized.text)).as_ref()
            {
                // Every exact start is editable. Add only whole-unit fuzzy spans with a
                // separate source occurrence. Trimming can move a fuzzy boundary inside the
                // exact span. Both searches advance in source order.
                let mut exact_starts = matching_starts(&normalized.text, old).peekable();
                let mut extra = 0;
                for pos in matching_starts(haystack, &needle) {
                    let line = line_at_fuzzy(pos);
                    if in_line_range(line) {
                        continue;
                    }
                    let Some((start, len)) = m.span(pos, pos + needle.len()) else {
                        continue;
                    };
                    while exact_starts
                        .next_if(|&exact| exact + old.len() < start + len)
                        .is_some()
                    {}
                    if exact_starts.peek().is_some_and(|&exact| exact <= start) {
                        continue;
                    }
                    if extra < MAX_REPORTED_ITEMS {
                        lines.push(line);
                    }
                    extra += 1;
                }
                count += extra;
                lines.sort_unstable();
                lines.truncate(MAX_REPORTED_ITEMS);
            }
            EntryProblem::NotFound {
                lines,
                more: count.saturating_sub(MAX_REPORTED_ITEMS),
            }
        };
        if fuzzy.in_count == 0 {
            problems[index] = Some(not_found_outside());
            continue;
        }
        if fuzzy.in_count > 1 {
            let lines = fuzzy
                .in_starts
                .iter()
                .map(|&pos| line_at_fuzzy(pos))
                .collect();
            problems[index] = Some(EntryProblem::Ambiguous {
                count: fuzzy.in_count,
                lines,
            });
            continue;
        }
        let Some((at, len)) = map
            .get_or_init(|| FuzzyMap::new(&normalized.text))
            .as_ref()
            .and_then(|m| m.span(fuzzy.in_starts[0], fuzzy.in_starts[0] + needle.len()))
        else {
            problems[index] = Some(not_found_outside());
            continue;
        };
        let (raw_at, raw_end) = (normalized.original(at), normalized.original(at + len));
        let new_text = if normalized.text[at..at + len] == *new {
            &content[raw_at..raw_end]
        } else {
            &inserted[index]
        };
        fuzzy_flags[index] = true;
        matched.push(Matched {
            index,
            at: raw_at,
            len: raw_end - raw_at,
            fuzzy: true,
            new_text,
        });
    }

    matched.sort_by_key(|edit| (edit.at, edit.index));
    let mut overlap_entries = 0usize;
    for (index, edit) in matched.iter().enumerate() {
        let end = edit
            .at
            .checked_add(edit.len)
            .ok_or_else(|| edit_memory_error(workspace.limit))?;
        for later in &matched[index + 1..] {
            if later.at >= end {
                break;
            }
            overlap_entries = overlap_entries
                .checked_add(2)
                .ok_or_else(|| edit_memory_error(workspace.limit))?;
        }
    }
    workspace.retain((|| {
        edit_sum(&[
            edit_growth(total, std::mem::size_of::<Vec<usize>>())?,
            edit_growth(
                overlap_entries.checked_add(total.checked_mul(8)?)?,
                std::mem::size_of::<usize>(),
            )?,
        ])
    })())?;
    let mut overlaps: Vec<Vec<usize>> = vec![Vec::new(); total];
    for i in 0..matched.len() {
        let end_i = matched[i].at + matched[i].len;
        for later in &matched[i + 1..] {
            if later.at >= end_i {
                break;
            }
            overlaps[matched[i].index].push(later.index);
            overlaps[later.index].push(matched[i].index);
        }
    }
    for partners in &mut overlaps {
        partners.sort_unstable();
        partners.dedup();
    }

    if problems.iter().any(Option::is_some) || overlaps.iter().any(|p| !p.is_empty()) {
        workspace.diagnostics(total, path, true)?;
        let mut bullets = Vec::new();
        let mut failures = Vec::new();
        for index in 0..total {
            if let Some(problem) = &problems[index] {
                match problem {
                    EntryProblem::Ambiguous { count, lines } => {
                        let shown = &lines[..lines.len().min(MAX_REPORTED_ITEMS)];
                        let more = count.saturating_sub(MAX_REPORTED_ITEMS);
                        let list = shown
                            .iter()
                            .map(usize::to_string)
                            .collect::<Vec<_>>()
                            .join(", ");
                        let extra = if more > 0 {
                            format!(" (and {more} more)")
                        } else {
                            String::new()
                        };
                        bullets.push(format!(
                            "- edits[{index}]: oldText occurs {count} times, starting at lines {list}{extra}; add surrounding text to make it unique."
                        ));
                        failures.push(EditFailure {
                            index,
                            problem: EditProblem::Ambiguous,
                            lines: Some(shown.to_vec()),
                            with: None,
                            more: (more > 0).then_some(more),
                        });
                    }
                    EntryProblem::NotFound { lines, more } => {
                        if lines.is_empty() {
                            bullets.push(format!(
                                "- edits[{index}]: oldText was not found, even ignoring trailing whitespace, Unicode compatibility forms, curly quotes, dashes, and special spaces."
                            ));
                            failures.push(EditFailure {
                                index,
                                problem: EditProblem::NotFound,
                                lines: None,
                                with: None,
                                more: None,
                            });
                        } else {
                            let shown = &lines[..lines.len().min(MAX_REPORTED_ITEMS)];
                            let list = shown
                                .iter()
                                .map(usize::to_string)
                                .collect::<Vec<_>>()
                                .join(", ");
                            let extra = if *more > 0 {
                                format!(" (and {more} more)")
                            } else {
                                String::new()
                            };
                            bullets.push(format!(
                                "- edits[{index}]: oldText was not found in the requested line range (it occurs outside that range, starting at lines {list}{extra})."
                            ));
                            failures.push(EditFailure {
                                index,
                                problem: EditProblem::NotFound,
                                lines: Some(shown.to_vec()),
                                with: None,
                                more: (*more > 0).then_some(*more),
                            });
                        }
                    }
                }
            } else if !overlaps[index].is_empty() {
                let partners = &overlaps[index];
                let shown = &partners[..partners.len().min(MAX_REPORTED_ITEMS)];
                let more = partners.len().saturating_sub(MAX_REPORTED_ITEMS);
                let labels = shown
                    .iter()
                    .map(|partner| format!("edits[{partner}]"))
                    .collect::<Vec<_>>()
                    .join(", ");
                let extra = if more > 0 {
                    format!(" (and {more} more)")
                } else {
                    String::new()
                };
                bullets.push(format!(
                    "- edits[{index}] overlaps {labels}{extra}; merge them into one edit or target disjoint regions."
                ));
                failures.push(EditFailure {
                    index,
                    problem: EditProblem::Overlap,
                    lines: None,
                    with: Some(shown.to_vec()),
                    more: (more > 0).then_some(more),
                });
            }
        }
        let error = format!(
            "Cannot edit {path}; nothing was changed:\n{}",
            bullets.join("\n")
        );
        return Err(EditError {
            error: FileError::Validation(error),
            path: Some(path.to_owned()),
            failures: Some(failures),
        });
    }

    // Reverse application can grow before a later replacement shrinks the file.
    // Sum every positive increment, and leave room for restoring the UTF-8 BOM.
    let largest = content.len().checked_add(3).and_then(|initial| {
        matched.iter().try_fold(initial, |length, edit| {
            length.checked_add(edit.new_text.len().saturating_sub(edit.len))
        })
    });
    workspace.retain(largest.and_then(|bytes| edit_growth(bytes, 1)))?;
    let (replaced, starts) = replace(content, &matched);
    if replaced == content {
        return Err(edit_no_change(path));
    }
    for edit in &matched {
        fuzzy_flags[edit.index] = edit.fuzzy;
    }
    Ok((replaced, starts, fuzzy_flags))
}

#[cfg(test)]
#[allow(clippy::type_complexity)]
pub(super) fn apply_edits(
    content: &str,
    request: &EditRequest,
    path: &str,
) -> Result<(String, Vec<usize>, Vec<bool>), EditError> {
    let pool = crate::lease::process_pool();
    let mut lease = Lease::empty(pool);
    apply_edits_budgeted(
        content,
        request,
        path,
        &mut EditWorkspace::new(&mut lease, pool.limit()),
    )
}

fn fuzzy_boundary_context(text: &str, at: usize, len: usize) -> Option<(usize, &str)> {
    let after = at + len;
    let bytes = text.as_bytes();
    // A following ASCII character cannot combine backwards across a boundary.
    if (at == 0 || bytes[at].is_ascii() || bytes[at - 1] == b'\n')
        && (after == bytes.len() || bytes[after].is_ascii())
    {
        return None;
    }
    // ASCII starts a new normalization unit. Keep a graphic character or newline on the
    // right so the enclosing line's trailing-whitespace rule is also preserved.
    let start = bytes[..=at].iter().rposition(u8::is_ascii).unwrap_or(0);
    let end = bytes[after..]
        .iter()
        .position(|&b| b.is_ascii_graphic() || b == b'\n')
        .map_or(text.len(), |pos| after + pos + 1);
    Some((start, &text[start..end]))
}

/// Normalization can absorb the sole exact match into an adjacent character (e + acute → é).
/// Count it separately when its source units contribute none of the fuzzy copies.
fn exact_survives_fuzzy(text: &str, at: usize, len: usize, needle: &str) -> bool {
    let Some((start, context)) = fuzzy_boundary_context(text, at, len) else {
        return true;
    };
    let after = at + len;
    let mut normalized: Vec<_> = context.nfkc().map(|ch| (ch, true)).collect();
    // Both strings have the same canonically ordered compatibility decomposition. Pairing
    // those characters identifies every source byte range contributing to each NFKC character,
    // including a mark that reorders past another before it composes with the starter.
    let sources = tagged_nfkd(
        context
            .char_indices()
            .map(|(pos, ch)| (ch, (at - start..after - start).contains(&pos))),
    );
    let outputs = tagged_nfkd(
        normalized
            .iter()
            .enumerate()
            .map(|(index, &(ch, _))| (ch, index)),
    );
    debug_assert_eq!(sources.len(), outputs.len());
    for ((source, inside), (output, index)) in sources.into_iter().zip(outputs) {
        debug_assert_eq!(source, output);
        normalized[index].1 &= inside;
    }
    let mut owned = String::new();
    for line in normalized.split_inclusive(|&(ch, _)| ch == '\n') {
        let newline = line.last().filter(|&&(ch, _)| ch == '\n');
        let body = &line[..line.len() - usize::from(newline.is_some())];
        let kept = body
            .iter()
            .rposition(|&(ch, _)| !is_js_whitespace(ch))
            .map_or(0, |pos| pos + 1);
        for &(ch, inside) in body[..kept].iter().chain(newline) {
            if inside {
                owned.push(mapped_character(ch));
            } else {
                if owned.contains(needle) {
                    return true;
                }
                owned.clear();
            }
        }
    }
    owned.contains(needle)
}

/// NFKD with a source label carried through decomposition and stable canonical ordering.
fn tagged_nfkd<T: Copy>(chars: impl Iterator<Item = (char, T)>) -> Vec<(char, T)> {
    let mut parts = Vec::new();
    for (ch, tag) in chars {
        unicode_normalization::char::decompose_compatible(ch, |part| parts.push((part, tag)));
    }
    for marks in parts.split_mut(|&(ch, _)| canonical_combining_class(ch) == 0) {
        marks.sort_by_key(|&(ch, _)| canonical_combining_class(ch));
    }
    parts
}

#[derive(Clone, Default)]
struct FilteredStarts {
    in_count: usize,
    in_starts: Vec<usize>,
    out_count: usize,
    out_starts: Vec<usize>,
}

/// Counts overlapping occurrences in one pass, partitioning in-range and out-of-range starts.
fn starts_positions_filtered(
    haystack: &str,
    needle: &str,
    mut in_range: impl FnMut(usize) -> bool,
) -> FilteredStarts {
    let mut out = FilteredStarts::default();
    for pos in matching_starts(haystack, needle) {
        let (count, starts) = if in_range(pos) {
            (&mut out.in_count, &mut out.in_starts)
        } else {
            (&mut out.out_count, &mut out.out_starts)
        };
        *count += 1;
        if starts.len() < MAX_REPORTED_ITEMS {
            starts.push(pos);
        }
    }
    out
}

/// Overlapping matches in ascending byte order, in linear time.
fn matching_starts<'a>(haystack: &'a str, needle: &'a str) -> impl Iterator<Item = usize> + 'a {
    let needle = needle.as_bytes();
    let mut table = vec![0usize; needle.len()];
    let mut matched = 0;
    for at in 1..needle.len() {
        while matched > 0 && needle[at] != needle[matched] {
            matched = table[matched - 1];
        }
        if needle[at] == needle[matched] {
            matched += 1;
        }
        table[at] = matched;
    }
    matched = 0;
    let haystack = if needle.is_empty() { "" } else { haystack };
    haystack.bytes().enumerate().filter_map(move |(idx, byte)| {
        while matched > 0 && byte != needle[matched] {
            matched = table[matched - 1];
        }
        if byte == needle[matched] {
            matched += 1;
        }
        if matched == needle.len() {
            matched = table[matched - 1];
            Some(idx + 1 - needle.len())
        } else {
            None
        }
    })
}

/// Applies matches to `content`, retaining each edit's start in the original file.
fn replace(content: &str, matched: &[Matched<'_>]) -> (String, Vec<usize>) {
    let mut starts = vec![0usize; matched.len()];
    for edit in matched {
        starts[edit.index] = edit.at;
    }
    let mut result = content.to_owned();
    for edit in matched.iter().rev() {
        result.replace_range(edit.at..edit.at + edit.len, edit.new_text);
    }
    (result, starts)
}

/// A normalized view and the positions where a CRLF lost its CR. A boundary before the LF
/// maps before the original CR; a boundary after it maps after the complete CRLF.
struct LfText {
    text: String,
    crlf: Vec<usize>,
}

impl LfText {
    fn new(content: &str) -> Self {
        let crlf: Vec<usize> = content
            .match_indices("\r\n")
            .enumerate()
            .map(|(removed, (at, _))| at - removed)
            .collect();
        Self {
            text: normalize_to_lf(content),
            crlf,
        }
    }

    fn original(&self, offset: usize) -> usize {
        offset + self.crlf.partition_point(|&at| at < offset)
    }
}

/// Counts `\n` line breaks, matching `read`'s line numbering (lone `\r` is ordinary content).
fn line_breaks(text: &str) -> usize {
    text.bytes().filter(|&byte| byte == b'\n').count()
}

/// The first line ending is the convention used only for inserted text.
pub(super) fn detect_line_ending(content: &str) -> &'static str {
    match content.find(['\r', '\n']) {
        Some(at) if content[at..].starts_with("\r\n") => "\r\n",
        Some(at) if content.as_bytes()[at] == b'\r' => "\r",
        _ => "\n",
    }
}

pub(crate) fn normalize_to_lf(text: &str) -> String {
    text.replace("\r\n", "\n").replace('\r', "\n")
}

pub(super) fn restore_line_endings(text: &str, ending: &str) -> String {
    if ending != "\n" {
        text.replace('\n', ending)
    } else {
        text.to_owned()
    }
}

/// The form matching falls back to: NFKC, no trailing whitespace, and ASCII quotes, dashes and
/// spaces, so text pasted from a document still finds its source.
pub(super) fn normalize_for_fuzzy_match(text: &str) -> String {
    let normalized = nfkc(text);
    mapped(
        &normalized
            .split('\n')
            .map(|line| line.trim_end_matches(is_js_whitespace))
            .collect::<Vec<_>>()
            .join("\n"),
    )
}

pub(super) fn nfkc(text: &str) -> String {
    text.nfkc().collect()
}

/// The characters the fuzzy form rewrites, one for one.
pub(super) fn mapped(text: &str) -> String {
    text.chars().map(mapped_character).collect()
}

fn mapped_character(character: char) -> char {
    match character {
        '\u{2018}' | '\u{2019}' | '\u{201a}' | '\u{201b}' => '\'',
        '\u{201c}' | '\u{201d}' | '\u{201e}' | '\u{201f}' => '"',
        '\u{2010}'..='\u{2015}' | '\u{2212}' => '-',
        '\u{00a0}' | '\u{2002}'..='\u{200a}' | '\u{202f}' | '\u{205f}' | '\u{3000}' => ' ',
        other => other,
    }
}

/// Where the fuzzy form of a text came from: a match found in that form names a span of the text
/// itself, so only the matched bytes are ever replaced.
struct FuzzyMap {
    /// For every byte of the trimmed fuzzy form, the byte of the untrimmed form it comes from.
    origin: Vec<usize>,
    /// The untrimmed byte the trimmed fuzzy form ends at.
    end: usize,
    /// Every unit of the text: its start and end in the untrimmed fuzzy form, then in the text.
    units: Vec<(usize, usize, usize, usize)>,
}

impl FuzzyMap {
    /// Builds the map of `text`, or nothing when its units do not reproduce the fuzzy form.
    fn new(text: &str) -> Option<Self> {
        let mut form = String::with_capacity(text.len());
        let mut units = Vec::new();
        for (at, end) in units_of(text) {
            let unit = mapped(&nfkc(&text[at..end]));
            if unit.is_empty() {
                return None;
            }
            units.push((form.len(), form.len() + unit.len(), at, end));
            form.push_str(&unit);
        }
        let (trimmed, origin, end) = trimmed_form(&form);
        if trimmed != normalize_for_fuzzy_match(text) {
            return None;
        }
        Some(Self { origin, end, units })
    }

    /// The span of the text the fuzzy form's bytes `[from, to)` stand for, when both ends fall on a
    /// unit boundary.
    fn span(&self, from: usize, to: usize) -> Option<(usize, usize)> {
        let start = self.start_of(from)?;
        let end = self.end_of(to)?;
        (end > start).then_some((start, end - start))
    }

    /// The byte of the text the unit starting at the byte of the fuzzy form at `from` starts at.
    fn start_of(&self, from: usize) -> Option<usize> {
        let at = match self.origin.get(from) {
            Some(at) => *at,
            None if from == self.origin.len() => self.end,
            None => return None,
        };
        self.units
            .binary_search_by_key(&at, |unit| unit.0)
            .ok()
            .map(|found| self.units[found].2)
    }

    /// The byte of the text just after the unit ending at the byte of the fuzzy form after `to`, so
    /// trailing whitespace the trim removed stays outside the span.
    fn end_of(&self, to: usize) -> Option<usize> {
        let at = match to.checked_sub(1).and_then(|last| self.origin.get(last)) {
            Some(last) => last + 1,
            None if to == 0 => 0,
            None => return None,
        };
        self.units
            .binary_search_by_key(&at, |unit| unit.1)
            .ok()
            .map(|found| self.units[found].3)
    }
}

/// The most characters one unit may hold. UAX #15's stream-safe format bounds a run of
/// non-starters at 30, so a longer run is split; if that splits a real composition the form check
/// refuses the map instead of guessing a span.
const MAX_UNIT: usize = 32;

/// Splits `text` into the units normalization works on: one character each, extended while the
/// next character can reorder with the marks already in the unit, or composes with them, up to
/// `MAX_UNIT` characters.
pub(super) fn units_of(text: &str) -> Vec<(usize, usize)> {
    let mut units = Vec::new();
    let mut characters = text.char_indices();
    while let Some((at, first)) = characters.next() {
        let mut end = at + first.len_utf8();
        let mut length = 1;
        while length < MAX_UNIT {
            let Some(next) = characters.as_str().chars().next() else {
                break;
            };
            // A mark that starts a unit can reorder with every later mark, so it stays in the unit
            // whether or not the two compose now. Whitespace keeps its own unit, because the trim
            // removes it on its own.
            let extends = (!is_js_whitespace(first) && starts_with_a_mark(next))
                || composes(&text[at..end], next);
            if !extends {
                break;
            }
            end += next.len_utf8();
            length += 1;
            characters.next();
        }
        units.push((at, end));
    }
    units
}

/// Whether `character` decomposes to a leading non-starter, which reorders with the marks before
/// it under NFKC.
fn starts_with_a_mark(character: char) -> bool {
    character
        .nfkd()
        .next()
        .is_some_and(|first| canonical_combining_class(first) != 0)
}

/// Whether `next` composes with `unit` under NFKC, which keeps the two one unit.
fn composes(unit: &str, next: char) -> bool {
    let mut joined = String::with_capacity(unit.len() + next.len_utf8());
    joined.push_str(unit);
    joined.push(next);
    let mut single = String::with_capacity(next.len_utf8());
    single.push(next);
    nfkc(&joined) != nfkc(unit) + &nfkc(&single)
}

/// The fuzzy form without the trailing whitespace of each line, where every byte comes from in the
/// form it trims, and where the kept bytes end.
pub(super) fn trimmed_form(form: &str) -> (String, Vec<usize>, usize) {
    let mut trimmed = String::with_capacity(form.len());
    let mut origin = Vec::with_capacity(form.len());
    let mut end = 0;
    let mut line_start = 0;
    for line in form.split_inclusive('\n') {
        let (body, newline) = match line.strip_suffix('\n') {
            Some(body) => (body, true),
            None => (line, false),
        };
        let kept = body.trim_end_matches(is_js_whitespace);
        for (offset, character) in kept.char_indices() {
            for byte in 0..character.len_utf8() {
                origin.push(line_start + offset + byte);
            }
            trimmed.push(character);
        }
        end = line_start + kept.len();
        if newline {
            // The kept newline is the physical one, past every byte the trim removed.
            origin.push(line_start + body.len());
            trimmed.push('\n');
            end = line_start + line.len();
        }
        line_start += line.len();
    }
    (trimmed, origin, end)
}

/// The whitespace the reference implementation strips, which includes U+FEFF and not U+0085.
fn is_js_whitespace(character: char) -> bool {
    matches!(
        character,
        '\t' | '\n' | '\u{000b}' | '\u{000c}' | '\r' | ' ' | '\u{00a0}' | '\u{1680}' | '\u{2000}'
            ..='\u{200a}'
                | '\u{2028}'
                | '\u{2029}'
                | '\u{202f}'
                | '\u{205f}'
                | '\u{3000}'
                | '\u{feff}'
    )
}
