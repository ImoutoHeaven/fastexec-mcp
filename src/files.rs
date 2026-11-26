//! The text file tools: `read`, `write` and `edit`, and the path resolution they share.
//!
//! Async core entrypoints run file work on `spawn_blocking` and return typed domain results.
//! Read streams a file and keeps only its output window; write and edit hold the whole file
//! because they rewrite it.

pub(crate) mod edit;
pub(crate) mod path;
pub(crate) mod read;
pub(crate) mod write;

#[cfg(test)]
mod tests;

pub use self::edit::{EditError, EditFailure, EditMatch, FileEditData, edit_core};
pub use self::read::{FileReadData, MediaPayload, ReadPolicy, TextSlice, read_core};
pub use self::write::{FileWriteData, write_core};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::{Mutex, MutexGuard};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileError {
    Validation(String),
    NotFound(String),
    Limit(String),
    Overload(String),
    Cancelled(String),
    Io(String),
}

impl FileError {
    pub fn message(&self) -> &str {
        match self {
            Self::Validation(msg)
            | Self::NotFound(msg)
            | Self::Limit(msg)
            | Self::Overload(msg)
            | Self::Cancelled(msg)
            | Self::Io(msg) => msg,
        }
    }

    pub fn into_message(self) -> String {
        match self {
            Self::Validation(msg)
            | Self::NotFound(msg)
            | Self::Limit(msg)
            | Self::Overload(msg)
            | Self::Cancelled(msg)
            | Self::Io(msg) => msg,
        }
    }
}

impl fmt::Display for FileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message())
    }
}

/// The one lock that serializes write and edit, so a read-modify-write cannot interleave with
/// another one. It is held for the whole blocking operation, never across an await.
static MUTATION: Mutex<()> = Mutex::new(());

pub(super) fn lock() -> MutexGuard<'static, ()> {
    MUTATION
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ReadRequest {
    /// Path to the file to read (relative or absolute)
    pub path: String,
    /// Line number to start reading from (1-indexed; negative counts from the end, -1 is the last line)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<i64>,
    /// Maximum number of lines to read (at least 1)
    #[schemars(range(min = 1))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct WriteRequest {
    /// Path to the file to write (relative or absolute)
    pub path: String,
    /// Content to write to the file
    pub content: String,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct EditRequest {
    /// Path to the file to edit (relative or absolute)
    pub path: String,
    /// One or more targeted replacements. Each edit is matched against the original file, not incrementally. Do not include overlapping or nested edits. If two changes touch the same block or nearby lines, merge them into one edit instead.
    pub edits: Vec<Edit>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Edit {
    /// Exact text for one targeted replacement. It must be unique in the original file (or within startLine..=endLine) and must not overlap with any other edits[].oldText in the same call.
    pub old_text: String,
    /// Replacement text for this targeted edit.
    pub new_text: String,
    /// Optional 1-based line range the match must start in (inclusive first line); use it to pick one of several identical matches.
    #[schemars(range(min = 1))]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_line: Option<u64>,
    /// Optional 1-based inclusive last line the match must start in.
    #[schemars(range(min = 1))]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_line: Option<u64>,
}
