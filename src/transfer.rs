//! The transfer table: one URL per file, for the HTTP session that asked for it.
//!
//! An entry lives until it has been idle for [`IDLE`]; a request in progress pins it, so a slow
//! download or a stalled upload never loses its URL. The table is in memory only: a daemon
//! restart starts with none.

use crate::tasks::ALPHABET;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};
use tokio::sync::Mutex as Upload;

/// How long an unused entry lives.
pub const IDLE: Duration = Duration::from_secs(10 * 60);

/// The characters an id is made of, and how many of them it has.
const ID_LENGTH: usize = 26;

/// The `transfer` tool's arguments.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Request {
    /// Path to the file to serve (relative or absolute)
    pub path: String,
}

/// The files this session has published, by id.
pub struct Transfers {
    entries: Mutex<HashMap<String, Entry>>,
}

struct Entry {
    /// The file as it was resolved when the URL was made.
    path: PathBuf,
    /// When this entry was last used, for the idle sweep.
    last_used: Instant,
    /// Requests in progress, which keep the entry alive.
    active: usize,
    /// The lock that makes uploads to this entry take turns.
    upload: Arc<Upload<()>>,
}

impl Default for Transfers {
    fn default() -> Self {
        Self::new()
    }
}

impl Transfers {
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
        }
    }

    /// Publishes `path` and answers the id its URL carries.
    pub fn create(&self, path: PathBuf) -> Result<String, String> {
        self.create_at(path, Instant::now())
    }

    /// `create` with the moment named, so a test can move time without sleeping.
    fn create_at(&self, path: PathBuf, now: Instant) -> Result<String, String> {
        let id = crate::tasks::random_chars(ID_LENGTH)
            .map_err(|error| format!("Cannot make a transfer id: {error}"))?;
        let entry = Entry {
            path,
            last_used: now,
            active: 0,
            upload: Arc::new(Upload::new(())),
        };
        let mut entries = lock(&self.entries);
        // A URL nobody ever asks for is swept here, so a table no request reaches still lets go
        // of what it published.
        sweep(&mut entries, now);
        entries.insert(id.clone(), entry);
        Ok(id)
    }

    /// Admits one request for `id`, or answers `None` when the id is unknown or expired.
    pub fn admit(self: &Arc<Self>, id: &str) -> Option<Admitted> {
        self.admit_at(id, Instant::now())
    }

    /// `admit` with the moment named, so a test can move time without sleeping.
    fn admit_at(self: &Arc<Self>, id: &str, now: Instant) -> Option<Admitted> {
        let mut entries = lock(&self.entries);
        sweep(&mut entries, now);
        let entry = entries.get_mut(id)?;
        entry.active += 1;
        entry.last_used = now;
        Some(Admitted {
            table: Arc::clone(self),
            id: id.to_owned(),
            path: entry.path.clone(),
            upload: Arc::clone(&entry.upload),
        })
    }
}

/// Drops the entries that have been idle for [`IDLE`]. No timer keeps an entry alive or takes it
/// away: the sweep runs on the two calls that touch the table.
fn sweep(entries: &mut HashMap<String, Entry>, now: Instant) {
    entries.retain(|_, entry| {
        entry.active > 0 || now.saturating_duration_since(entry.last_used) < IDLE
    });
}

/// One admitted request: the entry stays alive until this guard is dropped.
pub struct Admitted {
    table: Arc<Transfers>,
    id: String,
    path: PathBuf,
    upload: Arc<Upload<()>>,
}

impl Admitted {
    /// The file this entry names.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Waits for the uploads already running against this entry.
    pub async fn upload(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.upload.lock().await
    }
}

impl Drop for Admitted {
    fn drop(&mut self) {
        let mut entries = lock(&self.table.entries);
        if let Some(entry) = entries.get_mut(&self.id) {
            entry.active = entry.active.saturating_sub(1);
            entry.last_used = Instant::now();
        }
    }
}

/// Whether `id` has the shape `create` generates, so no other path under `/t/` is looked up.
pub fn valid_id(id: &str) -> bool {
    id.len() == ID_LENGTH && id.bytes().all(|byte| ALPHABET.contains(&byte))
}

/// One request's range of a file, as the `Range` header asked for it.
#[derive(Debug, PartialEq, Eq)]
pub enum Range {
    /// Send the whole file.
    Whole,
    /// Send these bytes, both ends included.
    Part { start: u64, end: u64 },
    /// The range starts at or past the end of the file.
    Unsatisfiable,
}

/// What a `Range` header asks of a file of `size` bytes.
///
/// One `bytes=a-` or `bytes=a-b` is honored; anything else — several ranges, a suffix range, a
/// reversed range, a number too large for `u64` — is ignored, and the whole file is sent.
pub fn parse_range(value: &str, size: u64) -> Range {
    let Some(list) = value.trim().strip_prefix("bytes=") else {
        return Range::Whole;
    };
    if list.contains(',') {
        return Range::Whole;
    }
    let Some((first, last)) = list.split_once('-') else {
        return Range::Whole;
    };
    if last.contains('-') {
        return Range::Whole;
    }
    let Ok(start) = decimal(first) else {
        // A suffix range names no start, so it is not one of the two forms honored here.
        return Range::Whole;
    };
    let end = if last.is_empty() {
        u64::MAX
    } else {
        match decimal(last) {
            Ok(end) => end,
            Err(()) => return Range::Whole,
        }
    };
    if end < start {
        return Range::Whole;
    }
    if start >= size {
        return Range::Unsatisfiable;
    }
    Range::Part {
        start,
        end: end.min(size - 1),
    }
}

/// A nonempty decimal number of ASCII digits, which is the only form a range or an offset has.
pub fn decimal(text: &str) -> Result<u64, ()> {
    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(());
    }
    text.parse().map_err(|_| ())
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_id_has_the_generated_shape() {
        let table = Transfers::new();
        let id = table.create(PathBuf::from("/tmp/a")).unwrap();
        assert_eq!(id.len(), ID_LENGTH);
        assert!(valid_id(&id));
        assert_ne!(id, table.create(PathBuf::from("/tmp/a")).unwrap());
        assert!(!valid_id(""));
        assert!(!valid_id(&id[..ID_LENGTH - 1]));
        assert!(!valid_id(&format!("{id}0")));
        assert!(!valid_id(&format!("{}i", &id[..ID_LENGTH - 1])));
        assert!(!valid_id("../t/../../etc/passwd"));
    }

    #[test]
    fn an_entry_expires_after_ten_idle_minutes() {
        let table = Arc::new(Transfers::new());
        let id = table.create(PathBuf::from("/tmp/a")).unwrap();
        let start = Instant::now();
        // Inside ten idle minutes it is served, and the use records the moment it happened.
        let used_at = start + IDLE - Duration::from_secs(1);
        let held = table.admit_at(&id, used_at).unwrap();
        assert_eq!(
            lock(&table.entries).get(&id).unwrap().last_used,
            used_at,
            "a use refreshes the entry"
        );
        drop(held);
        // Past ten idle minutes it is swept away.
        assert!(
            table
                .admit_at(&id, used_at + IDLE + Duration::from_secs(1))
                .is_none()
        );
    }

    #[test]
    fn a_mint_sweeps_what_no_request_will_use() {
        let table = Arc::new(Transfers::new());
        let start = Instant::now();
        let first = table.create_at(PathBuf::from("/tmp/a"), start).unwrap();
        // A URL nobody asked for is gone once its idle time is up, and the new one is there.
        let later = table
            .create_at(PathBuf::from("/tmp/b"), start + IDLE)
            .unwrap();
        let entries = lock(&table.entries);
        assert_eq!(
            entries.len(),
            1,
            "the idle entry was swept and the new one kept"
        );
        assert!(!entries.contains_key(&first));
        assert!(entries.contains_key(&later));
    }

    #[test]
    fn a_request_in_progress_keeps_its_entry_and_refreshes_it() {
        let table = Arc::new(Transfers::new());
        let id = table.create(PathBuf::from("/tmp/a")).unwrap();
        let start = Instant::now();
        let held = table.admit_at(&id, start).unwrap();
        // A request in progress pins the entry, however long the gap looks.
        let later = table.admit_at(&id, start + IDLE * 3).unwrap();
        assert_eq!(held.path(), Path::new("/tmp/a"));
        // A release records the moment it ended, not the moment it was admitted at, so the
        // entry outlives the admission it was made under.
        drop(later);
        let released = lock(&table.entries).get(&id).unwrap().last_used;
        assert!(
            released < start + IDLE * 3,
            "the release refreshed the entry: {released:?}"
        );
        drop(held);
    }

    #[test]
    fn an_unknown_id_is_never_admitted() {
        let table = Arc::new(Transfers::new());
        assert!(table.admit("0123456789abcdefghjkmnpqrs").is_none());
    }

    #[test]
    fn ranges_follow_the_two_forms_that_are_honored() {
        assert_eq!(
            parse_range("bytes=0-", 10),
            Range::Part { start: 0, end: 9 }
        );
        assert_eq!(
            parse_range("bytes=2-5", 10),
            Range::Part { start: 2, end: 5 }
        );
        assert_eq!(
            parse_range("bytes=5-1000", 10),
            Range::Part { start: 5, end: 9 }
        );
        assert_eq!(
            parse_range("bytes=9-9", 10),
            Range::Part { start: 9, end: 9 }
        );
        assert_eq!(parse_range("bytes=-3", 10), Range::Whole);
        assert_eq!(parse_range("bytes=0-1,3-4", 10), Range::Whole);
        assert_eq!(parse_range("bytes=5-2", 10), Range::Whole);
        assert_eq!(parse_range("garbage", 10), Range::Whole);
        assert_eq!(parse_range("bytes=0-", 0), Range::Unsatisfiable);
        assert_eq!(parse_range("bytes=10-", 10), Range::Unsatisfiable);
        assert_eq!(parse_range("bytes=11-20", 10), Range::Unsatisfiable);
        assert_eq!(
            parse_range("bytes=0-18446744073709551616", 10),
            Range::Whole
        );
        assert_eq!(parse_range("bytes=18446744073709551616-", 10), Range::Whole);
    }

    #[test]
    fn a_decimal_is_ascii_digits_only() {
        assert_eq!(decimal("0"), Ok(0));
        assert_eq!(decimal("18446744073709551615"), Ok(u64::MAX));
        assert_eq!(decimal("+1"), Err(()));
        assert_eq!(decimal("-1"), Err(()));
        assert_eq!(decimal("1.5"), Err(()));
        assert_eq!(decimal(""), Err(()));
        assert_eq!(decimal("18446744073709551616"), Err(()));
        assert_eq!(decimal(" 1"), Err(()));
    }
}
