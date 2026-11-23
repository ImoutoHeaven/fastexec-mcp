//! The transfer table: one temporary `/t/<id>` capability URL per published file.
//!
//! An entry lives until it has been idle for [`IDLE`]; a request in progress pins it, so a slow
//! download or a stalled upload never loses its URL. The table is in memory only: a server or
//! daemon restart starts with none.

use crate::tasks::ALPHABET;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard, Weak};
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

/// Published capabilities, by id.
pub struct Transfers {
    entries: Mutex<HashMap<String, Entry>>,
    /// Active writers share a lock by the opened file's identity, not by URL or path spelling.
    /// Weak entries do not retain a lock for every file ever published.
    uploads: Mutex<HashMap<FileIdentity, Weak<Upload<()>>>>,
}

/// The identity remains stable while an open handle keeps the file alive. Unlike a canonical
/// path, it also identifies hard links and a file created through either of two capabilities.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
struct FileIdentity {
    volume: u64,
    file: u64,
}

async fn file_identity(file: &tokio::fs::File) -> std::io::Result<FileIdentity> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let metadata = file.metadata().await?;
        Ok(FileIdentity {
            volume: metadata.dev(),
            file: metadata.ino(),
        })
    }
    #[cfg(windows)]
    {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Storage::FileSystem::{
            BY_HANDLE_FILE_INFORMATION, GetFileInformationByHandle,
        };
        // SAFETY: the live file owns this handle; the call initializes the structure supplied
        // for exactly its size, and no pointer escapes it.
        let mut info: BY_HANDLE_FILE_INFORMATION = unsafe { std::mem::zeroed() };
        let ok = unsafe { GetFileInformationByHandle(file.as_raw_handle().cast(), &mut info) };
        if ok == 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(FileIdentity {
            volume: u64::from(info.dwVolumeSerialNumber),
            file: (u64::from(info.nFileIndexHigh) << 32) | u64::from(info.nFileIndexLow),
        })
    }
}

struct Entry {
    /// The file as it was resolved when the URL was made.
    path: Arc<PathBuf>,
    /// When this entry was last used, for the idle sweep.
    last_used: Instant,
    /// Requests in progress, which keep the entry alive.
    active: usize,
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
            uploads: Mutex::new(HashMap::new()),
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
            path: Arc::new(path),
            last_used: now,
            active: 0,
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
            path: Arc::clone(&entry.path),
        })
    }

    /// Drops entries that have been idle for [`IDLE`] with no active requests.
    pub fn sweep(&self, now: Instant) {
        let _ = self.is_idle(now);
    }

    /// Sweeps expired entries at `now` and reports whether none remain live or in flight.
    pub fn is_idle(&self, now: Instant) -> bool {
        let empty = {
            let mut entries = lock(&self.entries);
            sweep(&mut entries, now);
            entries.is_empty()
        };
        if empty {
            lock(&self.uploads).retain(|_, entry| entry.strong_count() > 0);
        }
        empty
    }
}

/// Drops entries idle for [`IDLE`], called on capability operations and daemon ticks.
fn sweep(entries: &mut HashMap<String, Entry>, now: Instant) {
    entries.retain(|_, entry| {
        entry.active > 0 || now.saturating_duration_since(entry.last_used) < IDLE
    });
}

/// One admitted request: the entry stays alive until this guard is dropped.
pub struct Admitted {
    table: Arc<Transfers>,
    id: String,
    path: Arc<PathBuf>,
}

impl Admitted {
    /// Shares the published path allocation with an admitted request or blocking worker.
    pub fn path(&self) -> Arc<PathBuf> {
        Arc::clone(&self.path)
    }

    /// Waits for writers to the same opened file, including writers using another capability,
    /// a relative path, a symlink or a hard link. The caller keeps the file open through the
    /// write, so its identity cannot be recycled while this guard is in use.
    pub async fn upload(
        &self,
        file: &tokio::fs::File,
    ) -> std::io::Result<tokio::sync::OwnedMutexGuard<()>> {
        let identity = file_identity(file).await?;
        let upload = {
            let mut uploads = lock(&self.table.uploads);
            uploads.retain(|_, entry| entry.strong_count() > 0);
            match uploads.get(&identity).and_then(Weak::upgrade) {
                Some(upload) => upload,
                None => {
                    let upload = Arc::new(Upload::new(()));
                    uploads.insert(identity, Arc::downgrade(&upload));
                    upload
                }
            }
        };
        Ok(upload.lock_owned().await)
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
    fn admitted_requests_share_the_published_path_allocation() {
        let table = Arc::new(Transfers::new());
        let path = PathBuf::from("relative/future/ordinary.txt");
        let bytes = path.as_os_str().as_encoded_bytes().as_ptr();
        let id = table.create(path).unwrap();
        let first = table.admit(&id).unwrap();
        let second = table.admit(&id).unwrap();
        let shared = first.path();
        let entries = lock(&table.entries);
        let entry = entries.get(&id).unwrap();
        assert!(Arc::ptr_eq(&entry.path, &shared));
        assert!(Arc::ptr_eq(&shared, &second.path()));
        assert_eq!(shared.as_os_str().as_encoded_bytes().as_ptr(), bytes);
        assert_eq!(
            shared.as_path(),
            std::path::Path::new("relative/future/ordinary.txt")
        );
        assert_eq!(entry.active, 2);
        drop(entries);
        drop(first);
        assert_eq!(lock(&table.entries).get(&id).unwrap().active, 1);
        drop(second);
        assert_eq!(lock(&table.entries).get(&id).unwrap().active, 0);
        drop(shared);
        assert!(table.is_idle(Instant::now() + IDLE));
    }

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
        assert_eq!(held.path().as_path(), std::path::Path::new("/tmp/a"));
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
    fn is_idle_holds_for_live_and_active_entries_until_swept() {
        let table = Arc::new(Transfers::new());
        let start = Instant::now();
        assert!(table.is_idle(start));
        let id = table.create_at(PathBuf::from("/tmp/a"), start).unwrap();
        assert!(!table.is_idle(start));
        assert!(!table.is_idle(start + IDLE - Duration::from_secs(1)));

        let held = table.admit_at(&id, start).unwrap();
        assert!(!table.is_idle(start + IDLE * 2));
        drop(held);
        let after_drop = Instant::now();
        assert!(!table.is_idle(after_drop));
        assert!(table.is_idle(after_drop + IDLE));
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

/// How a local or HTTP server publishes `/t/` capabilities when `transfer` is enabled.
#[derive(Clone)]
pub(crate) enum TransferEndpoint {
    /// Local stdio: a dedicated `/t/` listener with a fixed base URL.
    Fixed {
        table: Arc<Transfers>,
        base: Arc<str>,
    },
    /// Streamable HTTP: `public_root` when configured, else inferred from each request.
    Http {
        table: Arc<Transfers>,
        public_root: Option<Arc<str>>,
    },
}

pub(crate) fn transfer_error(
    message: String,
    render: crate::service::render::Render,
) -> rmcp::model::CallToolResult {
    let effective = render.effective();
    let mut result =
        rmcp::model::CallToolResult::error(vec![rmcp::model::ContentBlock::text(message.clone())]);
    result.structured_content = Some(serde_json::json!({ "ok": false, "error": message }));
    crate::service::render::suppress(result, effective)
}

pub(crate) type TransferInspection = Result<(std::path::PathBuf, String, Option<u64>), String>;

pub(crate) fn inspect_transfer_request(
    request: Request,
    launch: Arc<crate::session::LaunchContext>,
    input_owner: crate::lease::Owner,
) -> impl std::future::Future<Output = crate::ipc::Owned<TransferInspection>> {
    // Build the owner before creating the future, including its never-polled case.
    let input = crate::ipc::Owned::new(request, input_owner);
    async move {
        match tokio::task::spawn_blocking(move || {
            input.flat_map(|request| {
                crate::files::path::with_path_workspace(&request.path, &launch, 0, |lease| {
                    let path = crate::files::path::resolve(&request.path, &launch)?;
                    let shown = crate::files::path::display_path(&path);
                    let bytes = inspect_transfer_target(&path, &shown, launch.memory(), lease)?;
                    Ok::<_, String>((path, shown, bytes))
                })
                .unwrap_or_else(|message| crate::ipc::Owned::new(Err(message), Default::default()))
            })
        })
        .await
        {
            Ok(result) => result,
            Err(error) => crate::ipc::Owned::new(
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
    table: &Arc<Transfers>,
    base: &str,
    render: crate::service::render::Render,
) -> rmcp::model::CallToolResult {
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
    let mut result =
        rmcp::model::CallToolResult::success(vec![rmcp::model::ContentBlock::text(text)]);
    result.structured_content = Some(serde_json::json!({
        "ok": true,
        "url": url,
        "path": shown_path,
        "bytes": bytes,
    }));
    crate::service::render::suppress(result, effective)
}

pub(crate) fn publish_transfer<'a>(
    request: Request,
    launch: Arc<crate::session::LaunchContext>,
    table: &'a Arc<Transfers>,
    base: &'a str,
    render: crate::service::render::Render,
    input_owner: crate::lease::Owner,
) -> impl std::future::Future<Output = crate::files::TrackedResult> + 'a {
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
        crate::files::TrackedResult::owned(result, owner)
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
            let target = crate::files::write::write_target(path, pool).map_err(failed)?;
            crate::files::path::reserve_target_workspace(&target.value, lease, pool)
                .map_err(failed)?;
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
                                crate::files::path::display_path(ancestor)
                            ));
                        }
                        Err(error) => return Err(failed(error)),
                        Ok(_) => {}
                    },
                    Ok(_) => {}
                }
                return Err(format!(
                    "Cannot transfer {shown_path}: {} is not a directory.",
                    crate::files::path::display_path(ancestor)
                ));
            }
            Ok(None)
        }
        Err(error) => Err(failed(error)),
    }
}

#[cfg(test)]
mod transfer_owner_tests {
    use super::*;
    use crate::service::render::{OutputMode, Render};
    use std::future::Future;
    use std::time::Duration;

    fn launch() -> Arc<crate::session::LaunchContext> {
        Arc::new(crate::session::LaunchContext::new(
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")),
            Vec::new(),
        ))
    }

    #[tokio::test]
    async fn transfer_owner_survives_unpolled_future_and_completed_inspection() {
        let pool = crate::lease::BytePool::new(64);
        let request = || Request {
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
                Request { path: path.into() },
                launch(),
                crate::lease::Owner::new(pool.try_acquire(64).unwrap()),
            )
            .await;
            assert_eq!(inspected.value.is_ok(), path == "Cargo.toml");
            assert_eq!(pool.used(), 64);
            drop(inspected);
            assert_eq!(pool.used(), 0);
        }

        let table = Arc::new(Transfers::new());
        let result = publish_transfer(
            request(),
            launch(),
            &table,
            "http://localhost/",
            Render {
                output_mode: OutputMode::Both,
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
            let pool = crate::lease::BytePool::new(64 * 1024);
            let context = Arc::new(
                crate::session::LaunchContext::new(
                    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")),
                    Vec::new(),
                )
                .with_memory(Arc::clone(&pool)),
            );
            let result = inspect_transfer_request(
                Request { path: path.into() },
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
        let pool = crate::lease::BytePool::new(0);
        let context = Arc::new(
            crate::session::LaunchContext::new(
                std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")),
                Vec::new(),
            )
            .with_memory(Arc::clone(&pool)),
        );
        let result = inspect_transfer_request(
            Request {
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
            let pool = crate::lease::BytePool::new(64);
            let (released, retired) = tokio::sync::oneshot::channel();
            let owner =
                crate::lease::Owner::new((pool.try_acquire(64).unwrap(), Released(Some(released))));
            let mut inspection = Box::pin(inspect_transfer_request(
                Request {
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
