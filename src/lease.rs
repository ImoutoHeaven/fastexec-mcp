//! Non-blocking process-wide active byte leases for media buffers and queued frames.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

/// Shared ownership used only by application-owned buffers, jobs and queues.
/// Official rmcp values do not carry this owner after a response is handed off.
#[derive(Clone, Default)]
pub(crate) struct Owner(Option<Arc<dyn std::any::Any + Send + Sync>>);

impl std::fmt::Debug for Owner {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("Owner")
            .field(&self.0.is_some())
            .finish()
    }
}

impl Owner {
    pub fn new(value: impl std::any::Any + Send + Sync) -> Self {
        Self(Some(Arc::new(value)))
    }

    pub fn get<T: std::any::Any>(&self) -> Option<&T> {
        self.0.as_ref()?.downcast_ref()
    }

    pub fn hold_future<F: std::future::Future>(self, future: F) -> OwnedFuture<F> {
        OwnedFuture {
            future: Box::pin(future),
            _owner: self,
        }
    }
}

// Field order also covers futures dropped before their first poll.
pub(crate) struct OwnedFuture<F> {
    future: std::pin::Pin<Box<F>>,
    _owner: Owner,
}

impl<F: std::future::Future> std::future::Future for OwnedFuture<F> {
    type Output = F::Output;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.get_mut().future.as_mut().poll(cx)
    }
}

/// The payload is destroyed before its last process-local byte owner.
#[derive(Debug)]
pub(crate) struct Owned<T> {
    pub value: T,
    pub owner: Owner,
}

impl<T> Owned<T> {
    pub fn new(value: T, owner: Owner) -> Self {
        Self { value, owner }
    }

    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> Owned<U> {
        let Self { value, owner } = self;
        Owned::new(f(value), owner)
    }

    /// A derived value may reserve additional buffers while keeping its input owner.
    pub fn flat_map<U>(self, f: impl FnOnce(T) -> Owned<U>) -> Owned<U> {
        let Self { value, owner } = self;
        let result = f(value);
        Owned::new(result.value, Owner::new((owner, result.owner)))
    }

    /// Capturing this whole wrapper also covers a blocking job that has not started.
    pub fn run<U>(self, f: impl FnOnce(T) -> U) -> U {
        let Self { value, owner } = self;
        let result = f(value);
        drop(owner);
        result
    }
}

/// Counts the serialized JSON bytes of `message` without allocating a buffer.
pub fn json_len<T: serde::Serialize + ?Sized>(message: &T) -> std::io::Result<usize> {
    struct ByteCounter(usize);

    impl std::io::Write for ByteCounter {
        fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
            self.0 += buffer.len();
            Ok(buffer.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    let mut counter = ByteCounter(0);
    serde_json::to_writer(&mut counter, message)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    Ok(counter.0)
}

/// Default per-process active byte budget: 1 GiB.
pub const DEFAULT_LIMIT: usize = 1 << 30;
/// Independent control-plane byte budget for `list`, short `kill`, progress, and error receipts.
pub const CONTROL_LIMIT: usize = 8 << 20;
/// Maximum serialized outbound frame body: 512 MiB.
pub const FRAME_LIMIT: usize = 512 << 20;
pub(crate) const SHORT_INPUT: usize = 64 * 1024;

/// Tracks active bytes across in-flight reads, encodings, and queued outbound frames.
#[derive(Debug)]
pub struct BytePool {
    used: AtomicUsize,
    limit: usize,
}

impl BytePool {
    pub fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            used: AtomicUsize::new(0),
            limit,
        })
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    pub fn used(&self) -> usize {
        self.used.load(Ordering::SeqCst)
    }

    pub fn available(&self) -> usize {
        self.limit.saturating_sub(self.used())
    }

    /// Reserves `bytes` without waiting, returning `None` immediately when full.
    pub fn try_acquire(self: &Arc<Self>, bytes: usize) -> Option<Lease> {
        if !self.try_add(bytes) {
            return None;
        }
        Some(Lease {
            pool: Arc::clone(self),
            bytes,
        })
    }

    fn try_add(&self, bytes: usize) -> bool {
        if bytes > self.limit {
            return false;
        }
        if bytes == 0 {
            return true;
        }
        let mut current = self.used.load(Ordering::Relaxed);
        loop {
            let Some(next) = current.checked_add(bytes) else {
                return false;
            };
            if next > self.limit {
                return false;
            }
            match self.used.compare_exchange_weak(
                current,
                next,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }

    fn sub(&self, bytes: usize) {
        if bytes > 0 {
            let prev = self.used.fetch_sub(bytes, Ordering::SeqCst);
            debug_assert!(
                prev >= bytes,
                "released {bytes} bytes with only {prev} used"
            );
        }
    }
}

/// The process-wide byte pool, overrideable in debug builds via `FASTEXEC_TEST_MEMORY_LIMIT`.
pub fn process_pool() -> &'static Arc<BytePool> {
    static POOL: OnceLock<Arc<BytePool>> = OnceLock::new();
    POOL.get_or_init(|| {
        let limit = std::env::var("FASTEXEC_TEST_MEMORY_LIMIT")
            .ok()
            .and_then(|raw| raw.parse::<usize>().ok())
            .filter(|limit| cfg!(debug_assertions) && (1..=DEFAULT_LIMIT).contains(limit))
            .unwrap_or(DEFAULT_LIMIT);
        BytePool::new(limit)
    })
}

/// Independent 8 MiB control-plane byte pool kept usable when the main media pool is full.
pub fn control_pool() -> &'static Arc<BytePool> {
    static CONTROL: OnceLock<Arc<BytePool>> = OnceLock::new();
    CONTROL.get_or_init(|| BytePool::new(CONTROL_LIMIT))
}

/// Ownership of reserved active bytes, released only when dropped or explicitly shrunk.
#[derive(Debug)]
pub struct Lease {
    pool: Arc<BytePool>,
    bytes: usize,
}

impl Lease {
    pub fn empty(pool: &Arc<BytePool>) -> Self {
        Self {
            pool: Arc::clone(pool),
            bytes: 0,
        }
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// Adds `more` bytes without blocking. On `false`, this lease stays unchanged until dropped.
    pub fn try_grow(&mut self, more: usize) -> bool {
        if more == 0 {
            return true;
        }
        let Some(next) = self.bytes.checked_add(more) else {
            return false;
        };
        if !self.pool.try_add(more) {
            return false;
        }
        self.bytes = next;
        true
    }

    /// Adjusts the lease to `target` bytes, shrinking in place or growing without blocking.
    pub fn try_resize(&mut self, target: usize) -> bool {
        if target <= self.bytes {
            self.shrink_to(target);
            true
        } else {
            self.try_grow(target - self.bytes)
        }
    }

    /// Releases bytes above `target` once an intermediate buffer has been freed.
    pub fn shrink_to(&mut self, target: usize) {
        if target < self.bytes {
            let freed = self.bytes - target;
            self.bytes = target;
            self.pool.sub(freed);
        }
    }

    /// Transfers `amount` already-reserved bytes into a separate lease without recounting them.
    pub fn split_off(&mut self, amount: usize) -> Self {
        assert!(
            amount <= self.bytes,
            "cannot split {amount} bytes from a {}-byte lease",
            self.bytes
        );
        self.bytes -= amount;
        Self {
            pool: Arc::clone(&self.pool),
            bytes: amount,
        }
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        self.pool.sub(self.bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn app_owner_holds_future_payload_until_drop_even_before_poll() {
        struct Payload(Arc<BytePool>);
        impl Drop for Payload {
            fn drop(&mut self) {
                assert_eq!(self.0.used(), 64, "payload must drop before its owner");
            }
        }
        for poll in [false, true] {
            let pool = BytePool::new(64);
            let payload = Payload(Arc::clone(&pool));
            let owner = Owner::new(pool.try_acquire(64).unwrap());
            let mut future = Box::pin(owner.hold_future(async move {
                let _payload = payload;
                std::future::pending::<()>().await;
            }));
            if poll {
                std::future::poll_fn(|cx| {
                    assert!(std::future::Future::poll(future.as_mut(), cx).is_pending());
                    std::task::Poll::Ready(())
                })
                .await;
            }
            drop(future);
            assert_eq!(pool.used(), 0);
        }
    }

    #[test]
    fn leases_admit_grow_handoff_and_reclaim_without_blocking() {
        let pool = BytePool::new(100);
        assert!(pool.try_acquire(101).is_none());
        assert_eq!(pool.used(), 0);

        let mut first = pool.try_acquire(60).expect("60 fits in 100");
        assert_eq!(pool.used(), 60);
        assert!(
            pool.try_acquire(41).is_none(),
            "second caller refuses immediately"
        );
        let second = pool.try_acquire(40).expect("exact boundary N=100 fits");
        assert_eq!(pool.used(), 100);
        assert!(!first.try_grow(1), "N+1 growth fails without blocking");
        drop(second);
        assert_eq!(pool.used(), 60);

        assert!(first.try_grow(30));
        assert_eq!(first.bytes(), 90);
        assert_eq!(pool.used(), 90);

        // Handoff transfers reserved bytes without double counting.
        let queued = first.split_off(50);
        assert_eq!(first.bytes(), 40);
        assert_eq!(queued.bytes(), 50);
        assert_eq!(pool.used(), 90);

        // Freeing the staging buffer returns only its share while queued bytes remain charged.
        first.shrink_to(0);
        drop(first);
        assert_eq!(pool.used(), 50);
        drop(queued);
        assert_eq!(pool.used(), 0);
    }

    #[tokio::test]
    async fn cancelled_caller_does_not_reclaim_lease_until_blocking_worker_exits() {
        let pool = BytePool::new(64);
        let lease = pool.try_acquire(64).unwrap();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();

        let worker = tokio::task::spawn_blocking(move || {
            let buffer = vec![0xAB_u8; 64];
            let _ = entered_tx.send(());
            release_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap();
            assert_eq!(buffer.len(), 64);
            drop(buffer);
            drop(lease);
        });

        entered_rx.await.unwrap();
        worker.abort();
        // Aborting a spawn_blocking handle cancels the JoinHandle wait, not the running OS thread.
        assert_eq!(pool.used(), 64);
        assert!(pool.try_acquire(1).is_none());

        release_tx.send(()).unwrap();
        let _ = worker.await;
        assert_eq!(pool.used(), 0);
        assert!(pool.try_acquire(64).is_some());
    }

    #[test]
    fn concurrent_leases_respect_capacity_and_reclaim_all_bytes() {
        let pool = BytePool::new(100);
        let barrier = std::sync::Barrier::new(2);
        std::thread::scope(|scope| {
            for _ in 0..2 {
                scope.spawn(|| {
                    let mut lease = pool.try_acquire(40);
                    barrier.wait();
                    let grew = lease.as_mut().map(|lease| lease.try_grow(30));
                    barrier.wait();
                    assert_eq!(grew, Some(false));
                });
            }
        });
        assert_eq!(pool.used(), 0);

        let mut lease = pool.try_acquire(40).unwrap();
        assert!(lease.try_grow(30));
        assert_eq!(pool.used(), 70);
        drop(lease);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn owned_drops_value_before_owner_and_json_len_matches_wire() {
        struct DropOrderCheck {
            pool: Arc<BytePool>,
            expected_used: usize,
        }
        impl Drop for DropOrderCheck {
            fn drop(&mut self) {
                assert_eq!(
                    self.pool.used(),
                    self.expected_used,
                    "value must drop before owner releases its lease"
                );
            }
        }

        assert_eq!(FRAME_LIMIT, 512 << 20);
        assert_eq!(SHORT_INPUT, 64 * 1024);

        let pool = BytePool::new(128);
        let owned = Owned::new(
            DropOrderCheck {
                pool: Arc::clone(&pool),
                expected_used: 32,
            },
            Owner::new(pool.try_acquire(32).unwrap()),
        );
        drop(owned);
        assert_eq!(pool.used(), 0);

        let chained = Owned::new(
            DropOrderCheck {
                pool: Arc::clone(&pool),
                expected_used: 48,
            },
            Owner::new(pool.try_acquire(32).unwrap()),
        )
        .flat_map(|first| {
            let derived = Owner::new(pool.try_acquire(16).unwrap());
            drop(first);
            Owned::new(
                DropOrderCheck {
                    pool: Arc::clone(&pool),
                    expected_used: 48,
                },
                derived,
            )
        });
        let out = chained.map(|inner| inner).run(|inner| {
            drop(inner);
            assert_eq!(pool.used(), 48);
            7usize
        });
        assert_eq!(out, 7);
        assert_eq!(pool.used(), 0);

        let sample = serde_json::json!({"hello": ["world", 42, true]});
        assert_eq!(
            json_len(&sample).unwrap(),
            serde_json::to_vec(&sample).unwrap().len()
        );
    }
}
