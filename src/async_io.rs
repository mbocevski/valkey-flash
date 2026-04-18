use crossbeam_channel::{bounded, Receiver, Sender, TrySendError};

use crate::storage::backend::{StorageError, StorageResult};

// ── CompletionHandle ──────────────────────────────────────────────────────────

/// Deliver an async I/O result to the original caller (blocked Valkey client in
/// production, sync channel in unit tests).
///
/// Implementors must be `Send + 'static` so they can be shipped to a worker
/// thread.
pub trait CompletionHandle: Send + 'static {
    fn complete(self: Box<Self>, result: StorageResult<Vec<u8>>);
}

// ── PoolError ─────────────────────────────────────────────────────────────────

#[derive(Debug, PartialEq, Eq)]
pub enum PoolError {
    /// Task queue is full; caller should reject the command with EBUSY.
    Full,
    /// Pool has been shut down (all senders dropped).
    Closed,
}

impl std::fmt::Display for PoolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PoolError::Full => write!(f, "async I/O pool queue is full"),
            PoolError::Closed => write!(f, "async I/O pool is closed"),
        }
    }
}

// ── AsyncThreadPool ───────────────────────────────────────────────────────────

type Task = Box<dyn FnOnce() -> StorageResult<Vec<u8>> + Send>;
type IoTask = (Box<dyn CompletionHandle>, Task);

pub struct AsyncThreadPool {
    sender: Sender<IoTask>,
    // Workers exit naturally when sender is dropped (channel closes). JoinHandles
    // are kept so tests can observe clean shutdown if needed.
    _workers: Vec<std::thread::JoinHandle<()>>,
}

impl AsyncThreadPool {
    /// Create a pool sized to the machine's logical CPU count with a queue depth
    /// of `num_threads * 4`.
    pub fn new_default() -> Self {
        let n = num_cpus::get();
        Self::with_queue_size(n, n * 4)
    }

    /// Create a pool with `num_threads` workers and a queue depth of
    /// `num_threads * 4`.
    pub fn new(num_threads: usize) -> Self {
        Self::with_queue_size(num_threads, num_threads * 4)
    }

    /// Create a pool with `num_threads` workers and a task queue bounded to
    /// `queue_size` entries. Submissions beyond `queue_size` return
    /// [`PoolError::Full`] immediately.
    pub fn with_queue_size(num_threads: usize, queue_size: usize) -> Self {
        let (sender, receiver) = bounded::<IoTask>(queue_size);
        let workers = (0..num_threads)
            .map(|_| {
                let rx: Receiver<IoTask> = receiver.clone();
                std::thread::spawn(move || worker_loop(rx))
            })
            .collect();
        // Drop the original receiver; each worker holds its own clone.
        drop(receiver);
        AsyncThreadPool {
            sender,
            _workers: workers,
        }
    }

    /// Submit an I/O task for execution on a worker thread.
    ///
    /// `handle.complete(result)` is called on the worker thread after `task()`
    /// returns (or panics). A panicking task causes `handle` to receive
    /// `StorageError::Other("worker task panicked")` — the worker survives and
    /// continues processing subsequent tasks.
    ///
    /// Returns `Err(PoolError::Full)` immediately without blocking if the queue
    /// is at capacity.
    pub fn submit(
        &self,
        handle: Box<dyn CompletionHandle>,
        task: impl FnOnce() -> StorageResult<Vec<u8>> + Send + 'static,
    ) -> Result<(), PoolError> {
        self.sender
            .try_send((handle, Box::new(task)))
            .map_err(|e| match e {
                TrySendError::Full(_) => PoolError::Full,
                TrySendError::Disconnected(_) => PoolError::Closed,
            })
    }

    /// Submit a task. If the pool is full or closed, `handle.complete(Err(...))` is called
    /// immediately on the calling thread instead of returning an error.
    ///
    /// Use this when the client has already been blocked: returning an error from the command
    /// handler after `block_client()` would leave the client permanently stuck, whereas this
    /// method guarantees `complete()` is always called exactly once.
    pub fn submit_or_complete(
        &self,
        handle: Box<dyn CompletionHandle>,
        task: impl FnOnce() -> StorageResult<Vec<u8>> + Send + 'static,
    ) {
        match self.sender.try_send((handle, Box::new(task))) {
            Ok(_) => {}
            Err(TrySendError::Full((handle, _))) => {
                handle.complete(Err(StorageError::Other(PoolError::Full.to_string())));
            }
            Err(TrySendError::Disconnected((handle, _))) => {
                handle.complete(Err(StorageError::Other(PoolError::Closed.to_string())));
            }
        }
    }
}

fn worker_loop(rx: Receiver<IoTask>) {
    while let Ok((handle, task)) = rx.recv() {
        // SAFETY: AssertUnwindSafe is sound here because:
        // - `task` is an owned, heap-allocated closure; a panic inside it is
        //   caught and turned into StorageError — no shared state is left
        //   inconsistent from the pool's perspective.
        // - `handle` is consumed by `complete()` immediately after; no aliasing.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(task))
            .unwrap_or_else(|_| Err(StorageError::Other("worker task panicked".into())));
        handle.complete(result);
    }
}

// ── Production: BlockedClientHandle ──────────────────────────────────────────

/// Wraps `ThreadSafeContext<BlockedClient<()>>` and replies to the blocked
/// Valkey client when the I/O task completes.
///
/// `tsc.reply()` requires no GIL lock (Reply functions are exempt from the
/// lock requirement). Dropping `tsc` automatically calls
/// `FreeThreadSafeContext` followed by `UnblockClient`.
///
/// Not compiled in test mode because the module API function pointers are not
/// registered outside a running Valkey server.
#[cfg(not(test))]
pub struct BlockedClientHandle {
    tsc: valkey_module::ThreadSafeContext<valkey_module::BlockedClient<()>>,
}

#[cfg(not(test))]
impl BlockedClientHandle {
    /// Take ownership of `bc` and wrap it in a `ThreadSafeContext`.
    ///
    /// # Safety invariants
    /// - `bc.inner` must be non-null (guaranteed by `ctx.block_client()`).
    /// - The blocked client pointer remains valid until `UnblockClient` is
    ///   called, which happens when `tsc` is dropped inside `complete()`.
    pub fn new(bc: valkey_module::BlockedClient<()>) -> Self {
        BlockedClientHandle {
            tsc: valkey_module::ThreadSafeContext::with_blocked_client(bc),
        }
    }
}

#[cfg(not(test))]
impl CompletionHandle for BlockedClientHandle {
    fn complete(self: Box<Self>, result: StorageResult<Vec<u8>>) {
        use valkey_module::{ValkeyError, ValkeyValue};
        let reply = result
            .map(ValkeyValue::StringBuffer)
            .map_err(|e| ValkeyError::String(e.to_string()));
        // tsc.reply() sends the reply to the blocked client (no GIL lock needed).
        // Dropping `self` → drops tsc → FreeThreadSafeContext + UnblockClient.
        self.tsc.reply(reply);
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::{self, SyncSender};
    use std::time::Duration;

    // ── Test CompletionHandle implementation ──────────────────────────────

    pub struct ChannelHandle {
        sender: SyncSender<StorageResult<Vec<u8>>>,
    }

    impl CompletionHandle for ChannelHandle {
        fn complete(self: Box<Self>, result: StorageResult<Vec<u8>>) {
            let _ = self.sender.send(result);
        }
    }

    fn make_handle() -> (Box<ChannelHandle>, mpsc::Receiver<StorageResult<Vec<u8>>>) {
        let (tx, rx) = mpsc::sync_channel(1);
        (Box::new(ChannelHandle { sender: tx }), rx)
    }

    // ── pool_roundtrip_single_task ─────────────────────────────────────────
    // Verifies the basic blocked-client round-trip path using ChannelHandle
    // as a mock for ThreadSafeContext.
    #[test]
    fn pool_roundtrip_single_task_mock_blocked_client() {
        let pool = AsyncThreadPool::new(1);
        let (handle, rx) = make_handle();
        pool.submit(handle, || Ok(b"hello".to_vec())).unwrap();
        let result = rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(result.unwrap(), b"hello".to_vec());
    }

    // ── pool_saturation_all_tasks_complete ────────────────────────────────
    // Submit 10× workers tasks; all must complete even under saturation.
    #[test]
    fn pool_saturation_all_tasks_complete() {
        let n_workers = 2;
        let n_tasks = n_workers * 5; // 10 tasks
                                     // Queue must be large enough to hold all tasks; workers drain concurrently.
        let pool = AsyncThreadPool::with_queue_size(n_workers, n_tasks);
        let (done_tx, done_rx) = mpsc::sync_channel::<StorageResult<Vec<u8>>>(n_tasks);
        for i in 0..n_tasks as u8 {
            let tx = done_tx.clone();
            pool.submit(Box::new(ChannelHandle { sender: tx }), move || Ok(vec![i]))
                .expect("submit should succeed while queue has room");
        }
        drop(done_tx);
        let results: Vec<_> = done_rx.iter().map(|r| r.expect("task ok")).collect();
        assert_eq!(results.len(), n_tasks);
    }

    // ── worker_panic_recovered_no_context_leak ────────────────────────────
    // A panicking task must not kill the worker or leak the CompletionHandle;
    // the next task must still execute cleanly.
    #[test]
    fn worker_panic_recovered_no_context_leak() {
        let pool = AsyncThreadPool::new(1);

        let (h1, rx1) = make_handle();
        pool.submit(h1, || panic!("intentional worker panic"))
            .unwrap();
        let r1 = rx1
            .recv_timeout(Duration::from_secs(2))
            .expect("handle must be completed even after panic");
        assert!(
            r1.is_err(),
            "panic must surface as an error via CompletionHandle"
        );

        // Worker must still be alive and process subsequent tasks.
        let (h2, rx2) = make_handle();
        pool.submit(h2, || Ok(b"pool_still_alive".to_vec()))
            .unwrap();
        let r2 = rx2.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(r2.unwrap(), b"pool_still_alive".to_vec());
    }

    // ── submit_to_saturated_pool_returns_pool_full ────────────────────────
    // Verify that submitting when the queue is full returns PoolError::Full
    // immediately (no blocking / timeout).
    #[test]
    fn submit_to_saturated_pool_returns_pool_full() {
        // 1 worker, queue depth 1: at most 2 tasks in flight (1 running + 1 queued).
        let pool = AsyncThreadPool::with_queue_size(1, 1);

        // Signal pair: lets the test know the worker is inside the first task.
        let (in_task_tx, in_task_rx) = mpsc::sync_channel::<()>(0);
        let (release_tx, release_rx) = mpsc::channel::<()>();

        let (h0, _rx0) = make_handle();
        pool.submit(h0, move || {
            // Signal: we are inside the task and about to block.
            let _ = in_task_tx.send(());
            // Block until the test releases us.
            let _ = release_rx.recv();
            Ok(vec![])
        })
        .expect("first submit must succeed on empty queue");

        // Wait until the worker is inside the first task (queue is now empty).
        in_task_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("worker should start the first task promptly");

        // Fill the single queue slot.
        let (h1, _rx1) = make_handle();
        pool.submit(h1, || Ok(vec![]))
            .expect("second submit must fit in the queue");

        // Queue is full; submitting another task must return PoolError::Full.
        let (h2, _rx2) = make_handle();
        let err = pool.submit(h2, || Ok(vec![]));
        assert!(
            matches!(err, Err(PoolError::Full)),
            "expected PoolError::Full when queue is saturated, got {err:?}"
        );

        // Release the worker so threads can clean up.
        let _ = release_tx.send(());
    }
}
