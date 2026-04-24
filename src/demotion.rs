//! Automatic hot → cold tier demotion.
//!
//! # Pipeline
//!
//! Demotion runs as a three-phase split so the NVMe write never blocks the
//! Valkey event loop. An event-loop timer drives phases 1 and 3; the async
//! I/O pool (`AsyncThreadPool`, same pool `FLASH.HSET` write-through uses)
//! drives phase 2.
//!
//! 1. **Phase 1 (event loop)** — the tick pops a candidate from
//!    `FlashCache::evict_candidate()`, opens the key writable, clones the hot
//!    payload, captures a value hash, and submits a `storage.alloc_and_write_cold`
//!    task to the pool. The key handle is dropped before phase 2 starts —
//!    the `&mut Tier<T>` borrow does not cross the async boundary.
//! 2. **Phase 2 (pool worker)** — `alloc_and_write_cold` allocates NVMe
//!    blocks and writes the payload. It does NOT touch the storage index;
//!    ownership of the blocks is carried in a `CompletedDemotion` record the
//!    worker pushes onto the commit queue.
//! 3. **Phase 3 (event loop, next tick)** — the tick drains the commit queue
//!    before submitting new work. For each completion it re-opens the key
//!    and race-checks against the phase-1 value hash: on a match it appends
//!    the WAL record, inserts into TIERING_MAP, transitions `obj.tier` to
//!    `Cold`, and evicts the hot cache entry. On a mismatch (client wrote
//!    during the in-flight window), key-deletion, or type-change, the
//!    newly-allocated blocks are released via `release_cold_blocks` and the
//!    candidate is silently dropped.
//!
//! # Why this shape
//!
//! Putting the NVMe write on the event loop coupled the demotion rate to
//! `storage.put` latency and blocked the loop for the duration of each
//! write. With the pipeline, phase 2 runs on pool workers — the event loop
//! only pays for phase-1 serialisation + submit and phase-3 tier mutation,
//! both measured in µs. Under sustained pressure the pool drains phase-2
//! work in parallel while client commands, cluster heartbeats, and other
//! timers continue to make progress.
//!
//! The tick is a good neighbour to client write-through. `AsyncThreadPool`
//! is shared with FLASH.SET / HSET / RPUSH / ZADD's durability writes, and
//! the pool's queue depth is `io_threads * 4`. To avoid starving clients,
//! the tick breaks its submit loop as soon as `pool.submit` returns
//! `PoolError::Full`, leaving room for client submits to drain the queue
//! before the next tick fires. `MAX_DEMOTIONS_PER_TICK` and `MAX_INFLIGHT`
//! are sized below typical `io_threads * 4` so a steady-state sweep rarely
//! fills the queue in the first place.
//!
//! # Crash-recovery notes
//!
//! Between phase 2 completing and phase 3 running there is a window (≤ one
//! tick interval, default 100 ms) where the NVMe payload exists but the WAL
//! record does not. On crash in this window the blocks are leaked — no data
//! loss (the Hot payload is still in Valkey's keyspace and recovers via
//! RDB/AOF), but the allocator's free-list does not reclaim the blocks until
//! a future compaction pass notices they're unreferenced. Bounded by
//! `MAX_INFLIGHT × block_size` ≈ a few MB per crash; acceptable for
//! production uptimes. A follow-up can track in-flight allocations via
//! `aux_save`.
//!
//! # Shutdown
//!
//! `shutdown()` flips an atomic; the next timer fires, sees it, and does not
//! re-arm. In-flight pool tasks may still complete and push onto the commit
//! queue but the queue is never drained again — this is fine because
//! `deinitialize()` shuts the pool down *after* `shutdown()`, so by the time
//! `dlclose` runs the worker threads are joined and the queue is effectively
//! frozen.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

use valkey_module::{Context, NotifyEvent};

use crate::async_io::{AsyncThreadPool, CompletionHandle, PoolError};
use crate::storage::backend::{StorageBackend, StorageError, StorageResult};
use crate::storage::file_io_uring::FileIoUringBackend;
use crate::types::Tier;
use crate::types::hash::{FLASH_HASH_TYPE, FlashHashObject, hash_serialize};
use crate::types::list::{FLASH_LIST_TYPE, FlashListObject, list_serialize};
use crate::types::string::{FLASH_STRING_TYPE, FlashStringObject};
use crate::types::zset::{FLASH_ZSET_TYPE, FlashZSetObject, zset_serialize};
use crate::{CACHE, POOL, STORAGE, TIERING_MAP, WAL};

// ── Tunables ─────────────────────────────────────────────────────────────────

/// Tick period for the auto-demotion timer.
const TICK_INTERVAL: Duration = Duration::from_millis(100);

/// Demote when the hot-tier RAM cache is at or above this fill fraction
/// (numerator over 100). Lowered from 95 % because S3-FIFO's auto-eviction
/// waterline varies with value size: at multi-KiB values the cache steadies
/// out at ~94 % as S3-FIFO evicts on every new insert, so a 95 % threshold
/// was never actually crossed and demotion silently didn't fire. 90 % is
/// comfortably below the observed steady-state waterline across value sizes
/// while still requiring meaningful cache pressure before demotion starts.
const DEMOTION_FILL_PCT: u64 = 90;

/// Supplementary trigger: demote even if fill is below the fill threshold
/// when S3-FIFO is actively evicting — any delta > 0 in `eviction_count`
/// since the last tick indicates the cache is losing entries without NVMe
/// backup, which is exactly when we want to catch up. This closes the
/// failure mode where dense value encodings hold the cache at 89.9 %
/// steady-state: demotion fires on the eviction signal instead.
static LAST_EVICTION_COUNT: AtomicU64 = AtomicU64::new(0);

/// Target upper bound on phase-1 event-loop stall per tick, in microseconds.
/// When a tick's submit loop runs longer than this, the adaptive batch cap
/// halves on the next tick to keep client command latency stable. When ticks
/// complete well under budget and cache pressure persists, the cap grows
/// additively. See `EFFECTIVE_BATCH_OVERRIDE`.
const STALL_BUDGET_US: u64 = 2_000;

/// Adaptive batch cap, shrunk from the configured ceiling when the tick
/// exceeds the stall budget. `0` means "no adaptive clamp — use the
/// `flash.demotion-batch` setting directly". A non-zero value is the
/// current per-tick ceiling and is always ≤ the configured setting.
///
/// The cap follows an additive-increase / multiplicative-decrease pattern:
/// on an over-budget tick it halves (fast backoff under pressure), and on
/// a cap-hitting tick that still had cache pressure it increments by 25 %
/// (slow recovery toward the configured ceiling).
pub static EFFECTIVE_BATCH_OVERRIDE: AtomicUsize = AtomicUsize::new(0);

// The per-tick demotion budget and in-flight cap are read from the
// `flash.demotion-batch` and `flash.demotion-max-inflight` configs on every
// tick. Both are mutable via `CONFIG SET` so operators can tune pool
// contention against client write-through without restarting the module.
// Default behaviour (knob = 0) auto-sizes from `flash.io-threads` to leave
// pool headroom for client writes. See `src/config.rs` for the formulas.

// ── Module-level state ───────────────────────────────────────────────────────

/// Set by `shutdown()` so an in-flight timer fires once, observes this, and
/// does not re-arm — preventing `dlclose()` from unloading the .so out from
/// under a pending timer callback.
static SHUTDOWN: AtomicBool = AtomicBool::new(false);

/// Total number of successful auto-demotions since module load. Exposed via
/// `INFO flash` as `flash_auto_demotions_total`.
pub static AUTO_DEMOTIONS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Current number of in-flight demotions (submitted to the pool but not yet
/// committed via phase 3). Exposed via `INFO flash` as
/// `flash_auto_demotions_inflight` for back-pressure monitoring.
pub static INFLIGHT: AtomicU64 = AtomicU64::new(0);

/// Wall-clock duration of the most recent tick's phase-1 work, in µs.
/// Exposed via `INFO flash` as `flash_demotion_tick_last_us`. Bench
/// harnesses sample this at their polling interval to reconstruct the
/// distribution without needing an on-module histogram.
pub static TICK_LAST_PHASE1_US: AtomicU64 = AtomicU64::new(0);

/// Cumulative number of ticks whose phase-1 wall time exceeded the
/// `STALL_BUDGET_US` threshold. Exposed via `INFO flash` as
/// `flash_demotion_stall_events_total` so operators can verify the
/// adaptive AIMD controller is (or isn't) kicking in against real load.
pub static TICK_OVER_BUDGET_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Queue of completions handed from pool workers back to the event-loop
/// commit phase. The tick drains this at the top of each iteration.
static COMMIT_QUEUE: LazyLock<Mutex<VecDeque<CompletedDemotion>>> =
    LazyLock::new(|| Mutex::new(VecDeque::new()));

/// Which Valkey flash type was observed at phase 1 — phase 3 dispatches on
/// this to probe the same type for the race check. Copy so the closure and
/// the handle can both capture it.
#[derive(Clone, Copy, Debug)]
enum FlashTypeTag {
    String,
    Hash,
    List,
    Zset,
}

/// Everything phase 3 needs to re-open the key, race-check the Hot payload,
/// and either commit the transition to Cold or roll back the NVMe blocks.
struct CompletedDemotion {
    key_bytes: Vec<u8>,
    tag: FlashTypeTag,
    phase1_value_hash: u64,
    backend_offset: u64,
    num_blocks: u32,
    value_len: u32,
}

// ── Public lifecycle ─────────────────────────────────────────────────────────

/// Kick off the periodic auto-demotion timer. Call once from `initialize()`
/// (and from `init_nvme_backend` on replica→primary promotion) after CACHE,
/// STORAGE, and POOL are set.
pub fn start(ctx: &Context) {
    SHUTDOWN.store(false, Ordering::Release);
    schedule(ctx, TICK_INTERVAL);
}

/// Signal the timer loop to stop re-arming. Call from `deinitialize()` before
/// the async pool is shut down.
pub fn shutdown() {
    SHUTDOWN.store(true, Ordering::Release);
}

/// Commit all pending demotion completions synchronously. Call from
/// `aux_save` BEFORE the fork so the snapshot captures a consistent view of
/// tier state — any `COMMIT_QUEUE` contents represent NVMe-written payloads
/// whose `Tier::Cold` transition hasn't happened yet, and if left uncommitted
/// across a fork the blocks would be orphaned on crash-recovery (the RDB
/// records the keys as still `Hot` in Valkey's keyspace while the NVMe file
/// has data at offsets not referenced by any `Tier::Cold` marker).
///
/// Callable only from the event-loop thread — internally opens writable key
/// handles to complete phase 3 of the demotion pipeline. Safe to call
/// alongside `FLASH.DEBUG.DEMOTE` or other event-loop commands.
///
/// Residual leak window: this does not drain `INFLIGHT` (demotions whose
/// pool worker hasn't yet pushed to `COMMIT_QUEUE`). Bounded by the pool's
/// per-task latency (µs) × number of in-flight demotions; much smaller than
/// the queued window this call closes.
pub fn drain_for_persistence(ctx: &Context) {
    if let Some(storage) = STORAGE.get() {
        drain_commit_queue(ctx, storage);
    }
}

fn schedule(ctx: &Context, delay: Duration) {
    ctx.create_timer(delay, tick, ());
}

// ── Tick ─────────────────────────────────────────────────────────────────────

fn tick(ctx: &Context, _data: ()) {
    if SHUTDOWN.load(Ordering::Acquire) {
        return;
    }
    if crate::replication::is_replica() {
        schedule(ctx, TICK_INTERVAL);
        return;
    }

    let (Some(cache), Some(storage), Some(pool)) = (CACHE.get(), STORAGE.get(), POOL.get()) else {
        schedule(ctx, TICK_INTERVAL);
        return;
    };

    // Phase 3 first: drain completions from earlier ticks so their memory
    // is reclaimed before we potentially submit more work.
    drain_commit_queue(ctx, storage);

    // Phase 1: submit new demotions up to per-tick + global in-flight caps.
    // On `PoolError::Full` we exit the loop immediately — the pool is shared
    // with FLASH.SET / HSET / RPUSH / ZADD write-through, and continuing
    // would starve client writes. Next tick retries after the pool drains.
    //
    // Trigger = fill ≥ threshold OR S3-FIFO has evicted at least one entry
    // since the last tick. The eviction delta catches workloads whose value
    // encoding keeps cache fill steady just below the fill threshold while
    // S3-FIFO churns underneath — without this, demotion silently skips
    // them even though Valkey-keyspace RAM is growing as Hot payloads
    // accumulate.
    let current_evictions = cache.metrics().evictions;
    let last_evictions = LAST_EVICTION_COUNT.swap(current_evictions, Ordering::Relaxed);
    let evictions_since_last_tick = current_evictions.saturating_sub(last_evictions);
    let fill_pressure = cache.approx_bytes().saturating_mul(100)
        >= cache.capacity_bytes().saturating_mul(DEMOTION_FILL_PCT);
    let pressure = fill_pressure || evictions_since_last_tick > 0;

    // Resolve the effective batch cap. `configured` is the user-visible
    // ceiling (the `flash.demotion-batch` knob); `override_cap` is the
    // adaptive clamp applied when the previous tick ran over budget.
    // Effective cap = min(configured, override_cap) if an override is set,
    // else just configured.
    let configured_batch = crate::config::flash_demotion_batch();
    let override_cap = EFFECTIVE_BATCH_OVERRIDE.load(Ordering::Relaxed);
    let batch = if override_cap == 0 {
        configured_batch
    } else {
        override_cap.min(configured_batch)
    };

    let max_inflight = crate::config::flash_demotion_max_inflight();
    let mut submitted = 0usize;
    let mut pool_full = false;

    let phase1_start = Instant::now();
    while pressure && submitted < batch && INFLIGHT.load(Ordering::Relaxed) < max_inflight {
        let Some(key_bytes) = cache.evict_candidate() else {
            break;
        };
        match submit_async_demotion(ctx, pool, &key_bytes) {
            SubmitOutcome::Submitted => submitted += 1,
            SubmitOutcome::Skipped => continue,
            SubmitOutcome::PoolFull => {
                pool_full = true;
                break;
            }
        }
    }
    let phase1_us = phase1_start.elapsed().as_micros() as u64;
    TICK_LAST_PHASE1_US.store(phase1_us, Ordering::Relaxed);
    if phase1_us > STALL_BUDGET_US {
        TICK_OVER_BUDGET_TOTAL.fetch_add(1, Ordering::Relaxed);
    }

    // Adaptive AIMD: see `aimd_next_override` for the pure state-transition
    // logic. Skip adjustment when the pool pushed back (pool_full) — the
    // bottleneck there isn't the event loop, so neither shrinking nor
    // growing would help.
    if !pool_full {
        let hit_cap = submitted == batch && pressure;
        let next_store = aimd_next_override(batch, configured_batch, phase1_us, hit_cap);
        if next_store != override_cap {
            EFFECTIVE_BATCH_OVERRIDE.store(next_store, Ordering::Relaxed);
        }
    }

    schedule(ctx, TICK_INTERVAL);
}

/// Pure AIMD state-transition for the adaptive batch override.
///
/// Inputs describe the tick that just ran:
///   - `batch`: the effective cap used this tick (already the min of
///     `configured` and any prior override)
///   - `configured`: the user-visible `flash.demotion-batch` ceiling
///   - `phase1_us`: measured phase-1 wall time, µs
///   - `hit_cap_with_pressure`: `true` when the submit loop hit `batch`
///     and the cache trigger condition was still true (i.e. more keys
///     wanted to be demoted than the cap allowed)
///
/// Returns the new value to store in `EFFECTIVE_BATCH_OVERRIDE`.
///   - `0` means "no clamp — use `configured` directly". Stored when the
///     adaptive cap has recovered back to (or above) the configured
///     ceiling, so steady-state observation is clean.
///   - Any non-zero value is the new cap, always strictly less than
///     `configured`.
fn aimd_next_override(
    batch: usize,
    configured: usize,
    phase1_us: u64,
    hit_cap_with_pressure: bool,
) -> usize {
    let step = (batch / 4).max(1);
    let next = if phase1_us > STALL_BUDGET_US {
        // Multiplicative decrease: halve on over-budget tick.
        (batch / 2).max(1)
    } else if hit_cap_with_pressure && batch < configured {
        // Additive increase: cap was binding and pressure persists.
        (batch + step).min(configured)
    } else if phase1_us * 2 < STALL_BUDGET_US && batch < configured {
        // Plenty of headroom and no stall — lift toward the ceiling.
        (batch + step).min(configured)
    } else {
        batch
    };
    if next >= configured { 0 } else { next }
}

// ── Phase 1 (event loop): prepare + submit ───────────────────────────────────

/// Tri-state outcome from `submit_async_demotion`. The tick uses it to
/// distinguish "keep trying the next candidate" (Skipped) from "pool is
/// saturated, back off for this tick" (PoolFull) — the latter protects
/// client write-through from starvation.
enum SubmitOutcome {
    Submitted,
    Skipped,
    PoolFull,
}

/// Probe the key, clone its hot payload, submit an NVMe-write task.
///
/// - `Submitted` — task is now in the pool; INFLIGHT was incremented.
/// - `Skipped` — key was missing / already cold / not a flash type. Caller
///   should continue with the next candidate.
/// - `PoolFull` — pool queue is saturated. Caller should exit the tick's
///   submit loop so client writes can drain the queue before the next tick.
fn submit_async_demotion(ctx: &Context, pool: &AsyncThreadPool, key_bytes: &[u8]) -> SubmitOutcome {
    let key = ctx.create_string(key_bytes);
    let key_handle = ctx.open_key_writable(&key);

    let (serialized, tag) =
        if let Ok(Some(obj)) = key_handle.get_value::<FlashStringObject>(&FLASH_STRING_TYPE) {
            match &obj.tier {
                Tier::Hot(v) => (v.clone(), FlashTypeTag::String),
                Tier::Cold { .. } => return SubmitOutcome::Skipped,
            }
        } else if let Ok(Some(obj)) = key_handle.get_value::<FlashHashObject>(&FLASH_HASH_TYPE) {
            match &obj.tier {
                Tier::Hot(fields) => (hash_serialize(fields), FlashTypeTag::Hash),
                Tier::Cold { .. } => return SubmitOutcome::Skipped,
            }
        } else if let Ok(Some(obj)) = key_handle.get_value::<FlashListObject>(&FLASH_LIST_TYPE) {
            match &obj.tier {
                Tier::Hot(items) => (list_serialize(items), FlashTypeTag::List),
                Tier::Cold { .. } => return SubmitOutcome::Skipped,
            }
        } else if let Ok(Some(obj)) = key_handle.get_value::<FlashZSetObject>(&FLASH_ZSET_TYPE) {
            match &obj.tier {
                Tier::Hot(inner) => (zset_serialize(inner), FlashTypeTag::Zset),
                Tier::Cold { .. } => return SubmitOutcome::Skipped,
            }
        } else {
            return SubmitOutcome::Skipped;
        };
    drop(key_handle);

    let phase1_hash = crate::util::value_hash(&serialized);
    let key_vec = key_bytes.to_vec();

    // Reserve a slot in the in-flight budget before submitting so the counter
    // accurately reflects "work the pool is obligated to process or we've
    // rolled back". If submit fails we release the slot immediately.
    INFLIGHT.fetch_add(1, Ordering::Relaxed);

    // Closure captures the owned payload + key + metadata by move; the
    // handle holds just enough context for the completion callback to
    // decrement the in-flight counter.
    let handle = Box::new(DemotePoolHandle);
    let task_key_bytes = key_vec;
    let task_serialized = serialized;
    let submit_result = pool.submit(handle, move || {
        let storage = STORAGE
            .get()
            .ok_or_else(|| StorageError::Other("storage closed during async demotion".into()))?;
        let (offset, num_blocks) = storage.alloc_and_write_cold(&task_serialized)?;
        let value_len = task_serialized.len() as u32;

        let completion = CompletedDemotion {
            key_bytes: task_key_bytes,
            tag,
            phase1_value_hash: phase1_hash,
            backend_offset: offset,
            num_blocks,
            value_len,
        };
        if let Ok(mut q) = COMMIT_QUEUE.lock() {
            q.push_back(completion);
        } else {
            // Queue poisoned — roll back the NVMe blocks so they don't leak.
            storage.release_cold_blocks(offset, num_blocks);
            return Err(StorageError::Other("demotion commit queue poisoned".into()));
        }
        Ok(Vec::new())
    });

    match submit_result {
        Ok(()) => SubmitOutcome::Submitted,
        Err(PoolError::Full) => {
            // Release the slot we reserved and signal the caller to stop
            // submitting this tick — the pool is saturated by either our
            // own prior submits or concurrent client write-through.
            INFLIGHT.fetch_sub(1, Ordering::Relaxed);
            SubmitOutcome::PoolFull
        }
        Err(PoolError::Closed) => {
            // Pool is shutting down. Release the slot; demotion won't
            // resume until re-init (e.g. after module reload).
            INFLIGHT.fetch_sub(1, Ordering::Relaxed);
            SubmitOutcome::Skipped
        }
    }
}

/// Minimal completion handle: all real work happens in the task closure
/// (pushing to COMMIT_QUEUE). This callback exists so the pool has a handle
/// type to call `complete()` on — it just decrements the in-flight counter
/// and logs any task-level error. The in-flight counter is released here
/// rather than inside the task so it always fires exactly once per submit
/// regardless of task success/failure/panic.
struct DemotePoolHandle;

impl CompletionHandle for DemotePoolHandle {
    fn complete(self: Box<Self>, result: StorageResult<Vec<u8>>) {
        INFLIGHT.fetch_sub(1, Ordering::Relaxed);
        if let Err(e) = result {
            valkey_module::logging::log_warning(
                format!("flash: demotion pool task failed: {e}").as_str(),
            );
        }
    }
}

// ── Phase 3 (event loop): drain + commit ─────────────────────────────────────

fn drain_commit_queue(ctx: &Context, storage: &FileIoUringBackend) {
    // Take the whole queue in one swap so we hold the lock minimally.
    let completions: VecDeque<CompletedDemotion> = match COMMIT_QUEUE.lock() {
        Ok(mut q) => std::mem::take(&mut *q),
        Err(_) => return, // poisoned — nothing we can safely do
    };
    let mut committed = 0usize;
    for c in completions {
        if commit_one(ctx, storage, &c) {
            committed += 1;
        } else {
            // Race-check failed / key gone / wrong type — release the NVMe
            // blocks we allocated in phase 2 so they don't leak.
            storage.release_cold_blocks(c.backend_offset, c.num_blocks);
        }
    }
    if committed > 0 {
        AUTO_DEMOTIONS_TOTAL.fetch_add(committed as u64, Ordering::Relaxed);
    }
}

/// Attempt to transition `c.key_bytes` from Hot to Cold. Returns `true` on
/// successful commit; `false` means the race check failed (the caller must
/// release the NVMe blocks).
fn commit_one(ctx: &Context, _storage: &FileIoUringBackend, c: &CompletedDemotion) -> bool {
    let key = ctx.create_string(c.key_bytes.as_slice());
    let key_handle = ctx.open_key_writable(&key);

    match c.tag {
        FlashTypeTag::String => commit_string(ctx, &key_handle, &key, c),
        FlashTypeTag::Hash => commit_hash(ctx, &key_handle, &key, c),
        FlashTypeTag::List => commit_list(ctx, &key_handle, &key, c),
        FlashTypeTag::Zset => commit_zset(ctx, &key_handle, &key, c),
    }
}

macro_rules! commit_type {
    ($fn_name:ident, $obj_ty:ty, $type_handle:expr, $serialize:expr) => {
        fn $fn_name(
            ctx: &Context,
            key_handle: &valkey_module::key::ValkeyKeyWritable,
            key: &valkey_module::ValkeyString,
            c: &CompletedDemotion,
        ) -> bool {
            let Ok(Some(obj)) = key_handle.get_value::<$obj_ty>(&$type_handle) else {
                return false; // key missing or wrong type now
            };
            let current_bytes = match &obj.tier {
                Tier::Hot(payload) => $serialize(payload),
                Tier::Cold { .. } => return false, // already cold (unexpected — discard)
            };
            if crate::util::value_hash(&current_bytes) != c.phase1_value_hash {
                return false; // client wrote during the in-flight window
            }

            // Race check passed: commit the transition.
            let key_hash = crate::util::key_hash(&c.key_bytes);
            if let Some(wal) = WAL.get() {
                let _ = wal.append(crate::storage::wal::WalOp::Put {
                    key_hash,
                    offset: c.backend_offset,
                    value_hash: c.phase1_value_hash,
                });
            }
            if let Ok(mut map) = TIERING_MAP.lock() {
                map.insert(
                    key_hash,
                    crate::recovery::TierEntry {
                        offset: c.backend_offset,
                        value_hash: c.phase1_value_hash,
                    },
                );
            }
            obj.tier = Tier::Cold {
                key_hash,
                backend_offset: c.backend_offset,
                num_blocks: c.num_blocks,
                value_len: c.value_len,
            };
            if let Some(cache) = CACHE.get() {
                cache.delete(key.as_slice());
            }
            // Reclaim the durability-write index entry left behind by the
            // write-through NVMe copy (from FLASH.SET / HSET / RPUSH / ZADD).
            // `alloc_and_write_cold` bypasses the index, so the stale entry
            // would leak without this step — unlike the sync path's
            // `storage.put` which overwrote the index and freed old blocks
            // automatically. `storage.delete` is a no-op for keys that
            // never had a durability entry (e.g. synthetic debug demotions).
            if let Some(storage) = STORAGE.get() {
                let _ = storage.delete(&c.key_bytes);
            }
            ctx.notify_keyspace_event(NotifyEvent::GENERIC, "flash.evict", key);
            true
        }
    };
}

// A tiny helper so string's "serialize" (identity copy) has the same callable
// shape as the other three types' serializers. Takes a byte slice and returns
// an owned Vec so the macro can call it uniformly against whatever the
// corresponding Tier::Hot variant holds.
fn string_serialize(v: &[u8]) -> Vec<u8> {
    v.to_vec()
}

commit_type!(
    commit_string,
    FlashStringObject,
    FLASH_STRING_TYPE,
    string_serialize
);
commit_type!(
    commit_hash,
    FlashHashObject,
    FLASH_HASH_TYPE,
    hash_serialize
);
commit_type!(
    commit_list,
    FlashListObject,
    FLASH_LIST_TYPE,
    list_serialize
);
commit_type!(
    commit_zset,
    FlashZSetObject,
    FLASH_ZSET_TYPE,
    zset_serialize
);

// ── Synchronous demote_bytes (retained for FLASH.DEBUG.DEMOTE) ───────────────

use valkey_module::{ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

/// Synchronously write `bytes` to NVMe, update tiering map + WAL, transition
/// `tier` to `Cold`, and evict the key from the hot cache. Callers must hold
/// the writable key handle for `key`.
///
/// Used by `FLASH.DEBUG.DEMOTE` for deterministic test paths. The production
/// auto-demotion pipeline is async (see the module docstring); this helper
/// stays for test-only admin use where immediate completion matters more than
/// event-loop responsiveness.
pub(crate) fn demote_bytes<T>(
    key: &ValkeyString,
    storage: &FileIoUringBackend,
    bytes: &[u8],
    tier: &mut Tier<T>,
) -> ValkeyResult {
    let key_bytes = key.as_slice();
    let kh = crate::util::key_hash(key_bytes);
    let vh = crate::util::value_hash(bytes);

    let (backend_offset, num_blocks) = storage
        .alloc_and_write_cold(bytes)
        .map_err(|e| ValkeyError::String(e.to_string()))?;
    let value_len = bytes.len() as u32;

    if let Some(wal) = WAL.get() {
        let _ = wal.append(crate::storage::wal::WalOp::Put {
            key_hash: kh,
            offset: backend_offset,
            value_hash: vh,
        });
    }

    if let Ok(mut map) = TIERING_MAP.lock() {
        map.insert(
            kh,
            crate::recovery::TierEntry {
                offset: backend_offset,
                value_hash: vh,
            },
        );
    }

    *tier = Tier::Cold {
        key_hash: kh,
        backend_offset,
        num_blocks,
        value_len,
    };

    if let Some(cache) = CACHE.get() {
        cache.delete(key.as_slice());
    }

    Ok(ValkeyValue::SimpleStringStatic("OK"))
}

// ── Unit tests ───────────────────────────────────────────────────────────────

#[cfg(all(test, not(loom)))]
mod tests {
    use super::{STALL_BUDGET_US, aimd_next_override};

    // Stall budget is 2_000 µs; use constants that straddle it to exercise
    // each branch unambiguously.
    const OVER: u64 = STALL_BUDGET_US + 1;
    const UNDER_HALF: u64 = STALL_BUDGET_US / 4;
    const AT_HALF: u64 = STALL_BUDGET_US - 1; // over-half-budget but below stall

    #[test]
    fn over_budget_tick_halves_cap() {
        // batch 32, stall — halve to 16. Below configured 64 → returns 16.
        assert_eq!(aimd_next_override(32, 64, OVER, false), 16);
        // batch 32 halves to 16 even when the tick hit its cap with pressure;
        // multiplicative decrease wins over additive increase.
        assert_eq!(aimd_next_override(32, 64, OVER, true), 16);
    }

    #[test]
    fn over_budget_never_below_one() {
        // batch 1 halves to max(0, 1) = 1. Still below configured 64 → 1.
        assert_eq!(aimd_next_override(1, 64, OVER, true), 1);
    }

    #[test]
    fn hit_cap_with_pressure_grows_additively() {
        // batch 32 hit cap with pressure + plenty of headroom: grow by 25 %
        // (step = max(8, 1) = 8) toward configured 64 → 40.
        assert_eq!(aimd_next_override(32, 64, UNDER_HALF, true), 40);
    }

    #[test]
    fn idle_headroom_also_grows() {
        // Same batch 32 + configured 64 but no cap-hit: the idle-headroom
        // branch still lifts toward the ceiling so recovery proceeds even
        // when current cap isn't binding.
        assert_eq!(aimd_next_override(32, 64, UNDER_HALF, false), 40);
    }

    #[test]
    fn grow_clamps_at_configured_returns_zero() {
        // Growing past configured returns 0 — sentinel for "no clamp active,
        // use the configured ceiling directly".
        assert_eq!(aimd_next_override(60, 64, UNDER_HALF, true), 0);
    }

    #[test]
    fn mid_budget_not_idle_holds_steady() {
        // Phase-1 runtime above half the stall budget, no cap-hit: neither
        // branch fires, cap stays at 32. Not at configured → return 32.
        assert_eq!(aimd_next_override(32, 64, AT_HALF, false), 32);
    }

    #[test]
    fn at_configured_stays_at_sentinel() {
        // Already at configured ceiling under any calm-tick input: returns 0.
        assert_eq!(aimd_next_override(64, 64, UNDER_HALF, false), 0);
        assert_eq!(aimd_next_override(64, 64, UNDER_HALF, true), 0);
        assert_eq!(aimd_next_override(64, 64, AT_HALF, false), 0);
    }
}
