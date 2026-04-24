//! Automatic hot → cold tier demotion.
//!
//! A periodic event-loop timer checks whether the in-RAM [`FlashCache`] is over
//! capacity. When it is, the timer pulls insertion-ordered candidates from the
//! cache's eviction queue and moves each to NVMe cold tier through the same
//! pipeline that `FLASH.DEBUG.DEMOTE` uses.
//!
//! All tier-state mutations run on the event-loop thread — that is the only
//! context where `ctx.open_key_writable()` yields a safe `&mut Tier<T>`. The
//! NVMe write (`storage.put`) is synchronous and blocks that thread for the
//! duration of the write; a per-tick budget caps the worst-case stall.
//!
//! The scheduling pattern — boxed state passed through `create_timer`, the
//! callback re-arming itself at the end of each tick — mirrors
//! `cluster::prewarm_tick` at `src/cluster/mod.rs`.
//!
//! [`FlashCache`]: crate::storage::cache::FlashCache

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use valkey_module::{Context, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

use crate::storage::backend::StorageBackend;
use crate::storage::file_io_uring::FileIoUringBackend;
use crate::types::Tier;
use crate::types::hash::{FLASH_HASH_TYPE, FlashHashObject, hash_serialize};
use crate::types::list::{FLASH_LIST_TYPE, FlashListObject, list_serialize};
use crate::types::string::{FLASH_STRING_TYPE, FlashStringObject};
use crate::types::zset::{FLASH_ZSET_TYPE, FlashZSetObject, zset_serialize};
use crate::{CACHE, STORAGE, TIERING_MAP, WAL};

/// Tick period for the auto-demotion timer.
const TICK_INTERVAL: Duration = Duration::from_millis(100);

/// Maximum number of keys to demote in a single tick. Caps the worst-case
/// event-loop stall; each NVMe put is synchronous.
const MAX_DEMOTIONS_PER_TICK: usize = 128;

/// Start demoting when the hot-tier RAM cache reaches this fill fraction
/// (numerator over 100).  `FlashCache::approx_bytes()` now returns
/// quick_cache's native `weight()`, which the S3-FIFO policy maintains at or
/// below `capacity_bytes` at all times — strict overflow can never occur.
/// Triggering at 95% gives the demotion loop a chance to move entries to NVMe
/// cold tier before S3-FIFO has to discard them silently.
const DEMOTION_FILL_PCT: u64 = 95;

/// Set by `shutdown()` so an in-flight timer fires once, observes this, and
/// does not re-arm — preventing dlclose() from unloading the .so out from
/// under a pending timer callback.
static SHUTDOWN: AtomicBool = AtomicBool::new(false);

/// Total number of successful auto-demotions since module load. Exposed via
/// `INFO flash` as `flash_auto_demotions_total` so operators can verify the
/// loop is firing against their workload.
pub static AUTO_DEMOTIONS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Kick off the periodic auto-demotion timer. Call once from `initialize()`
/// after CACHE and STORAGE are set.
pub fn start(ctx: &Context) {
    SHUTDOWN.store(false, Ordering::Release);
    schedule(ctx, TICK_INTERVAL);
}

/// Signal the timer loop to stop re-arming. Call from `deinitialize()` before
/// joining background threads. Any pending timer will fire once more, see
/// the flag, and exit without scheduling another tick.
pub fn shutdown() {
    SHUTDOWN.store(true, Ordering::Release);
}

fn schedule(ctx: &Context, delay: Duration) {
    ctx.create_timer(delay, tick, ());
}

fn tick(ctx: &Context, _data: ()) {
    if SHUTDOWN.load(Ordering::Acquire) {
        return;
    }

    // Replicas apply writes from the primary directly to RAM without a local
    // NVMe copy; they should not demote on their own. `replication::is_replica()`
    // is toggled by the role-changed event handler, so this correctly tracks
    // primary ↔ replica transitions at runtime.
    if crate::replication::is_replica() {
        schedule(ctx, TICK_INTERVAL);
        return;
    }

    // Tolerate transient absence of cache/storage (e.g. RAM-only replica
    // before promotion). Re-arm and try again next tick.
    let (Some(cache), Some(storage)) = (CACHE.get(), STORAGE.get()) else {
        schedule(ctx, TICK_INTERVAL);
        return;
    };

    let mut demoted = 0usize;
    // `approx_bytes()` returns quick_cache's native `weight()`, which S3-FIFO
    // keeps ≤ capacity_bytes at all times.  Compare against a 95% fill
    // threshold so demotion fires proactively, before S3-FIFO is forced to
    // discard entries without an NVMe backup.
    while cache.approx_bytes().saturating_mul(100)
        >= cache.capacity_bytes().saturating_mul(DEMOTION_FILL_PCT)
        && demoted < MAX_DEMOTIONS_PER_TICK
    {
        let Some(key_bytes) = cache.evict_candidate() else {
            break;
        };
        if let Ok(true) = try_demote_key(ctx, storage, &key_bytes) {
            demoted += 1;
        }
    }

    if demoted > 0 {
        AUTO_DEMOTIONS_TOTAL.fetch_add(demoted as u64, Ordering::Relaxed);
    }

    schedule(ctx, TICK_INTERVAL);
}

/// Probe `key_bytes` as each of the four flash types in turn. On a match with a
/// hot-tier value, demote it to cold tier and return `Ok(true)`. Absent /
/// already-cold / non-flash-type keys return `Ok(false)` (the caller will try
/// the next candidate). Only an actual NVMe-write failure returns `Err`.
fn try_demote_key(
    ctx: &Context,
    storage: &FileIoUringBackend,
    key_bytes: &[u8],
) -> ValkeyResult<bool> {
    let key = ctx.create_string(key_bytes);
    let key_handle = ctx.open_key_writable(&key);

    if let Ok(Some(obj)) = key_handle.get_value::<FlashStringObject>(&FLASH_STRING_TYPE) {
        let bytes = match &obj.tier {
            Tier::Hot(v) => v.clone(),
            Tier::Cold { .. } => return Ok(false),
        };
        demote_bytes(&key, storage, &bytes, &mut obj.tier)?;
        ctx.notify_keyspace_event(NotifyEvent::GENERIC, "flash.evict", &key);
        return Ok(true);
    }

    if let Ok(Some(obj)) = key_handle.get_value::<FlashHashObject>(&FLASH_HASH_TYPE) {
        let bytes = match &obj.tier {
            Tier::Hot(fields) => hash_serialize(fields),
            Tier::Cold { .. } => return Ok(false),
        };
        demote_bytes(&key, storage, &bytes, &mut obj.tier)?;
        ctx.notify_keyspace_event(NotifyEvent::GENERIC, "flash.evict", &key);
        return Ok(true);
    }

    if let Ok(Some(obj)) = key_handle.get_value::<FlashListObject>(&FLASH_LIST_TYPE) {
        let bytes = match &obj.tier {
            Tier::Hot(items) => list_serialize(items),
            Tier::Cold { .. } => return Ok(false),
        };
        demote_bytes(&key, storage, &bytes, &mut obj.tier)?;
        ctx.notify_keyspace_event(NotifyEvent::GENERIC, "flash.evict", &key);
        return Ok(true);
    }

    if let Ok(Some(obj)) = key_handle.get_value::<FlashZSetObject>(&FLASH_ZSET_TYPE) {
        let bytes = match &obj.tier {
            Tier::Hot(inner) => zset_serialize(inner),
            Tier::Cold { .. } => return Ok(false),
        };
        demote_bytes(&key, storage, &bytes, &mut obj.tier)?;
        ctx.notify_keyspace_event(NotifyEvent::GENERIC, "flash.evict", &key);
        return Ok(true);
    }

    Ok(false)
}

/// Write `bytes` to NVMe, update tiering map + WAL, transition `tier` to
/// `Cold`, evict from the hot cache. Callers must already hold the writable
/// key handle for `key` so the mutation of `*tier` is safe.
///
/// Note: this currently re-writes the value even when the key already has a
/// durability-copy on NVMe (see `src/commands/hset.rs` write-through path).
/// The storage backend's `put()` reclaims the old blocks via the free-list, so
/// this is wasteful but not leaking — optimisation opportunity tracked
/// separately.
pub(crate) fn demote_bytes<T>(
    key: &ValkeyString,
    storage: &FileIoUringBackend,
    bytes: &[u8],
    tier: &mut Tier<T>,
) -> ValkeyResult {
    let key_bytes = key.as_slice();
    let kh = crate::util::key_hash(key_bytes);
    let vh = crate::util::value_hash(bytes);
    let num_blocks = FileIoUringBackend::blocks_needed(bytes.len());
    let value_len = bytes.len() as u32;

    let backend_offset = storage
        .put(key_bytes, bytes)
        .map_err(|e| ValkeyError::String(e.to_string()))?;

    storage.remove_from_index(key_bytes);

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
