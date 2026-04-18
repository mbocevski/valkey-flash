use std::sync::atomic::Ordering;

use valkey_module::{InfoContext, ValkeyResult};

use crate::cluster::IS_CLUSTER;
use crate::storage::file_io_uring::{BYTES_RECLAIMED, COMPACTION_RUNS};
use crate::{CACHE, MODULE_STATE, STORAGE, TIERING_MAP, WAL};

pub fn flash_info_handler(ctx: &InfoContext) -> ValkeyResult<()> {
    // ── Cache metrics ─────────────────────────────────────────────────────────
    let (hits, misses, evictions, cache_size, cache_capacity) = CACHE
        .get()
        .map(|c| {
            let m = c.metrics();
            (
                m.hits,
                m.misses,
                m.evictions,
                c.approx_bytes(),
                c.capacity_bytes(),
            )
        })
        .unwrap_or((0, 0, 0, 0, 0));

    let total = hits + misses;
    let hit_ratio = if total > 0 {
        format!("{:.4}", hits as f64 / total as f64)
    } else {
        "0.0000".to_string()
    };

    // ── Storage metrics ───────────────────────────────────────────────────────
    let (storage_used, storage_free, storage_capacity) = STORAGE
        .get()
        .map(|s| {
            const BLOCK: u64 = 4096;
            let used = s.next_block_snapshot() * BLOCK;
            let free = s.free_block_count() * BLOCK;
            let cap = s.capacity_bytes();
            (used, free, cap)
        })
        .unwrap_or((0, 0, 0));

    // ── WAL size ──────────────────────────────────────────────────────────────
    let wal_size = WAL.get().and_then(|w| w.current_offset().ok()).unwrap_or(0);

    // ── Compaction counters ───────────────────────────────────────────────────
    let compaction_runs = COMPACTION_RUNS.load(Ordering::Relaxed);
    let compaction_bytes_reclaimed = BYTES_RECLAIMED.load(Ordering::Relaxed);

    // ── Tiering map ───────────────────────────────────────────────────────────
    let tiered_keys = TIERING_MAP.lock().map(|m| m.len() as u64).unwrap_or(0);

    // ── Module state ──────────────────────────────────────────────────────────
    let module_state = MODULE_STATE
        .lock()
        .map(|s| format!("{s:?}").to_lowercase())
        .unwrap_or_else(|_| "error".to_string());

    // ── Cluster mode ──────────────────────────────────────────────────────────
    let cluster_mode = if IS_CLUSTER.load(Ordering::Acquire) {
        "yes"
    } else {
        "no"
    };

    ctx.builder()
        .add_section("flash")
        .field("flash_cache_hits", hits.to_string())?
        .field("flash_cache_misses", misses.to_string())?
        .field("flash_cache_hit_ratio", hit_ratio)?
        .field("flash_cache_size_bytes", cache_size.to_string())?
        .field("flash_cache_capacity_bytes", cache_capacity.to_string())?
        .field("flash_eviction_count", evictions.to_string())?
        .field("flash_storage_used_bytes", storage_used.to_string())?
        .field("flash_storage_free_bytes", storage_free.to_string())?
        .field("flash_storage_capacity_bytes", storage_capacity.to_string())?
        .field("flash_wal_size_bytes", wal_size.to_string())?
        .field("flash_compaction_runs", compaction_runs.to_string())?
        .field(
            "flash_compaction_bytes_reclaimed",
            compaction_bytes_reclaimed.to_string(),
        )?
        .field("flash_tiered_keys", tiered_keys.to_string())?
        .field("flash_module_state", module_state)?
        .field("flash_cluster_mode", cluster_mode.to_string())?
        .build_section()?
        .build_info()?;

    Ok(())
}
