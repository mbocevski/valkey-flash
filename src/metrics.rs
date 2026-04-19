use std::sync::atomic::Ordering;

use valkey_module::{InfoContext, ValkeyResult};

use crate::cluster::{
    IS_CLUSTER, MIGRATION_BYTES_RECEIVED, MIGRATION_BYTES_SENT, MIGRATION_ERRORS,
    MIGRATION_KEYS_MIGRATED, MIGRATION_KEYS_REJECTED, MIGRATION_LAST_DURATION_MS,
    MIGRATION_SLOTS_IN_PROGRESS,
};
use crate::config::FLASH_MIGRATION_BANDWIDTH_MBPS;
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

    // ── Migration progress ────────────────────────────────────────────────────
    let migration_slots = MIGRATION_SLOTS_IN_PROGRESS.load(Ordering::Relaxed).max(0);
    let migration_bytes_sent = MIGRATION_BYTES_SENT.load(Ordering::Relaxed);
    let migration_bytes_received = MIGRATION_BYTES_RECEIVED.load(Ordering::Relaxed);
    let migration_last_duration_ms = MIGRATION_LAST_DURATION_MS.load(Ordering::Relaxed);
    let migration_errors = MIGRATION_ERRORS.load(Ordering::Relaxed);
    let migration_bandwidth_mbps = FLASH_MIGRATION_BANDWIDTH_MBPS.load(Ordering::Relaxed);
    let migration_keys_migrated = MIGRATION_KEYS_MIGRATED.load(Ordering::Relaxed);
    let migration_keys_rejected = MIGRATION_KEYS_REJECTED.load(Ordering::Relaxed);

    ctx.builder()
        .add_section("flash")
        .field("cache_hits", hits.to_string())?
        .field("cache_misses", misses.to_string())?
        .field("cache_hit_ratio", hit_ratio)?
        .field("cache_size_bytes", cache_size.to_string())?
        .field("cache_capacity_bytes", cache_capacity.to_string())?
        .field("eviction_count", evictions.to_string())?
        .field("storage_used_bytes", storage_used.to_string())?
        .field("storage_free_bytes", storage_free.to_string())?
        .field("storage_capacity_bytes", storage_capacity.to_string())?
        .field("wal_size_bytes", wal_size.to_string())?
        .field("compaction_runs", compaction_runs.to_string())?
        .field(
            "compaction_bytes_reclaimed",
            compaction_bytes_reclaimed.to_string(),
        )?
        .field("tiered_keys", tiered_keys.to_string())?
        .field("module_state", module_state)?
        .field("cluster_mode", cluster_mode.to_string())?
        .field(
            "migration_slots_in_progress",
            migration_slots.to_string(),
        )?
        .field(
            "migration_bytes_sent",
            migration_bytes_sent.to_string(),
        )?
        .field(
            "migration_bytes_received",
            migration_bytes_received.to_string(),
        )?
        .field(
            "migration_last_duration_ms",
            migration_last_duration_ms.to_string(),
        )?
        .field("migration_errors", migration_errors.to_string())?
        .field(
            "migration_bandwidth_mbps",
            migration_bandwidth_mbps.to_string(),
        )?
        .field(
            "migration_keys_migrated",
            migration_keys_migrated.to_string(),
        )?
        .field(
            "migration_keys_rejected",
            migration_keys_rejected.to_string(),
        )?
        .build_section()?
        .build_info()?;

    Ok(())
}
