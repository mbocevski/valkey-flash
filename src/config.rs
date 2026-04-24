use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{LazyLock, Mutex};
use valkey_module::enum_configuration;

// ── SyncMode ──────────────────────────────────────────────────────────────────

enum_configuration! {
    #[derive(PartialEq, Debug)]
    #[allow(non_camel_case_types)]
    pub enum SyncMode {
        always = 0,
        everysec = 1,
        no = 2,
    }
}

// ── flash.path ────────────────────────────────────────────────────────────────

pub const FLASH_PATH_DEFAULT: &str = "/tmp/valkey-flash.bin";

/// Backing store path. Immutable after module load (`ConfigurationFlags::IMMUTABLE`).
/// Access via `.lock().map_err(|e| StorageError::Other(e.to_string()))`.
pub static FLASH_PATH: LazyLock<Mutex<String>> =
    LazyLock::new(|| Mutex::new(FLASH_PATH_DEFAULT.to_string()));

// ── flash.capacity-bytes ──────────────────────────────────────────────────────

pub const FLASH_CAPACITY_BYTES_DEFAULT: i64 = 1 << 30; // 1 GiB
pub const FLASH_CAPACITY_BYTES_MIN: i64 = 1 << 20; // 1 MiB — below this Valkey rejects CONFIG SET
pub const FLASH_CAPACITY_BYTES_MAX: i64 = i64::MAX;

/// Total NVMe backing file size. Immutable after module load.
pub static FLASH_CAPACITY_BYTES: AtomicI64 = AtomicI64::new(FLASH_CAPACITY_BYTES_DEFAULT);

// ── flash.cache-size-bytes ────────────────────────────────────────────────────

pub const FLASH_CACHE_SIZE_BYTES_DEFAULT: i64 = 1 << 28; // 256 MiB
pub const FLASH_CACHE_SIZE_BYTES_MIN: i64 = 1 << 20; // 1 MiB — below this Valkey rejects CONFIG SET
pub const FLASH_CACHE_SIZE_BYTES_MAX: i64 = i64::MAX;

/// In-memory cache capacity. Mutable via CONFIG SET.
pub static FLASH_CACHE_SIZE_BYTES: AtomicI64 = AtomicI64::new(FLASH_CACHE_SIZE_BYTES_DEFAULT);

// ── flash.sync ────────────────────────────────────────────────────────────────

/// Write-sync policy. Mutable via CONFIG SET.
/// Access via `.lock().map_err(|e| StorageError::Other(e.to_string()))`.
pub static FLASH_SYNC: LazyLock<Mutex<SyncMode>> = LazyLock::new(|| Mutex::new(SyncMode::everysec));

// ── flash.io-threads ─────────────────────────────────────────────────────────

// Default 0 = auto (num_cpus::get()). initialize() stores the actual CPU count
// when the value is still 0, so accessors can always rely on a non-zero value
// after module load.
pub const FLASH_IO_THREADS_DEFAULT: i64 = 0;
pub const FLASH_IO_THREADS_MIN: i64 = 0; // 0 = auto is valid at load time
pub const FLASH_IO_THREADS_MAX: i64 = 256; // above this Valkey rejects CONFIG SET

/// Worker thread count. Immutable after module load. 0 is replaced by
/// `num_cpus::get()` inside `initialize()`.
pub static FLASH_IO_THREADS: AtomicI64 = AtomicI64::new(FLASH_IO_THREADS_DEFAULT);

/// Returns the effective I/O thread count.
/// - After `initialize()` this is always ≥ 1.
/// - In unit tests (no server, no `initialize()`), 0 maps to `num_cpus::get()`.
pub fn flash_io_threads() -> usize {
    let v = FLASH_IO_THREADS.load(Ordering::Relaxed);
    if v == 0 { num_cpus::get() } else { v as usize }
}

// ── flash.io-uring-entries ────────────────────────────────────────────────────

pub const FLASH_IO_URING_ENTRIES_DEFAULT: i64 = 256;
pub const FLASH_IO_URING_ENTRIES_MIN: i64 = 16;
pub const FLASH_IO_URING_ENTRIES_MAX: i64 = 65536;

/// io_uring submission-queue depth. Immutable after module load.
pub static FLASH_IO_URING_ENTRIES: AtomicI64 = AtomicI64::new(FLASH_IO_URING_ENTRIES_DEFAULT);

// ── flash.compaction-interval-sec ────────────────────────────────────────────

pub const FLASH_COMPACTION_INTERVAL_SEC_DEFAULT: i64 = 60;
pub const FLASH_COMPACTION_INTERVAL_SEC_MIN: i64 = 1;
pub const FLASH_COMPACTION_INTERVAL_SEC_MAX: i64 = 3600;

/// How often the background compaction thread runs (seconds). Mutable via CONFIG SET.
pub static FLASH_COMPACTION_INTERVAL_SEC: AtomicI64 =
    AtomicI64::new(FLASH_COMPACTION_INTERVAL_SEC_DEFAULT);

// ── flash.demotion-batch ──────────────────────────────────────────────────────
//
// Maximum number of demotions `demotion::tick` submits per invocation. Bounds
// phase-1 event-loop work and — critically — the burst rate at which demotion
// occupies the shared `AsyncThreadPool` queue (also used by FLASH.SET / HSET /
// RPUSH / ZADD write-through). Setting this too high relative to the pool
// queue depth (`io-threads * 4`) starves client writes with `PoolError::Full`.
//
// `0` = auto: `max(1, flash.io-threads / 2)`. Leaves at least half the pool
// queue open for client write-through in the worst case. Mutable via
// CONFIG SET.

pub const FLASH_DEMOTION_BATCH_DEFAULT: i64 = 0;
pub const FLASH_DEMOTION_BATCH_MIN: i64 = 0;
pub const FLASH_DEMOTION_BATCH_MAX: i64 = 4096;

pub static FLASH_DEMOTION_BATCH: AtomicI64 = AtomicI64::new(FLASH_DEMOTION_BATCH_DEFAULT);

/// Effective per-tick demotion budget. Never returns zero — when the knob is
/// `0` it auto-sizes from `flash.io-threads`.
pub fn flash_demotion_batch() -> usize {
    let v = FLASH_DEMOTION_BATCH.load(Ordering::Relaxed);
    if v == 0 {
        (flash_io_threads() / 2).max(1)
    } else {
        v as usize
    }
}

// ── flash.demotion-max-inflight ───────────────────────────────────────────────
//
// Hard cap on demotions submitted to the pool but not yet committed via
// phase 3. Bounds transient NVMe footprint (each in-flight entry holds both
// a cold-tier block and the stale durability-write block) and also prevents
// demotion from monopolising the pool queue across ticks.
//
// `0` = auto: `max(2, flash.io-threads * 2)`. At typical io-thread counts
// this keeps the shared pool's queue half-empty for client write-through.
// Mutable via CONFIG SET.

pub const FLASH_DEMOTION_MAX_INFLIGHT_DEFAULT: i64 = 0;
pub const FLASH_DEMOTION_MAX_INFLIGHT_MIN: i64 = 0;
pub const FLASH_DEMOTION_MAX_INFLIGHT_MAX: i64 = 65536;

pub static FLASH_DEMOTION_MAX_INFLIGHT: AtomicI64 =
    AtomicI64::new(FLASH_DEMOTION_MAX_INFLIGHT_DEFAULT);

/// Effective demotion in-flight cap. Never returns zero — `0` auto-sizes
/// from `flash.io-threads`.
pub fn flash_demotion_max_inflight() -> u64 {
    let v = FLASH_DEMOTION_MAX_INFLIGHT.load(Ordering::Relaxed);
    if v == 0 {
        (flash_io_threads() as u64 * 2).max(2)
    } else {
        v as u64
    }
}

// ── flash.cluster-mode-enabled ────────────────────────────────────────────────

pub const FLASH_CLUSTER_MODE_ENABLED_DEFAULT: &str = "auto";

/// Cluster mode detection.
/// `"auto"` — detect from `ContextFlags::CLUSTER` at load time (default).
/// `"yes"`  — always treat this instance as part of a cluster.
/// `"no"`   — always treat this instance as standalone.
pub static FLASH_CLUSTER_MODE_ENABLED: LazyLock<Mutex<String>> =
    LazyLock::new(|| Mutex::new(FLASH_CLUSTER_MODE_ENABLED_DEFAULT.to_string()));

// ── flash.replica-tier-enabled ────────────────────────────────────────────────

/// Enables the flash NVMe tier on cluster replicas (default `false`).
/// Downstream code reads this flag without touching config.rs.
pub static FLASH_REPLICA_TIER_ENABLED: AtomicBool = AtomicBool::new(false);

// ── flash.migration-max-key-bytes ─────────────────────────────────────────────

pub const FLASH_MIGRATION_MAX_KEY_BYTES_DEFAULT: i64 = 64 * 1024 * 1024; // 64 MiB
pub const FLASH_MIGRATION_MAX_KEY_BYTES_MIN: i64 = 1024; // 1 KiB
pub const FLASH_MIGRATION_MAX_KEY_BYTES_MAX: i64 = i64::MAX;

/// Per-key size limit during slot migration pre-warm. Keys larger than this are
/// skipped (they are still migrated correctly via rdb_save's NVMe read path).
/// Immutable after module load.
pub static FLASH_MIGRATION_MAX_KEY_BYTES: AtomicI64 =
    AtomicI64::new(FLASH_MIGRATION_MAX_KEY_BYTES_DEFAULT);

// ── flash.migration-bandwidth-mbps ───────────────────────────────────────────

pub const FLASH_MIGRATION_BANDWIDTH_MBPS_DEFAULT: i64 = 100;
pub const FLASH_MIGRATION_BANDWIDTH_MBPS_MIN: i64 = 0; // 0 = unlimited (no throttle)
pub const FLASH_MIGRATION_BANDWIDTH_MBPS_MAX: i64 = 100_000;

/// Soft bandwidth cap (MiB/s) for NVMe reads during EXPORT_STARTED pre-warm.
/// Mutable via CONFIG SET.
pub static FLASH_MIGRATION_BANDWIDTH_MBPS: AtomicI64 =
    AtomicI64::new(FLASH_MIGRATION_BANDWIDTH_MBPS_DEFAULT);

// ── flash.migration-chunk-timeout-sec ────────────────────────────────────────

pub const FLASH_MIGRATION_CHUNK_TIMEOUT_SEC_DEFAULT: i64 = 30;
pub const FLASH_MIGRATION_CHUNK_TIMEOUT_SEC_MIN: i64 = 1;
pub const FLASH_MIGRATION_CHUNK_TIMEOUT_SEC_MAX: i64 = 3600;

/// Wall-clock budget (seconds) for pre-warming cold keys on EXPORT_STARTED.
/// Keys not pre-warmed are still migrated correctly via rdb_save's NVMe read.
/// Mutable via CONFIG SET.
pub static FLASH_MIGRATION_CHUNK_TIMEOUT_SEC: AtomicI64 =
    AtomicI64::new(FLASH_MIGRATION_CHUNK_TIMEOUT_SEC_DEFAULT);

// ── flash.migration-probe-cache-sec ──────────────────────────────────────────

pub const FLASH_MIGRATION_PROBE_CACHE_SEC_DEFAULT: i64 = 60;
pub const FLASH_MIGRATION_PROBE_CACHE_SEC_MIN: i64 = 0; // 0 = no caching
pub const FLASH_MIGRATION_PROBE_CACHE_SEC_MAX: i64 = 3600;

/// TTL (seconds) for FLASH.MIGRATE.PROBE results cached by host:port.
/// 0 disables caching. Immutable after module load.
pub static FLASH_MIGRATION_PROBE_CACHE_SEC: AtomicI64 =
    AtomicI64::new(FLASH_MIGRATION_PROBE_CACHE_SEC_DEFAULT);

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_correct() {
        assert_eq!(FLASH_PATH_DEFAULT, "/tmp/valkey-flash.bin");
        assert_eq!(FLASH_CAPACITY_BYTES_DEFAULT, 1 << 30);
        assert_eq!(FLASH_CACHE_SIZE_BYTES_DEFAULT, 1 << 28);
        assert_eq!(FLASH_IO_THREADS_DEFAULT, 0);
        assert_eq!(FLASH_IO_URING_ENTRIES_DEFAULT, 256);
    }

    #[test]
    fn capacity_bytes_bounds_reject_below_one_mib() {
        assert_eq!(
            FLASH_CAPACITY_BYTES_MIN,
            1 << 20,
            "minimum must be 1 MiB so Valkey rejects values below it"
        );
        // Verify the constant itself is in the correct order (compile-time check).
        const { assert!((1_i64 << 20) - 1 < FLASH_CAPACITY_BYTES_MIN) };
    }

    #[test]
    fn cache_size_bytes_bounds_reject_below_one_mib() {
        assert_eq!(
            FLASH_CACHE_SIZE_BYTES_MIN,
            1 << 20,
            "minimum must be 1 MiB so Valkey rejects values below it"
        );
        const { assert!((1_i64 << 20) - 1 < FLASH_CACHE_SIZE_BYTES_MIN) };
    }

    #[test]
    fn io_threads_bounds_reject_above_256() {
        assert_eq!(
            FLASH_IO_THREADS_MAX, 256,
            "maximum must be 256 so Valkey rejects values above it"
        );
        const { assert!(257 > FLASH_IO_THREADS_MAX) };
    }

    #[test]
    fn io_uring_entries_bounds() {
        assert_eq!(FLASH_IO_URING_ENTRIES_MIN, 16);
        assert_eq!(FLASH_IO_URING_ENTRIES_MAX, 65536);
    }

    #[test]
    fn flash_io_threads_zero_maps_to_num_cpus() {
        FLASH_IO_THREADS.store(0, Ordering::Relaxed);
        assert_eq!(flash_io_threads(), num_cpus::get());
    }

    #[test]
    fn flash_io_threads_explicit_value_returned_as_is() {
        FLASH_IO_THREADS.store(8, Ordering::Relaxed);
        assert_eq!(flash_io_threads(), 8);
        FLASH_IO_THREADS.store(0, Ordering::Relaxed); // restore
    }

    #[test]
    fn cache_size_bytes_is_mutable() {
        let original = FLASH_CACHE_SIZE_BYTES.load(Ordering::Relaxed);
        FLASH_CACHE_SIZE_BYTES.store(1 << 25, Ordering::Relaxed);
        assert_eq!(FLASH_CACHE_SIZE_BYTES.load(Ordering::Relaxed), 1 << 25);
        FLASH_CACHE_SIZE_BYTES.store(original, Ordering::Relaxed);
    }

    #[test]
    fn sync_mode_default_is_everysec() {
        let mode = FLASH_SYNC.lock().unwrap();
        assert_eq!(*mode, SyncMode::everysec);
    }

    #[test]
    fn sync_mode_is_mutable() {
        {
            let mut mode = FLASH_SYNC.lock().unwrap();
            *mode = SyncMode::always;
            assert_eq!(*mode, SyncMode::always);
            *mode = SyncMode::everysec; // restore
        }
        assert_eq!(*FLASH_SYNC.lock().unwrap(), SyncMode::everysec);
    }

    #[test]
    fn path_default_is_correct() {
        let path = FLASH_PATH.lock().unwrap();
        assert_eq!(path.as_str(), FLASH_PATH_DEFAULT);
    }

    #[test]
    fn compaction_interval_default_is_60() {
        assert_eq!(FLASH_COMPACTION_INTERVAL_SEC_DEFAULT, 60);
        assert_eq!(FLASH_COMPACTION_INTERVAL_SEC_MIN, 1);
        assert_eq!(FLASH_COMPACTION_INTERVAL_SEC_MAX, 3600);
    }

    #[test]
    fn compaction_interval_is_mutable() {
        let original = FLASH_COMPACTION_INTERVAL_SEC.load(Ordering::Relaxed);
        FLASH_COMPACTION_INTERVAL_SEC.store(120, Ordering::Relaxed);
        assert_eq!(FLASH_COMPACTION_INTERVAL_SEC.load(Ordering::Relaxed), 120);
        FLASH_COMPACTION_INTERVAL_SEC.store(original, Ordering::Relaxed);
    }

    #[test]
    fn cluster_mode_enabled_default_is_auto() {
        assert_eq!(FLASH_CLUSTER_MODE_ENABLED_DEFAULT, "auto");
        let v = FLASH_CLUSTER_MODE_ENABLED.lock().unwrap();
        assert_eq!(v.as_str(), "auto");
    }

    #[test]
    fn cluster_mode_enabled_is_mutable() {
        {
            let mut v = FLASH_CLUSTER_MODE_ENABLED.lock().unwrap();
            *v = "yes".to_string();
            assert_eq!(v.as_str(), "yes");
            *v = "auto".to_string();
        }
        assert_eq!(FLASH_CLUSTER_MODE_ENABLED.lock().unwrap().as_str(), "auto");
    }

    #[test]
    fn replica_tier_enabled_default_is_false() {
        assert!(!FLASH_REPLICA_TIER_ENABLED.load(Ordering::Relaxed));
    }

    #[test]
    fn replica_tier_enabled_is_mutable() {
        FLASH_REPLICA_TIER_ENABLED.store(true, Ordering::Relaxed);
        assert!(FLASH_REPLICA_TIER_ENABLED.load(Ordering::Relaxed));
        FLASH_REPLICA_TIER_ENABLED.store(false, Ordering::Relaxed);
    }

    #[test]
    fn migration_config_defaults() {
        assert_eq!(FLASH_MIGRATION_MAX_KEY_BYTES_DEFAULT, 64 * 1024 * 1024);
        assert_eq!(FLASH_MIGRATION_BANDWIDTH_MBPS_DEFAULT, 100);
        assert_eq!(FLASH_MIGRATION_CHUNK_TIMEOUT_SEC_DEFAULT, 30);
        assert_eq!(FLASH_MIGRATION_PROBE_CACHE_SEC_DEFAULT, 60);
    }

    #[test]
    fn migration_bandwidth_mbps_is_mutable() {
        let original = FLASH_MIGRATION_BANDWIDTH_MBPS.load(Ordering::Relaxed);
        FLASH_MIGRATION_BANDWIDTH_MBPS.store(200, Ordering::Relaxed);
        assert_eq!(FLASH_MIGRATION_BANDWIDTH_MBPS.load(Ordering::Relaxed), 200);
        FLASH_MIGRATION_BANDWIDTH_MBPS.store(original, Ordering::Relaxed);
    }

    #[test]
    fn migration_chunk_timeout_is_mutable() {
        let original = FLASH_MIGRATION_CHUNK_TIMEOUT_SEC.load(Ordering::Relaxed);
        FLASH_MIGRATION_CHUNK_TIMEOUT_SEC.store(60, Ordering::Relaxed);
        assert_eq!(
            FLASH_MIGRATION_CHUNK_TIMEOUT_SEC.load(Ordering::Relaxed),
            60
        );
        FLASH_MIGRATION_CHUNK_TIMEOUT_SEC.store(original, Ordering::Relaxed);
    }
}
