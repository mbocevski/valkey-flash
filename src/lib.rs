use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{LazyLock, Mutex};
use valkey_module::{
    Context, ContextFlags, InfoContext, Status, ValkeyResult, ValkeyString,
    configuration::{ConfigurationContext, ConfigurationFlags},
    logging, raw, valkey_module,
};
use valkey_module_macros::info_command_handler;

pub mod async_io;
pub mod cluster;
pub mod commands;
pub mod config;
pub mod metrics;
pub mod persistence;
pub mod recovery;
pub mod replication;
pub mod storage;
pub mod types;
pub mod util;
pub mod util_expire;

use crate::async_io::AsyncThreadPool;
use crate::config::SyncMode;
use crate::config::{
    FLASH_CACHE_SIZE_BYTES, FLASH_CACHE_SIZE_BYTES_DEFAULT, FLASH_CACHE_SIZE_BYTES_MAX,
    FLASH_CACHE_SIZE_BYTES_MIN, FLASH_CAPACITY_BYTES, FLASH_CAPACITY_BYTES_DEFAULT,
    FLASH_CAPACITY_BYTES_MAX, FLASH_CAPACITY_BYTES_MIN, FLASH_CLUSTER_MODE_ENABLED,
    FLASH_CLUSTER_MODE_ENABLED_DEFAULT, FLASH_COMPACTION_INTERVAL_SEC,
    FLASH_COMPACTION_INTERVAL_SEC_DEFAULT, FLASH_COMPACTION_INTERVAL_SEC_MAX,
    FLASH_COMPACTION_INTERVAL_SEC_MIN, FLASH_IO_THREADS, FLASH_IO_THREADS_DEFAULT,
    FLASH_IO_THREADS_MAX, FLASH_IO_THREADS_MIN, FLASH_IO_URING_ENTRIES,
    FLASH_IO_URING_ENTRIES_DEFAULT, FLASH_IO_URING_ENTRIES_MAX, FLASH_IO_URING_ENTRIES_MIN,
    FLASH_MIGRATION_BANDWIDTH_MBPS, FLASH_MIGRATION_BANDWIDTH_MBPS_DEFAULT,
    FLASH_MIGRATION_BANDWIDTH_MBPS_MAX, FLASH_MIGRATION_BANDWIDTH_MBPS_MIN,
    FLASH_MIGRATION_CHUNK_TIMEOUT_SEC, FLASH_MIGRATION_CHUNK_TIMEOUT_SEC_DEFAULT,
    FLASH_MIGRATION_CHUNK_TIMEOUT_SEC_MAX, FLASH_MIGRATION_CHUNK_TIMEOUT_SEC_MIN,
    FLASH_MIGRATION_MAX_KEY_BYTES, FLASH_MIGRATION_MAX_KEY_BYTES_DEFAULT,
    FLASH_MIGRATION_MAX_KEY_BYTES_MAX, FLASH_MIGRATION_MAX_KEY_BYTES_MIN,
    FLASH_MIGRATION_PROBE_CACHE_SEC, FLASH_MIGRATION_PROBE_CACHE_SEC_DEFAULT,
    FLASH_MIGRATION_PROBE_CACHE_SEC_MAX, FLASH_MIGRATION_PROBE_CACHE_SEC_MIN, FLASH_PATH,
    FLASH_PATH_DEFAULT, FLASH_REPLICA_TIER_ENABLED, FLASH_SYNC, flash_io_threads,
};
use crate::recovery::{ModuleState, TierEntry};
use crate::storage::cache::FlashCache;
use crate::storage::file_io_uring::FileIoUringBackend;
use crate::storage::wal::{Wal, WalSyncMode};
use crate::types::hash::FLASH_HASH_TYPE;
use crate::types::list::FLASH_LIST_TYPE;
use crate::types::string::FLASH_STRING_TYPE;
use crate::types::zset::FLASH_ZSET_TYPE;

use crate::commands::aux_info::flash_aux_info_command;
use crate::commands::blmove::flash_blmove_command;
use crate::commands::blpop::flash_blpop_command;
use crate::commands::blpop::flash_brpop_command;
use crate::commands::bzpop::{flash_bzpopmax_command, flash_bzpopmin_command};
use crate::commands::compaction::{
    flash_compaction_stats_command, flash_compaction_trigger_command,
};
use crate::commands::debug_demote::flash_debug_demote_command;
use crate::commands::debug_state::flash_debug_state_command;
use crate::commands::del::flash_del_command;
use crate::commands::get::flash_get_command;
use crate::commands::hdel::flash_hdel_command;
use crate::commands::hexists::flash_hexists_command;
use crate::commands::hget::flash_hget_command;
use crate::commands::hgetall::flash_hgetall_command;
use crate::commands::hlen::flash_hlen_command;
use crate::commands::hset::flash_hset_command;
use crate::commands::lindex::flash_lindex_command;
use crate::commands::linsert::flash_linsert_command;
use crate::commands::llen::flash_llen_command;
use crate::commands::lmove::flash_lmove_command;
use crate::commands::lpop::flash_lpop_command;
use crate::commands::lpop::flash_rpop_command;
use crate::commands::lpush::flash_lpush_command;
use crate::commands::lpush::flash_lpushx_command;
use crate::commands::lpush::flash_rpush_command;
use crate::commands::lpush::flash_rpushx_command;
use crate::commands::lrange::flash_lrange_command;
use crate::commands::lrem::flash_lrem_command;
use crate::commands::lset::flash_lset_command;
use crate::commands::ltrim::flash_ltrim_command;
use crate::commands::migrate::flash_migrate_command;
use crate::commands::migrate_probe::flash_migrate_probe_command;
use crate::commands::set::flash_set_command;
use crate::commands::zadd::flash_zadd_command;
use crate::commands::zpop::{
    flash_zincrby_command, flash_zpopmax_command, flash_zpopmin_command, flash_zrem_command,
};
use crate::commands::zrange::{
    flash_zrange_command, flash_zrangebylex_command, flash_zrangebyscore_command,
    flash_zrevrangebylex_command, flash_zrevrangebyscore_command,
};
use crate::commands::zread::{
    flash_zcard_command, flash_zcount_command, flash_zlexcount_command, flash_zrank_command,
    flash_zrevrank_command, flash_zscan_command, flash_zscore_command,
};
use crate::commands::zstore::{
    flash_zdiffstore_command, flash_zinterstore_command, flash_zrangestore_command,
    flash_zunionstore_command,
};

pub const MODULE_NAME: &str = "flash";
pub const MODULE_VERSION: i32 = 1;

// ── Module-level singletons ───────────────────────────────────────────────────

/// In-memory hot cache. Initialised once in `initialize()`.
pub static CACHE: std::sync::OnceLock<FlashCache> = std::sync::OnceLock::new();

/// NVMe backing store. Initialised once in `initialize()`.
pub static STORAGE: std::sync::OnceLock<FileIoUringBackend> = std::sync::OnceLock::new();

/// Async I/O thread pool. Initialised once in `initialize()`.
pub static POOL: std::sync::OnceLock<AsyncThreadPool> = std::sync::OnceLock::new();

/// Write-ahead log. Initialised once in `initialize()` after recovery completes.
pub static WAL: std::sync::OnceLock<Wal> = std::sync::OnceLock::new();

/// In-memory tiering map: `key_hash → (offset, value_hash)` for cold-tier entries.
/// Populated by recovery; updated by FLASH.SET/DEL when they demote/delete cold keys.
pub static TIERING_MAP: LazyLock<Mutex<HashMap<u64, TierEntry>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Current module lifecycle state. Starts `Recovering`, transitions to `Ready`
/// (or `Error` on unrecoverable failure). Readable via `FLASH.DEBUG.STATE`.
pub static MODULE_STATE: LazyLock<Mutex<ModuleState>> =
    LazyLock::new(|| Mutex::new(ModuleState::Recovering));

/// Shutdown signal for the background compaction thread.
/// `deinitialize` locks the mutex, sets the flag, notifies the condvar, then
/// joins the thread — guaranteeing exit before `dlclose()` unmaps the `.so`.
static COMPACTION_SHUTDOWN: LazyLock<(Mutex<bool>, std::sync::Condvar)> =
    LazyLock::new(|| (Mutex::new(false), std::sync::Condvar::new()));

/// Join handle for the background compaction thread.
static COMPACTION_THREAD: LazyLock<Mutex<Option<std::thread::JoinHandle<()>>>> =
    LazyLock::new(|| Mutex::new(None));

// ── Module lifecycle ──────────────────────────────────────────────────────────

fn initialize(ctx: &Context, _args: &[ValkeyString]) -> Status {
    logging::log_notice("flash: initialize() entered");
    // 0 is the sentinel default for flash.io-threads (auto-detect).
    // Replace it with the actual logical CPU count once the server is running.
    if FLASH_IO_THREADS.load(Ordering::Relaxed) == 0 {
        FLASH_IO_THREADS.store(num_cpus::get() as i64, Ordering::Relaxed);
    }

    // ── Cluster mode detection ────────────────────────────────────────────────
    //
    // Read the knob first; fall back to flag-based auto-detection for "auto".
    // IS_CLUSTER is set before the SLAVE early-exit so replicas also carry the
    // correct value (downstream tasks like #82 read it from replica context).
    let cluster_setting = match FLASH_CLUSTER_MODE_ENABLED.lock() {
        Ok(g) => g.clone(),
        Err(e) => {
            logging::log_warning(
                format!("flash: FLASH_CLUSTER_MODE_ENABLED lock poisoned: {e}").as_str(),
            );
            return Status::Err;
        }
    };
    let cluster_active = match cluster_setting.to_lowercase().as_str() {
        "yes" => true,
        "no" => false,
        _ => ctx.get_flags().contains(ContextFlags::CLUSTER), // "auto"
    };
    cluster::IS_CLUSTER.store(cluster_active, Ordering::Release);

    if cluster_active {
        // SAFETY: ValkeyModule_GetMyClusterID returns a static 40-char hex string
        // (the 160-bit SHA1 node ID) or NULL if not in cluster mode. The pointer
        // is valid for the lifetime of the server process.
        let node_id = unsafe {
            #[allow(static_mut_refs)]
            if let Some(f) = raw::RedisModule_GetMyClusterID {
                let ptr = f();
                if ptr.is_null() {
                    "<unknown>".to_string()
                } else {
                    std::ffi::CStr::from_ptr(ptr).to_string_lossy().into_owned()
                }
            } else {
                "<unavailable>".to_string()
            }
        };
        logging::log_notice(format!("flash: cluster mode enabled, node {node_id}").as_str());

        // Allow CLUSTER MIGRATESLOTS to proceed — without this Valkey returns
        // "ERR This command is not allowed when module <name> is loaded" for
        // modules that have not explicitly opted in. (VALKEYMODULE_OPTIONS_HANDLE_ATOMIC_SLOT_MIGRATION = 1<<5)
        const HANDLE_ATOMIC_SLOT_MIGRATION: ::std::os::raw::c_int = 1 << 5;
        unsafe {
            #[allow(static_mut_refs)]
            if let Some(f) = raw::RedisModule_SetModuleOptions {
                f(ctx.ctx, HANDLE_ATOMIC_SLOT_MIGRATION);
            }
        }

        cluster::subscribe_cluster_events(ctx);
    }

    let path = match FLASH_PATH.lock() {
        Ok(g) => g.clone(),
        Err(e) => {
            logging::log_warning(format!("flash: FLASH_PATH lock poisoned: {e}").as_str());
            return Status::Err;
        }
    };
    let capacity = FLASH_CAPACITY_BYTES.load(Ordering::Relaxed) as u64;
    let cache_size = FLASH_CACHE_SIZE_BYTES.load(Ordering::Relaxed) as u64;
    let io_uring_entries = FLASH_IO_URING_ENTRIES.load(Ordering::Relaxed) as u32;
    let io_threads = flash_io_threads();

    // ── Replica handling ──────────────────────────────────────────────────────
    if ctx.get_flags().contains(ContextFlags::SLAVE) {
        replication::IS_REPLICA.store(true, Ordering::Release);
        if !FLASH_REPLICA_TIER_ENABLED.load(Ordering::Relaxed) {
            // v1 / default: RAM-only replica — NVMe backend deferred until promotion.
            let _ = CACHE.set(FlashCache::new(cache_size));
            if let Ok(mut state) = MODULE_STATE.lock() {
                *state = ModuleState::Ready;
            }
            logging::log_notice(
                "flash: starting as replica — NVMe backend deferred until promotion",
            );
            return Status::Ok;
        }
        // flash.replica-tier-enabled=true: open a local NVMe tier so this node
        // can make independent cache/eviction decisions. Fall through to the
        // full init path below (same as a primary).
        logging::log_notice(
            "flash: starting as replica with flash.replica-tier-enabled — \
             initializing local NVMe backend",
        );
    }

    // Derive WAL path from the backing-store path (same dir, .wal extension).
    let wal_path = PathBuf::from(&path).with_extension("wal");

    // WAL sync mode follows the same knob as the backing store.
    let wal_sync_mode = match FLASH_SYNC.lock() {
        Ok(g) => {
            if *g == SyncMode::always {
                WalSyncMode::Always
            } else if *g == SyncMode::no {
                WalSyncMode::No
            } else {
                WalSyncMode::Everysec
            }
        }
        Err(e) => {
            logging::log_warning(format!("flash: FLASH_SYNC lock poisoned: {e}").as_str());
            return Status::Err;
        }
    };

    // Read aux state that was populated by aux_load during RDB restore.
    // May be None on a fresh start (no RDB present).
    let aux_state = match persistence::aux::LOADED_AUX_STATE.lock() {
        Ok(guard) => guard.clone(),
        Err(e) => {
            logging::log_warning(format!("flash: LOADED_AUX_STATE lock poisoned: {e}").as_str());
            return Status::Err;
        }
    };

    // ── Crash recovery ────────────────────────────────────────────────────────
    //
    // Run WAL replay before opening the operational WAL so that any torn-tail
    // truncation happens on the recovery handle first.  The operational WAL is
    // then opened fresh against the (possibly truncated) file.

    match recovery::run_recovery(aux_state.as_ref(), &wal_path, &path, capacity) {
        Ok((stats, tiering_map)) => {
            for warn in &stats.warnings {
                logging::log_warning(warn.as_str());
            }
            logging::log_notice(
                format!(
                    "flash: recovery complete: {} records applied in {}ms",
                    stats.records_applied, stats.elapsed_ms,
                )
                .as_str(),
            );
            if let Ok(mut map) = TIERING_MAP.lock() {
                *map = tiering_map;
            }
        }
        Err(e) => {
            logging::log_warning(format!("flash: recovery failed: {e}").as_str());
            if let Ok(mut state) = MODULE_STATE.lock() {
                *state = ModuleState::Error;
            }
            return Status::Err;
        }
    }

    // ── Open backing store ────────────────────────────────────────────────────

    match FileIoUringBackend::open(&path, capacity, io_uring_entries) {
        Ok(backend) => {
            let _ = STORAGE.set(backend);
        }
        Err(e) => {
            logging::log_warning(
                format!("flash: failed to open storage at '{path}': {e}").as_str(),
            );
            return Status::Err;
        }
    }

    // Restore NVMe allocator state from aux so freed blocks survive restarts.
    if let Some(ref state) = aux_state
        && let Some(storage) = STORAGE.get()
    {
        let nb = state.before.nvme_next_block;
        let free = state.before.free_blocks.clone();
        let n_ranges = free.len();
        storage.restore_state(nb, free);
        logging::log_notice(
            format!("flash: restored NVMe allocator: next_block={nb}, free_ranges={n_ranges}")
                .as_str(),
        );
    }

    // ── Open operational WAL ──────────────────────────────────────────────────

    match Wal::open(&wal_path, wal_sync_mode) {
        Ok(wal) => {
            let _ = WAL.set(wal);
        }
        Err(e) => {
            logging::log_warning(
                format!("flash: failed to open WAL at '{}': {e}", wal_path.display()).as_str(),
            );
            return Status::Err;
        }
    }

    let _ = CACHE.set(FlashCache::new(cache_size));
    let _ = POOL.set(AsyncThreadPool::new(io_threads));

    // Reset shutdown flag before spawning (handles a re-load within the same process).
    if let Ok(mut flag) = COMPACTION_SHUTDOWN.0.lock() {
        *flag = false;
    }

    // Background compaction thread: coalesces the free-list every
    // flash.compaction-interval-sec seconds. Uses Condvar::wait_timeout so that
    // a shutdown signal from deinitialize() wakes the thread immediately rather
    // than waiting up to one full tick interval.
    let handle = std::thread::spawn(compaction_worker);
    if let Ok(mut guard) = COMPACTION_THREAD.lock() {
        *guard = Some(handle);
    }

    if let Ok(mut state) = MODULE_STATE.lock() {
        *state = ModuleState::Ready;
    }

    Status::Ok
}

fn deinitialize(_ctx: &Context) -> Status {
    // Set the shutdown flag and wake the compaction thread immediately via the
    // condvar, then join it.  This guarantees the thread exits before
    // dlclose() unmaps the .so text segment.  The wait is bounded by one
    // condvar wait_timeout (≤ 100 ms) plus the duration of any in-progress
    // compaction tick (purely in-memory, negligible).
    {
        let (lock, cvar) = &*COMPACTION_SHUTDOWN;
        *lock.lock().unwrap_or_else(|e| e.into_inner()) = true;
        cvar.notify_one();
    }
    let handle = COMPACTION_THREAD
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .take();
    if let Some(h) = handle {
        let _ = h.join();
    }
    // Flush LLVM coverage profraw before dlclose() removes our atexit handler.
    // dlclose() would otherwise make the registered atexit a dangling pointer,
    // silently dropping all integration-test coverage data.
    // Only compiled when --cfg=coverage is set (cargo llvm-cov --no-report).
    #[cfg(coverage)]
    {
        unsafe extern "C" {
            fn __llvm_profile_write_file() -> std::os::raw::c_int;
        }
        // SAFETY: __llvm_profile_write_file is internally synchronized per LLVM docs.
        unsafe {
            __llvm_profile_write_file();
        }
    }
    Status::Ok
}

fn compaction_worker() {
    let (lock, cvar) = &*COMPACTION_SHUTDOWN;
    let mut elapsed_decisecs: u64 = 0;
    loop {
        let guard = lock.lock().unwrap_or_else(|e| e.into_inner());
        let (guard, _) = cvar
            .wait_timeout(guard, std::time::Duration::from_millis(100))
            .unwrap_or_else(|e| e.into_inner());
        if *guard {
            return;
        }
        drop(guard);

        elapsed_decisecs += 1;
        let interval_decisecs = FLASH_COMPACTION_INTERVAL_SEC.load(Ordering::Relaxed) as u64 * 10;
        if elapsed_decisecs >= interval_decisecs {
            elapsed_decisecs = 0;
            if let Some(storage) = STORAGE.get() {
                storage.run_compaction_tick();
            }
        }
    }
}

/// Initialise the NVMe backend (STORAGE, WAL, POOL, compaction thread) when
/// this instance is promoted from replica to primary via `REPLICAOF NO ONE`.
///
/// Early-returns without error if STORAGE is already set — this handles the
/// primary→replica→primary round-trip where the existing backend is still live.
/// On failure, MODULE_STATE is set to Error and a warning is logged.
pub(crate) fn init_nvme_backend() {
    if STORAGE.get().is_some() {
        logging::log_notice("flash: promotion: NVMe backend already present — resuming");
        return;
    }

    let path = match FLASH_PATH.lock() {
        Ok(g) => g.clone(),
        Err(e) => {
            logging::log_warning(
                format!("flash: promotion: FLASH_PATH lock poisoned: {e}").as_str(),
            );
            return;
        }
    };
    let capacity = FLASH_CAPACITY_BYTES.load(Ordering::Relaxed) as u64;
    let io_uring_entries = FLASH_IO_URING_ENTRIES.load(Ordering::Relaxed) as u32;
    let io_threads = flash_io_threads();
    let wal_path = PathBuf::from(&path).with_extension("wal");

    let wal_sync_mode = match FLASH_SYNC.lock() {
        Ok(g) => {
            if *g == SyncMode::always {
                WalSyncMode::Always
            } else if *g == SyncMode::no {
                WalSyncMode::No
            } else {
                WalSyncMode::Everysec
            }
        }
        Err(e) => {
            logging::log_warning(
                format!("flash: promotion: FLASH_SYNC lock poisoned: {e}").as_str(),
            );
            return;
        }
    };

    // Freshly promoted replica has no prior WAL or NVMe state; None aux is correct.
    match recovery::run_recovery(None, &wal_path, &path, capacity) {
        Ok((stats, tiering_map)) => {
            for warn in &stats.warnings {
                logging::log_warning(warn.as_str());
            }
            logging::log_notice(
                format!(
                    "flash: promotion recovery: {} records in {}ms",
                    stats.records_applied, stats.elapsed_ms,
                )
                .as_str(),
            );
            if let Ok(mut map) = TIERING_MAP.lock() {
                *map = tiering_map;
            }
        }
        Err(e) => {
            logging::log_warning(format!("flash: promotion: recovery failed: {e}").as_str());
            if let Ok(mut state) = MODULE_STATE.lock() {
                *state = ModuleState::Error;
            }
            return;
        }
    }

    match FileIoUringBackend::open(&path, capacity, io_uring_entries) {
        Ok(backend) => {
            let _ = STORAGE.set(backend);
        }
        Err(e) => {
            logging::log_warning(
                format!("flash: promotion: failed to open storage at '{path}': {e}").as_str(),
            );
            if let Ok(mut state) = MODULE_STATE.lock() {
                *state = ModuleState::Error;
            }
            return;
        }
    }

    match Wal::open(&wal_path, wal_sync_mode) {
        Ok(wal) => {
            let _ = WAL.set(wal);
        }
        Err(e) => {
            logging::log_warning(
                format!(
                    "flash: promotion: failed to open WAL at '{}': {e}",
                    wal_path.display()
                )
                .as_str(),
            );
            if let Ok(mut state) = MODULE_STATE.lock() {
                *state = ModuleState::Error;
            }
            return;
        }
    }

    let _ = POOL.set(AsyncThreadPool::new(io_threads));

    if let Ok(mut flag) = COMPACTION_SHUTDOWN.0.lock() {
        *flag = false;
    }
    let handle = std::thread::spawn(compaction_worker);
    if let Ok(mut guard) = COMPACTION_THREAD.lock() {
        *guard = Some(handle);
    }

    if let Ok(mut state) = MODULE_STATE.lock() {
        *state = ModuleState::Ready;
    }

    logging::log_notice("flash: promotion to primary complete — NVMe backend initialized");
}

/// Wake the compaction thread so the new `flash.compaction-interval-sec` takes
/// effect immediately rather than waiting for the current 100 ms sleep to expire.
pub(crate) fn wake_compaction() {
    let (_, cvar) = &*COMPACTION_SHUTDOWN;
    cvar.notify_all();
}

#[info_command_handler]
fn info_handler(ctx: &InfoContext, _for_crash_report: bool) -> ValkeyResult<()> {
    metrics::flash_info_handler(ctx)
}

valkey_module! {
    name: MODULE_NAME,
    version: MODULE_VERSION,
    allocator: (valkey_module::alloc::ValkeyAlloc, valkey_module::alloc::ValkeyAlloc),
    data_types: [
        FLASH_STRING_TYPE,
        FLASH_HASH_TYPE,
        FLASH_LIST_TYPE,
        FLASH_ZSET_TYPE,
    ],
    init: initialize,
    deinit: deinitialize,
    acl_categories: [
        "flash",
    ]
    commands: [
        ["FLASH.SET", flash_set_command, "write deny-oom", 1, 1, 1, "write fast flash"],
        ["FLASH.GET", flash_get_command, "readonly", 1, 1, 1, "read flash"],
        ["FLASH.DEL", flash_del_command, "write", 1, -1, 1, "write fast flash"],
        ["FLASH.HSET", flash_hset_command, "write deny-oom", 1, 1, 1, "write flash"],
        ["FLASH.HGET", flash_hget_command, "readonly", 1, 1, 1, "read flash"],
        ["FLASH.HGETALL", flash_hgetall_command, "readonly", 1, 1, 1, "read flash"],
        ["FLASH.HDEL", flash_hdel_command, "write", 1, 1, 1, "write flash"],
        ["FLASH.HEXISTS", flash_hexists_command, "readonly", 1, 1, 1, "read flash"],
        ["FLASH.HLEN", flash_hlen_command, "readonly", 1, 1, 1, "read flash"],
        ["FLASH.AUX.INFO", flash_aux_info_command, "readonly admin", 0, 0, 0, "admin flash"],
        ["FLASH.DEBUG.DEMOTE", flash_debug_demote_command, "write admin", 1, 1, 1, "admin dangerous flash"],
        ["FLASH.DEBUG.STATE", flash_debug_state_command, "readonly no-auth allow-busy", 0, 0, 0, "admin flash"],
        ["FLASH.COMPACTION.STATS", flash_compaction_stats_command, "readonly admin", 0, 0, 0, "admin dangerous flash"],
        ["FLASH.COMPACTION.TRIGGER", flash_compaction_trigger_command, "write admin", 0, 0, 0, "admin dangerous flash"],
        ["FLASH.LPUSH", flash_lpush_command, "write deny-oom", 1, 1, 1, "write flash"],
        ["FLASH.RPUSH", flash_rpush_command, "write deny-oom", 1, 1, 1, "write flash"],
        ["FLASH.LPUSHX", flash_lpushx_command, "write deny-oom", 1, 1, 1, "write flash"],
        ["FLASH.RPUSHX", flash_rpushx_command, "write deny-oom", 1, 1, 1, "write flash"],
        ["FLASH.LPOP", flash_lpop_command, "write", 1, 1, 1, "write flash"],
        ["FLASH.RPOP", flash_rpop_command, "write", 1, 1, 1, "write flash"],
        ["FLASH.LRANGE", flash_lrange_command, "readonly", 1, 1, 1, "read flash"],
        ["FLASH.LLEN", flash_llen_command, "readonly fast", 1, 1, 1, "read fast flash"],
        ["FLASH.LINDEX", flash_lindex_command, "readonly", 1, 1, 1, "read flash"],
        ["FLASH.LSET", flash_lset_command, "write deny-oom", 1, 1, 1, "write flash"],
        ["FLASH.LINSERT", flash_linsert_command, "write deny-oom", 1, 1, 1, "write flash"],
        ["FLASH.LREM", flash_lrem_command, "write", 1, 1, 1, "write flash"],
        ["FLASH.LTRIM", flash_ltrim_command, "write", 1, 1, 1, "write flash"],
        ["FLASH.LMOVE", flash_lmove_command, "write deny-oom", 1, 2, 1, "write flash"],
        ["FLASH.BLPOP", flash_blpop_command, "write blocking", 1, -2, 1, "write slow blocking flash"],
        ["FLASH.BRPOP", flash_brpop_command, "write blocking", 1, -2, 1, "write slow blocking flash"],
        ["FLASH.BLMOVE", flash_blmove_command, "write deny-oom blocking", 1, 2, 1, "write slow blocking flash"],
        ["FLASH.MIGRATE", flash_migrate_command, "write admin", 0, 0, 0, "admin dangerous flash"],
        ["FLASH.MIGRATE.PROBE", flash_migrate_probe_command, "readonly admin", 0, 0, 0, "admin dangerous flash"],
        ["FLASH.ZADD", flash_zadd_command, "write deny-oom", 1, 1, 1, "write flash"],
        ["FLASH.ZREM", flash_zrem_command, "write", 1, 1, 1, "write flash"],
        ["FLASH.ZINCRBY", flash_zincrby_command, "write deny-oom", 1, 1, 1, "write flash"],
        ["FLASH.ZPOPMIN", flash_zpopmin_command, "write", 1, 1, 1, "write flash"],
        ["FLASH.ZPOPMAX", flash_zpopmax_command, "write", 1, 1, 1, "write flash"],
        ["FLASH.ZSCORE", flash_zscore_command, "readonly fast", 1, 1, 1, "read fast flash"],
        ["FLASH.ZRANK", flash_zrank_command, "readonly", 1, 1, 1, "read flash"],
        ["FLASH.ZREVRANK", flash_zrevrank_command, "readonly", 1, 1, 1, "read flash"],
        ["FLASH.ZCARD", flash_zcard_command, "readonly fast", 1, 1, 1, "read fast flash"],
        ["FLASH.ZCOUNT", flash_zcount_command, "readonly", 1, 1, 1, "read flash"],
        ["FLASH.ZLEXCOUNT", flash_zlexcount_command, "readonly", 1, 1, 1, "read flash"],
        ["FLASH.ZRANGE", flash_zrange_command, "readonly", 1, 1, 1, "read flash"],
        ["FLASH.ZRANGEBYSCORE", flash_zrangebyscore_command, "readonly", 1, 1, 1, "read flash"],
        ["FLASH.ZREVRANGEBYSCORE", flash_zrevrangebyscore_command, "readonly", 1, 1, 1, "read flash"],
        ["FLASH.ZRANGEBYLEX", flash_zrangebylex_command, "readonly", 1, 1, 1, "read flash"],
        ["FLASH.ZREVRANGEBYLEX", flash_zrevrangebylex_command, "readonly", 1, 1, 1, "read flash"],
        ["FLASH.ZSCAN", flash_zscan_command, "readonly", 1, 1, 1, "read flash"],
        ["FLASH.BZPOPMIN", flash_bzpopmin_command, "write blocking", 1, -2, 1, "write slow blocking flash"],
        ["FLASH.BZPOPMAX", flash_bzpopmax_command, "write blocking", 1, -2, 1, "write slow blocking flash"],
        ["FLASH.ZUNIONSTORE", flash_zunionstore_command, "write deny-oom", 1, 1, 1, "write slow flash"],
        ["FLASH.ZINTERSTORE", flash_zinterstore_command, "write deny-oom", 1, 1, 1, "write slow flash"],
        ["FLASH.ZDIFFSTORE", flash_zdiffstore_command, "write deny-oom", 1, 1, 1, "write slow flash"],
        ["FLASH.ZRANGESTORE", flash_zrangestore_command, "write deny-oom", 1, 1, 1, "write slow flash"],
    ],
    configurations: [
        i64: [
            // immutable knobs — ConfigurationFlags::IMMUTABLE prevents CONFIG SET after load
            ["capacity-bytes", &FLASH_CAPACITY_BYTES, FLASH_CAPACITY_BYTES_DEFAULT, FLASH_CAPACITY_BYTES_MIN, FLASH_CAPACITY_BYTES_MAX, ConfigurationFlags::IMMUTABLE, None],
            ["io-threads",     &FLASH_IO_THREADS,     FLASH_IO_THREADS_DEFAULT,     FLASH_IO_THREADS_MIN,     FLASH_IO_THREADS_MAX,     ConfigurationFlags::IMMUTABLE, None],
            ["io-uring-entries", &FLASH_IO_URING_ENTRIES, FLASH_IO_URING_ENTRIES_DEFAULT, FLASH_IO_URING_ENTRIES_MIN, FLASH_IO_URING_ENTRIES_MAX, ConfigurationFlags::IMMUTABLE, None],
            // mutable knobs — on_changed callback applies the new value at runtime
            ["cache-size-bytes", &FLASH_CACHE_SIZE_BYTES, FLASH_CACHE_SIZE_BYTES_DEFAULT, FLASH_CACHE_SIZE_BYTES_MIN, FLASH_CACHE_SIZE_BYTES_MAX, ConfigurationFlags::DEFAULT,
             Some(Box::new(|_ctx: &ConfigurationContext, _name: &str, variable: &'static std::sync::atomic::AtomicI64| {
                 let new_capacity = variable.load(Ordering::Relaxed) as u64;
                 if let Some(cache) = CACHE.get() {
                     cache.resize(new_capacity);
                 }
             }))],
            ["compaction-interval-sec", &FLASH_COMPACTION_INTERVAL_SEC, FLASH_COMPACTION_INTERVAL_SEC_DEFAULT, FLASH_COMPACTION_INTERVAL_SEC_MIN, FLASH_COMPACTION_INTERVAL_SEC_MAX, ConfigurationFlags::DEFAULT,
             Some(Box::new(|_ctx: &ConfigurationContext, _name: &str, _variable: &'static std::sync::atomic::AtomicI64| {
                 wake_compaction();
             }))],
            // migration knobs
            ["migration-max-key-bytes", &FLASH_MIGRATION_MAX_KEY_BYTES, FLASH_MIGRATION_MAX_KEY_BYTES_DEFAULT, FLASH_MIGRATION_MAX_KEY_BYTES_MIN, FLASH_MIGRATION_MAX_KEY_BYTES_MAX, ConfigurationFlags::IMMUTABLE, None],
            ["migration-bandwidth-mbps", &FLASH_MIGRATION_BANDWIDTH_MBPS, FLASH_MIGRATION_BANDWIDTH_MBPS_DEFAULT, FLASH_MIGRATION_BANDWIDTH_MBPS_MIN, FLASH_MIGRATION_BANDWIDTH_MBPS_MAX, ConfigurationFlags::DEFAULT, None],
            ["migration-chunk-timeout-sec", &FLASH_MIGRATION_CHUNK_TIMEOUT_SEC, FLASH_MIGRATION_CHUNK_TIMEOUT_SEC_DEFAULT, FLASH_MIGRATION_CHUNK_TIMEOUT_SEC_MIN, FLASH_MIGRATION_CHUNK_TIMEOUT_SEC_MAX, ConfigurationFlags::DEFAULT, None],
            ["migration-probe-cache-sec", &FLASH_MIGRATION_PROBE_CACHE_SEC, FLASH_MIGRATION_PROBE_CACHE_SEC_DEFAULT, FLASH_MIGRATION_PROBE_CACHE_SEC_MIN, FLASH_MIGRATION_PROBE_CACHE_SEC_MAX, ConfigurationFlags::IMMUTABLE, None],
        ],
        string: [
            ["path", &*FLASH_PATH, FLASH_PATH_DEFAULT, ConfigurationFlags::IMMUTABLE, None],
            ["cluster-mode-enabled", &*FLASH_CLUSTER_MODE_ENABLED, FLASH_CLUSTER_MODE_ENABLED_DEFAULT, ConfigurationFlags::IMMUTABLE, None],
        ],
        bool: [
            ["replica-tier-enabled", &FLASH_REPLICA_TIER_ENABLED, false, ConfigurationFlags::IMMUTABLE, None],
        ],
        enum: [
            ["sync", &*FLASH_SYNC, SyncMode::everysec, ConfigurationFlags::DEFAULT,
             Some(Box::new(|_ctx: &ConfigurationContext, _name: &str, variable: &'static Mutex<SyncMode>| {
                 let mode = match *variable.lock().unwrap_or_else(|e| e.into_inner()) {
                     SyncMode::always => WalSyncMode::Always,
                     SyncMode::everysec => WalSyncMode::Everysec,
                     SyncMode::no => WalSyncMode::No,
                 };
                 if let Some(wal) = WAL.get() {
                     wal.set_sync_mode(mode);
                 }
             }))],
        ],
        module_args_as_configuration: true,
    ]
}
