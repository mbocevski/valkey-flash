use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{LazyLock, Mutex};
use valkey_module::{
    configuration::ConfigurationFlags, logging, valkey_module, Context, Status, ValkeyString,
};

pub mod async_io;
pub mod commands;
pub mod config;
pub mod persistence;
pub mod recovery;
pub mod storage;
pub mod types;
pub mod util;

use crate::async_io::AsyncThreadPool;
use crate::config::SyncMode;
use crate::config::{
    flash_io_threads, FLASH_CACHE_SIZE_BYTES, FLASH_CACHE_SIZE_BYTES_DEFAULT,
    FLASH_CACHE_SIZE_BYTES_MAX, FLASH_CACHE_SIZE_BYTES_MIN, FLASH_CAPACITY_BYTES,
    FLASH_CAPACITY_BYTES_DEFAULT, FLASH_CAPACITY_BYTES_MAX, FLASH_CAPACITY_BYTES_MIN,
    FLASH_COMPACTION_INTERVAL_SEC, FLASH_COMPACTION_INTERVAL_SEC_DEFAULT,
    FLASH_COMPACTION_INTERVAL_SEC_MAX, FLASH_COMPACTION_INTERVAL_SEC_MIN, FLASH_IO_THREADS,
    FLASH_IO_THREADS_DEFAULT, FLASH_IO_THREADS_MAX, FLASH_IO_THREADS_MIN, FLASH_IO_URING_ENTRIES,
    FLASH_IO_URING_ENTRIES_DEFAULT, FLASH_IO_URING_ENTRIES_MAX, FLASH_IO_URING_ENTRIES_MIN,
    FLASH_PATH, FLASH_PATH_DEFAULT, FLASH_SYNC,
};
use crate::recovery::{ModuleState, TierEntry};
use crate::storage::cache::FlashCache;
use crate::storage::file_io_uring::FileIoUringBackend;
use crate::storage::wal::{Wal, WalSyncMode};
use crate::types::hash::FLASH_HASH_TYPE;
use crate::types::string::FLASH_STRING_TYPE;

use crate::commands::aux_info::flash_aux_info_command;
use crate::commands::compaction::{
    flash_compaction_stats_command, flash_compaction_trigger_command,
};
use crate::commands::debug_state::flash_debug_state_command;
use crate::commands::del::flash_del_command;
use crate::commands::get::flash_get_command;
use crate::commands::set::flash_set_command;

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

/// Signal the background compaction thread to exit on module unload.
static COMPACTION_SHUTDOWN: AtomicBool = AtomicBool::new(false);

/// Join handle for the background compaction thread.
/// `deinitialize` takes the handle and joins it so the thread exits before
/// `dlclose()` unmaps the `.so` text segment.
static COMPACTION_THREAD: LazyLock<Mutex<Option<std::thread::JoinHandle<()>>>> =
    LazyLock::new(|| Mutex::new(None));

// ── Module lifecycle ──────────────────────────────────────────────────────────

fn initialize(_ctx: &Context, _args: &[ValkeyString]) -> Status {
    // 0 is the sentinel default for flash.io-threads (auto-detect).
    // Replace it with the actual logical CPU count once the server is running.
    if FLASH_IO_THREADS.load(Ordering::Relaxed) == 0 {
        FLASH_IO_THREADS.store(num_cpus::get() as i64, Ordering::Relaxed);
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
    if let Some(ref state) = aux_state {
        if let Some(storage) = STORAGE.get() {
            let nb = state.before.nvme_next_block;
            let free = state.before.free_blocks.clone();
            let n_ranges = free.len();
            storage.restore_state(nb, free);
            logging::log_notice(
                format!("flash: restored NVMe allocator: next_block={nb}, free_ranges={n_ranges}")
                    .as_str(),
            );
        }
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

    // Background compaction thread: coalesces the free-list every
    // flash.compaction-interval-sec seconds. Sleeps in 100 ms increments so
    // module unload (COMPACTION_SHUTDOWN) is noticed quickly.
    COMPACTION_SHUTDOWN.store(false, Ordering::Relaxed);
    let handle = std::thread::spawn(|| {
        let mut elapsed_decisecs: u64 = 0; // units of 100 ms
        loop {
            std::thread::sleep(std::time::Duration::from_millis(100));
            if COMPACTION_SHUTDOWN.load(Ordering::Relaxed) {
                return;
            }
            elapsed_decisecs += 1;
            let interval_decisecs =
                FLASH_COMPACTION_INTERVAL_SEC.load(Ordering::Relaxed) as u64 * 10;
            if elapsed_decisecs >= interval_decisecs {
                elapsed_decisecs = 0;
                if let Some(storage) = STORAGE.get() {
                    storage.run_compaction_tick();
                }
            }
        }
    });
    if let Ok(mut guard) = COMPACTION_THREAD.lock() {
        *guard = Some(handle);
    }

    if let Ok(mut state) = MODULE_STATE.lock() {
        *state = ModuleState::Ready;
    }

    Status::Ok
}

fn deinitialize(_ctx: &Context) -> Status {
    // Signal the compaction thread to stop, then join it (≤ 100 ms wait).
    // This must complete before dlclose() unmaps the .so text segment.
    COMPACTION_SHUTDOWN.store(true, Ordering::Relaxed);
    if let Ok(mut guard) = COMPACTION_THREAD.lock() {
        if let Some(handle) = guard.take() {
            let _ = handle.join();
        }
    }
    Status::Ok
}

valkey_module! {
    name: MODULE_NAME,
    version: MODULE_VERSION,
    allocator: (valkey_module::alloc::ValkeyAlloc, valkey_module::alloc::ValkeyAlloc),
    data_types: [
        FLASH_STRING_TYPE,
        FLASH_HASH_TYPE,
    ],
    init: initialize,
    deinit: deinitialize,
    commands: [
        ["FLASH.SET", flash_set_command, "write deny-oom", 1, 1, 1, "write flash"],
        ["FLASH.GET", flash_get_command, "readonly", 1, 1, 1, "read flash"],
        ["FLASH.DEL", flash_del_command, "write", 1, -1, 1, "write flash"],
        ["FLASH.AUX.INFO", flash_aux_info_command, "readonly", 0, 0, 0, "read flash"],
        ["FLASH.DEBUG.STATE", flash_debug_state_command, "readonly no-auth allow-busy", 0, 0, 0, "read flash"],
        ["FLASH.COMPACTION.STATS", flash_compaction_stats_command, "readonly", 0, 0, 0, "read flash"],
        ["FLASH.COMPACTION.TRIGGER", flash_compaction_trigger_command, "write", 0, 0, 0, "write flash"],
    ],
    configurations: [
        i64: [
            // immutable knobs — ConfigurationFlags::IMMUTABLE prevents CONFIG SET after load
            ["capacity-bytes", &FLASH_CAPACITY_BYTES, FLASH_CAPACITY_BYTES_DEFAULT, FLASH_CAPACITY_BYTES_MIN, FLASH_CAPACITY_BYTES_MAX, ConfigurationFlags::IMMUTABLE, None],
            ["io-threads",     &FLASH_IO_THREADS,     FLASH_IO_THREADS_DEFAULT,     FLASH_IO_THREADS_MIN,     FLASH_IO_THREADS_MAX,     ConfigurationFlags::IMMUTABLE, None],
            ["io-uring-entries", &FLASH_IO_URING_ENTRIES, FLASH_IO_URING_ENTRIES_DEFAULT, FLASH_IO_URING_ENTRIES_MIN, FLASH_IO_URING_ENTRIES_MAX, ConfigurationFlags::IMMUTABLE, None],
            // mutable knobs
            ["cache-size-bytes", &FLASH_CACHE_SIZE_BYTES, FLASH_CACHE_SIZE_BYTES_DEFAULT, FLASH_CACHE_SIZE_BYTES_MIN, FLASH_CACHE_SIZE_BYTES_MAX, ConfigurationFlags::DEFAULT, None],
            ["compaction-interval-sec", &FLASH_COMPACTION_INTERVAL_SEC, FLASH_COMPACTION_INTERVAL_SEC_DEFAULT, FLASH_COMPACTION_INTERVAL_SEC_MIN, FLASH_COMPACTION_INTERVAL_SEC_MAX, ConfigurationFlags::DEFAULT, None],
        ],
        string: [
            ["path", &*FLASH_PATH, FLASH_PATH_DEFAULT, ConfigurationFlags::IMMUTABLE, None],
        ],
        bool: [],
        enum: [
            ["sync", &*FLASH_SYNC, SyncMode::everysec, ConfigurationFlags::DEFAULT, None],
        ],
        module_args_as_configuration: true,
    ]
}
