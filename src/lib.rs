use std::sync::atomic::Ordering;
use valkey_module::{
    configuration::ConfigurationFlags, logging, valkey_module, Context, Status, ValkeyString,
};

pub mod async_io;
pub mod commands;
pub mod config;
pub mod storage;
pub mod types;

use crate::async_io::AsyncThreadPool;
use crate::config::SyncMode;
use crate::config::{
    flash_io_threads, FLASH_CACHE_SIZE_BYTES, FLASH_CACHE_SIZE_BYTES_DEFAULT,
    FLASH_CACHE_SIZE_BYTES_MAX, FLASH_CACHE_SIZE_BYTES_MIN, FLASH_CAPACITY_BYTES,
    FLASH_CAPACITY_BYTES_DEFAULT, FLASH_CAPACITY_BYTES_MAX, FLASH_CAPACITY_BYTES_MIN,
    FLASH_IO_THREADS, FLASH_IO_THREADS_DEFAULT, FLASH_IO_THREADS_MAX, FLASH_IO_THREADS_MIN,
    FLASH_IO_URING_ENTRIES, FLASH_IO_URING_ENTRIES_DEFAULT, FLASH_IO_URING_ENTRIES_MAX,
    FLASH_IO_URING_ENTRIES_MIN, FLASH_PATH, FLASH_PATH_DEFAULT, FLASH_SYNC,
};
use crate::storage::cache::FlashCache;
use crate::storage::file_io_uring::FileIoUringBackend;
use crate::types::hash::FLASH_HASH_TYPE;
use crate::types::string::FLASH_STRING_TYPE;

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

    let _ = CACHE.set(FlashCache::new(cache_size));
    let _ = POOL.set(AsyncThreadPool::new(io_threads));

    Status::Ok
}

fn deinitialize(_ctx: &Context) -> Status {
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
    ],
    configurations: [
        i64: [
            // immutable knobs — ConfigurationFlags::IMMUTABLE prevents CONFIG SET after load
            ["capacity-bytes", &FLASH_CAPACITY_BYTES, FLASH_CAPACITY_BYTES_DEFAULT, FLASH_CAPACITY_BYTES_MIN, FLASH_CAPACITY_BYTES_MAX, ConfigurationFlags::IMMUTABLE, None],
            ["io-threads",     &FLASH_IO_THREADS,     FLASH_IO_THREADS_DEFAULT,     FLASH_IO_THREADS_MIN,     FLASH_IO_THREADS_MAX,     ConfigurationFlags::IMMUTABLE, None],
            ["io-uring-entries", &FLASH_IO_URING_ENTRIES, FLASH_IO_URING_ENTRIES_DEFAULT, FLASH_IO_URING_ENTRIES_MIN, FLASH_IO_URING_ENTRIES_MAX, ConfigurationFlags::IMMUTABLE, None],
            // mutable knob
            ["cache-size-bytes", &FLASH_CACHE_SIZE_BYTES, FLASH_CACHE_SIZE_BYTES_DEFAULT, FLASH_CACHE_SIZE_BYTES_MIN, FLASH_CACHE_SIZE_BYTES_MAX, ConfigurationFlags::DEFAULT, None],
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
