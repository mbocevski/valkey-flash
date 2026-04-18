use std::sync::atomic::Ordering;
use valkey_module::{
    configuration::ConfigurationFlags, valkey_module, Context, Status, ValkeyString,
};

pub mod async_io;
pub mod config;
pub mod storage;
pub mod types;

use crate::config::SyncMode;
use crate::config::{
    FLASH_CACHE_SIZE_BYTES, FLASH_CACHE_SIZE_BYTES_DEFAULT, FLASH_CACHE_SIZE_BYTES_MAX,
    FLASH_CACHE_SIZE_BYTES_MIN, FLASH_CAPACITY_BYTES, FLASH_CAPACITY_BYTES_DEFAULT,
    FLASH_CAPACITY_BYTES_MAX, FLASH_CAPACITY_BYTES_MIN, FLASH_IO_THREADS, FLASH_IO_THREADS_DEFAULT,
    FLASH_IO_THREADS_MAX, FLASH_IO_THREADS_MIN, FLASH_IO_URING_ENTRIES,
    FLASH_IO_URING_ENTRIES_DEFAULT, FLASH_IO_URING_ENTRIES_MAX, FLASH_IO_URING_ENTRIES_MIN,
    FLASH_PATH, FLASH_PATH_DEFAULT, FLASH_SYNC,
};
use crate::types::hash::FLASH_HASH_TYPE;
use crate::types::string::FLASH_STRING_TYPE;

pub const MODULE_NAME: &str = "flash";
pub const MODULE_VERSION: i32 = 1;

fn initialize(_ctx: &Context, _args: &[ValkeyString]) -> Status {
    // 0 is the sentinel default for flash.io-threads (auto-detect).
    // Replace it with the actual logical CPU count once the server is running.
    if FLASH_IO_THREADS.load(Ordering::Relaxed) == 0 {
        FLASH_IO_THREADS.store(num_cpus::get() as i64, Ordering::Relaxed);
    }
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
    commands: [],
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
