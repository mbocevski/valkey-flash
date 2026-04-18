use std::sync::atomic::{AtomicI64, Ordering};
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
    if v == 0 {
        num_cpus::get()
    } else {
        v as usize
    }
}

// ── flash.io-uring-entries ────────────────────────────────────────────────────

pub const FLASH_IO_URING_ENTRIES_DEFAULT: i64 = 256;
pub const FLASH_IO_URING_ENTRIES_MIN: i64 = 16;
pub const FLASH_IO_URING_ENTRIES_MAX: i64 = 65536;

/// io_uring submission-queue depth. Immutable after module load.
pub static FLASH_IO_URING_ENTRIES: AtomicI64 = AtomicI64::new(FLASH_IO_URING_ENTRIES_DEFAULT);

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
}
