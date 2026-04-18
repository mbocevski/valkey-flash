#![no_main]
use libfuzzer_sys::fuzz_target;
use tempfile::tempdir;
use valkey_flash::storage::wal::{Wal, WalSyncMode};

fuzz_target!(|data: &[u8]| {
    let dir = match tempdir() {
        Ok(d) => d,
        Err(_) => return,
    };
    let path = dir.path().join("fuzz.wal");
    if std::fs::write(&path, data).is_err() {
        return;
    }
    // Both header validation and record-frame parsing must never panic.
    if let Ok(wal) = Wal::open(&path, WalSyncMode::No) {
        if let Ok(iter) = wal.iter_records() {
            for result in iter {
                let _ = result;
            }
        }
    }
});
