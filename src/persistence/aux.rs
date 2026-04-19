use std::os::raw::c_int;
use std::sync::{LazyLock, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use valkey_module::{logging, raw};

use crate::storage::BlockRange;

// ── Wire format constants ─────────────────────────────────────────────────────

// "FLSX" little-endian
const AUX_MAGIC: u32 = 0x5853_4C46;
const AUX_ENCODING_VERSION: u8 = 1;

// when values from Valkey (mirrors raw::Aux)
const AUX_BEFORE_RDB: c_int = 1;
const AUX_AFTER_RDB: c_int = 2;

// Shared encoding config for all aux payloads.  `legacy()` matches bincode 1.x
// default options (fixed-int LE, no size limit) — required for byte-compatible
// reads of RDB files written by earlier module builds.
fn cfg() -> impl bincode::config::Config {
    bincode::config::legacy()
}

// ── Serialisable structs ──────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuxTierEntry {
    pub key_hash: u64,
    /// 0 = Hot, 1 = Cold
    pub tier_tag: u8,
    pub bytes: u64,
}

/// v1 shape (no free_blocks / nvme_next_block) — used as fallback when loading
/// an RDB written by an older module build.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct AuxBeforePayloadV1 {
    magic: u32,
    version: u8,
    entries: Vec<AuxTierEntry>,
    path: String,
    capacity_bytes: u64,
    io_uring_entries: u32,
    wal_cursor: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuxBeforePayload {
    pub magic: u32,
    pub version: u8,
    pub entries: Vec<AuxTierEntry>,
    pub path: String,
    pub capacity_bytes: u64,
    pub io_uring_entries: u32,
    pub wal_cursor: u64,
    /// Last value of the NVMe bump-allocator pointer at save time.
    pub nvme_next_block: u64,
    /// Free-list snapshot at save time; seeded back on restart.
    pub free_blocks: Vec<BlockRange>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuxAfterPayload {
    pub magic: u32,
    pub version: u8,
    pub saved_at_unix_ms: u64,
    /// Placeholder; Valkey computes the real RDB CRC separately.
    pub rdb_crc: u64,
}

/// Full aux state reconstructed from both phases of RDB load.
#[derive(Debug, Clone)]
pub struct AuxState {
    pub before: AuxBeforePayload,
    pub after: Option<AuxAfterPayload>,
}

// ── Module-global loaded aux state ────────────────────────────────────────────

/// Populated by `aux_load` during RDB restore; consumed by the recovery flow
/// (task #39). Initialised lazily on first access.
pub static LOADED_AUX_STATE: LazyLock<Mutex<Option<AuxState>>> = LazyLock::new(|| Mutex::new(None));

// ── aux_save ──────────────────────────────────────────────────────────────────

/// Called by Valkey before and after RDB save for `FLASH_STRING_TYPE`.
///
/// # Safety
pub unsafe extern "C" fn aux_save(io: *mut raw::RedisModuleIO, when: c_int) {
    use crate::config::{FLASH_CAPACITY_BYTES, FLASH_IO_URING_ENTRIES, FLASH_PATH};
    use std::sync::atomic::Ordering;

    if when == AUX_BEFORE_RDB {
        let path = match FLASH_PATH.lock() {
            Ok(g) => g.clone(),
            Err(_) => {
                logging::log_warning("flash: aux_save BEFORE: FLASH_PATH lock poisoned");
                return;
            }
        };
        let capacity_bytes = FLASH_CAPACITY_BYTES.load(Ordering::Relaxed) as u64;
        let io_uring_entries = FLASH_IO_URING_ENTRIES.load(Ordering::Relaxed) as u32;

        // Snapshot the current WAL write position so recovery can skip records
        // that are already reflected in this RDB snapshot.
        let wal_cursor = crate::WAL
            .get()
            .and_then(|wal| wal.current_offset().ok())
            .unwrap_or(0);

        // Snapshot NVMe allocator state for cross-session free-list reclaim.
        let nvme_next_block = crate::STORAGE
            .get()
            .map(|s| s.next_block_snapshot())
            .unwrap_or(0);
        let free_blocks = crate::STORAGE
            .get()
            .map(|s| s.free_list_snapshot())
            .unwrap_or_default();

        let payload = AuxBeforePayload {
            magic: AUX_MAGIC,
            version: AUX_ENCODING_VERSION,
            // Tiering map populated by demotion (future task).
            entries: Vec::new(),
            path,
            capacity_bytes,
            io_uring_entries,
            wal_cursor,
            nvme_next_block,
            free_blocks,
        };

        match bincode::serde::encode_to_vec(&payload, cfg()) {
            Ok(bytes) => raw::save_slice(io, &bytes),
            Err(e) => {
                logging::log_warning(
                    format!("flash: aux_save BEFORE: serialization error: {e}").as_str(),
                );
            }
        }
    } else if when == AUX_AFTER_RDB {
        let saved_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let payload = AuxAfterPayload {
            magic: AUX_MAGIC,
            version: AUX_ENCODING_VERSION,
            saved_at_unix_ms: saved_at,
            rdb_crc: 0,
        };

        match bincode::serde::encode_to_vec(&payload, cfg()) {
            Ok(bytes) => raw::save_slice(io, &bytes),
            Err(e) => {
                logging::log_warning(
                    format!("flash: aux_save AFTER: serialization error: {e}").as_str(),
                );
            }
        }
    }
}

// ── aux_load ──────────────────────────────────────────────────────────────────

/// Called by Valkey when loading an RDB that contains `FLASH_STRING_TYPE` aux data.
/// Returns `REDISMODULE_OK` (0) on success, `REDISMODULE_ERR` (-1) on failure.
///
/// # Safety
pub unsafe extern "C" fn aux_load(
    io: *mut raw::RedisModuleIO,
    encver: c_int,
    when: c_int,
) -> c_int {
    if encver > 1 {
        logging::log_warning(
            format!("flash: aux_load: unsupported encver {encver} (max supported: 1)").as_str(),
        );
        return raw::Status::Err as c_int;
    }

    let buf = match raw::load_string_buffer(io) {
        Ok(b) => b,
        Err(_) => {
            logging::log_warning("flash: aux_load: failed to read aux buffer from RDB");
            return raw::Status::Err as c_int;
        }
    };
    let bytes = buf.as_ref();

    if when == AUX_BEFORE_RDB {
        // Try current (v2) shape first; fall back to v1 (no nvme_next_block /
        // free_blocks) for RDB files written by an older module build.
        let payload: AuxBeforePayload =
            match bincode::serde::decode_from_slice(bytes, cfg()) {
                Ok((p, _)) => p,
                Err(_) => {
                    match bincode::serde::decode_from_slice::<AuxBeforePayloadV1, _>(
                        bytes,
                        cfg(),
                    ) {
                        Ok((v1, _)) => {
                            logging::log_notice(
                                "flash: aux_load BEFORE: v1 aux detected, promoting (free_blocks=empty)",
                            );
                            AuxBeforePayload {
                                magic: v1.magic,
                                version: v1.version,
                                entries: v1.entries,
                                path: v1.path,
                                capacity_bytes: v1.capacity_bytes,
                                io_uring_entries: v1.io_uring_entries,
                                wal_cursor: v1.wal_cursor,
                                nvme_next_block: 0,
                                free_blocks: Vec::new(),
                            }
                        }
                        Err(e) => {
                            logging::log_warning(
                                format!(
                                    "flash: aux_load BEFORE: deserialization error: {e}"
                                )
                                .as_str(),
                            );
                            return raw::Status::Err as c_int;
                        }
                    }
                }
            };

        if payload.magic != AUX_MAGIC {
            logging::log_warning(
                format!(
                    "flash: aux_load BEFORE: bad magic {:#010x} (expected {:#010x})",
                    payload.magic, AUX_MAGIC
                )
                .as_str(),
            );
            return raw::Status::Err as c_int;
        }
        if payload.version > AUX_ENCODING_VERSION {
            logging::log_warning(
                format!(
                    "flash: aux_load BEFORE: unsupported version {} (max supported: {})",
                    payload.version, AUX_ENCODING_VERSION
                )
                .as_str(),
            );
            return raw::Status::Err as c_int;
        }

        logging::log_notice(
            format!(
                "flash: aux_load BEFORE: loaded {} tiering entries, path='{}', \
                 capacity={}, wal_cursor={}, nvme_next_block={}, free_ranges={}",
                payload.entries.len(),
                payload.path,
                payload.capacity_bytes,
                payload.wal_cursor,
                payload.nvme_next_block,
                payload.free_blocks.len(),
            )
            .as_str(),
        );

        if let Ok(mut guard) = LOADED_AUX_STATE.lock() {
            let state = guard.get_or_insert_with(|| AuxState {
                before: payload.clone(),
                after: None,
            });
            state.before = payload;
        }
    } else if when == AUX_AFTER_RDB {
        let payload: AuxAfterPayload =
            match bincode::serde::decode_from_slice(bytes, cfg()) {
                Ok((p, _)) => p,
                Err(e) => {
                    logging::log_warning(
                        format!("flash: aux_load AFTER: deserialization error: {e}").as_str(),
                    );
                    return raw::Status::Err as c_int;
                }
            };

        if payload.magic != AUX_MAGIC {
            logging::log_warning(
                format!(
                    "flash: aux_load AFTER: bad magic {:#010x} (expected {:#010x})",
                    payload.magic, AUX_MAGIC
                )
                .as_str(),
            );
            return raw::Status::Err as c_int;
        }
        if payload.version > AUX_ENCODING_VERSION {
            logging::log_warning(
                format!(
                    "flash: aux_load AFTER: unsupported version {} (max supported: {})",
                    payload.version, AUX_ENCODING_VERSION
                )
                .as_str(),
            );
            return raw::Status::Err as c_int;
        }

        logging::log_notice(
            format!(
                "flash: aux_load AFTER: saved_at_unix_ms={}",
                payload.saved_at_unix_ms
            )
            .as_str(),
        );

        if let Ok(mut guard) = LOADED_AUX_STATE.lock()
            && let Some(state) = guard.as_mut() {
                state.after = Some(payload);
            }
    }

    raw::Status::Ok as c_int
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_before(n_entries: usize, version: u8, magic: u32) -> Vec<u8> {
        let payload = AuxBeforePayload {
            magic,
            version,
            entries: (0..n_entries as u64)
                .map(|i| AuxTierEntry {
                    key_hash: i,
                    tier_tag: 0,
                    bytes: 1024,
                })
                .collect(),
            path: "/tmp/test.bin".to_string(),
            capacity_bytes: 1 << 30,
            io_uring_entries: 256,
            wal_cursor: 42,
            nvme_next_block: 0,
            free_blocks: Vec::new(),
        };
        bincode::serde::encode_to_vec(&payload, cfg()).unwrap()
    }

    fn make_before_v1(n_entries: usize, version: u8, magic: u32) -> Vec<u8> {
        let payload = AuxBeforePayloadV1 {
            magic,
            version,
            entries: (0..n_entries as u64)
                .map(|i| AuxTierEntry {
                    key_hash: i,
                    tier_tag: 0,
                    bytes: 1024,
                })
                .collect(),
            path: "/tmp/test.bin".to_string(),
            capacity_bytes: 1 << 30,
            io_uring_entries: 256,
            wal_cursor: 99,
        };
        bincode::serde::encode_to_vec(&payload, cfg()).unwrap()
    }

    fn make_after(version: u8, magic: u32) -> Vec<u8> {
        let payload = AuxAfterPayload {
            magic,
            version,
            saved_at_unix_ms: 1_700_000_000_000,
            rdb_crc: 0,
        };
        bincode::serde::encode_to_vec(&payload, cfg()).unwrap()
    }

    #[test]
    fn before_roundtrip_empty_entries() {
        let bytes = make_before(0, AUX_ENCODING_VERSION, AUX_MAGIC);
        let decoded: AuxBeforePayload =
            bincode::serde::decode_from_slice(&bytes, cfg()).unwrap().0;
        assert_eq!(decoded.magic, AUX_MAGIC);
        assert_eq!(decoded.version, AUX_ENCODING_VERSION);
        assert!(decoded.entries.is_empty());
        assert_eq!(decoded.wal_cursor, 42);
    }

    #[test]
    fn before_roundtrip_with_entries() {
        let bytes = make_before(10, AUX_ENCODING_VERSION, AUX_MAGIC);
        let decoded: AuxBeforePayload =
            bincode::serde::decode_from_slice(&bytes, cfg()).unwrap().0;
        assert_eq!(decoded.entries.len(), 10);
        for (i, entry) in decoded.entries.iter().enumerate() {
            assert_eq!(entry.key_hash, i as u64);
            assert_eq!(entry.tier_tag, 0);
            assert_eq!(entry.bytes, 1024);
        }
    }

    #[test]
    fn after_roundtrip() {
        let bytes = make_after(AUX_ENCODING_VERSION, AUX_MAGIC);
        let decoded: AuxAfterPayload =
            bincode::serde::decode_from_slice(&bytes, cfg()).unwrap().0;
        assert_eq!(decoded.magic, AUX_MAGIC);
        assert_eq!(decoded.version, AUX_ENCODING_VERSION);
        assert_eq!(decoded.saved_at_unix_ms, 1_700_000_000_000);
    }

    #[test]
    fn bad_magic_detected_before() {
        let bytes = make_before(0, AUX_ENCODING_VERSION, 0xDEADBEEF);
        let decoded: AuxBeforePayload =
            bincode::serde::decode_from_slice(&bytes, cfg()).unwrap().0;
        assert_ne!(decoded.magic, AUX_MAGIC);
    }

    #[test]
    fn bad_magic_detected_after() {
        let bytes = make_after(AUX_ENCODING_VERSION, 0xDEADBEEF);
        let decoded: AuxAfterPayload =
            bincode::serde::decode_from_slice(&bytes, cfg()).unwrap().0;
        assert_ne!(decoded.magic, AUX_MAGIC);
    }

    #[test]
    fn future_version_detected_before() {
        let bytes = make_before(0, 99, AUX_MAGIC);
        let decoded: AuxBeforePayload =
            bincode::serde::decode_from_slice(&bytes, cfg()).unwrap().0;
        assert!(decoded.version > AUX_ENCODING_VERSION);
    }

    #[test]
    fn future_version_detected_after() {
        let bytes = make_after(99, AUX_MAGIC);
        let decoded: AuxAfterPayload =
            bincode::serde::decode_from_slice(&bytes, cfg()).unwrap().0;
        assert!(decoded.version > AUX_ENCODING_VERSION);
    }

    #[test]
    fn tier_tag_values() {
        // Hot = 0, Cold = 1 — verify tag range
        let hot = AuxTierEntry {
            key_hash: 1,
            tier_tag: 0,
            bytes: 512,
        };
        let cold = AuxTierEntry {
            key_hash: 2,
            tier_tag: 1,
            bytes: 4096,
        };
        assert_eq!(hot.tier_tag, 0);
        assert_eq!(cold.tier_tag, 1);
    }

    #[test]
    fn aux_encoding_version_is_one() {
        assert_eq!(AUX_ENCODING_VERSION, 1);
    }

    #[test]
    fn aux_magic_constant() {
        // "FLSX" in little-endian: 0x46='F', 0x4C='L', 0x53='S', 0x58='X'
        assert_eq!(AUX_MAGIC, 0x5853_4C46);
    }

    #[test]
    fn before_roundtrip_with_free_blocks() {
        let free = vec![
            BlockRange { start: 5, len: 2 },
            BlockRange { start: 10, len: 3 },
        ];
        let payload = AuxBeforePayload {
            magic: AUX_MAGIC,
            version: AUX_ENCODING_VERSION,
            entries: Vec::new(),
            path: "/tmp/test.bin".to_string(),
            capacity_bytes: 1 << 30,
            io_uring_entries: 256,
            wal_cursor: 0,
            nvme_next_block: 20,
            free_blocks: free.clone(),
        };
        let bytes = bincode::serde::encode_to_vec(&payload, cfg()).unwrap();
        let decoded: AuxBeforePayload =
            bincode::serde::decode_from_slice(&bytes, cfg()).unwrap().0;
        assert_eq!(decoded.nvme_next_block, 20);
        assert_eq!(decoded.free_blocks, free);
    }

    #[test]
    fn v1_bytes_deserialize_as_v2_with_empty_free_blocks() {
        // Simulate loading an RDB written by the old module (no free_blocks field).
        let v1_bytes = make_before_v1(3, AUX_ENCODING_VERSION, AUX_MAGIC);
        // Must NOT decode as v2 (it will hit EOF on free_blocks Vec).
        assert!(
            bincode::serde::decode_from_slice::<AuxBeforePayload, _>(&v1_bytes, cfg()).is_err()
        );
        // Must decode as v1 successfully.
        let v1: AuxBeforePayloadV1 =
            bincode::serde::decode_from_slice(&v1_bytes, cfg()).unwrap().0;
        assert_eq!(v1.entries.len(), 3);
        assert_eq!(v1.wal_cursor, 99);
    }

    #[test]
    fn v2_bytes_deserialize_correctly() {
        let v2_bytes = make_before(2, AUX_ENCODING_VERSION, AUX_MAGIC);
        let decoded: AuxBeforePayload =
            bincode::serde::decode_from_slice(&v2_bytes, cfg()).unwrap().0;
        assert_eq!(decoded.entries.len(), 2);
        assert_eq!(decoded.nvme_next_block, 0);
        assert!(decoded.free_blocks.is_empty());
    }

    #[test]
    fn legacy_config_byte_identical_to_bincode1_fixture() {
        // Fixture bytes produced by bincode 1.3.3 `serialize()` for AuxAfterPayload
        // with fixed-int little-endian encoding (bincode 1.x default = bincode 2.x legacy()).
        //
        // Wire layout: magic(u32 LE, 4B) + version(u8, 1B)
        //            + saved_at_unix_ms(u64 LE, 8B) + rdb_crc(u64 LE, 8B) = 21 bytes.
        //
        // 1_700_000_000_000 = 0x0000_018B_CFE5_6800
        // LE bytes:  00 68 E5 CF 8B 01 00 00
        #[rustfmt::skip]
        let expected: &[u8] = &[
            0x46, 0x4C, 0x53, 0x58,                           // magic = AUX_MAGIC (LE)
            0x01,                                               // version = 1
            0x00, 0x68, 0xE5, 0xCF, 0x8B, 0x01, 0x00, 0x00,  // saved_at_unix_ms
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  // rdb_crc = 0
        ];
        let payload = AuxAfterPayload {
            magic: AUX_MAGIC,
            version: 1,
            saved_at_unix_ms: 1_700_000_000_000,
            rdb_crc: 0,
        };
        let encoded = bincode::serde::encode_to_vec(&payload, cfg()).unwrap();
        assert_eq!(
            encoded, expected,
            "bincode 2 legacy config must produce bincode 1.x-identical bytes"
        );
        // Round-trip decode of the fixture bytes.
        let decoded: AuxAfterPayload =
            bincode::serde::decode_from_slice(expected, cfg()).unwrap().0;
        assert_eq!(decoded.magic, AUX_MAGIC);
        assert_eq!(decoded.saved_at_unix_ms, 1_700_000_000_000);
        assert_eq!(decoded.rdb_crc, 0);
    }
}
