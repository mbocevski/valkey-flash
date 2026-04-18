use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

use crate::storage::backend::StorageBackend;
use crate::storage::file_io_uring::FileIoUringBackend;
use crate::types::string::{FlashStringObject, FLASH_STRING_TYPE};
use crate::types::Tier;
use crate::{CACHE, STORAGE, TIERING_MAP, WAL};

/// `FLASH.DEBUG.DEMOTE key`
///
/// Force-demote a hot-tier flash-string key to cold tier (NVMe). Intended for
/// integration tests that need to exercise the cold-tier read and TTL-expiry
/// paths without real eviction pressure.
///
/// Steps:
///   1. Write the hot value to NVMe via `storage.put()` (returns byte offset).
///   2. Remove from STORAGE index (`remove_from_index`): ownership transfers to
///      `Tier::Cold`, so the index entry is no longer needed. This prevents a
///      double-free when TTL expiry later calls `free()` (which reclaims via
///      `release_cold_blocks`). Cold reads use `read_at_offset` directly.
///   3. WAL-log the put so recovery can rebuild tiering state.
///   4. Update TIERING_MAP with the new cold entry.
///   5. Mutate the in-memory object to `Tier::Cold`.
///   6. Evict the key from the hot cache.
pub fn flash_debug_demote_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() != 2 {
        return Err(ValkeyError::WrongArity);
    }

    let key = &args[1];
    let storage = STORAGE
        .get()
        .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;

    let key_handle = ctx.open_key_writable(key);
    let obj = match key_handle.get_value::<FlashStringObject>(&FLASH_STRING_TYPE) {
        Err(_) => return Err(ValkeyError::WrongType),
        Ok(None) => return Err(ValkeyError::Str("ERR key does not exist")),
        Ok(Some(obj)) => obj,
    };

    let (value, _ttl_ms) = match &obj.tier {
        Tier::Hot(v) => (v.clone(), obj.ttl_ms),
        Tier::Cold { .. } => return Ok(ValkeyValue::SimpleStringStatic("ALREADY_COLD")),
    };

    let key_bytes = key.as_slice();
    let kh = crate::util::key_hash(key_bytes);
    let vh = crate::util::value_hash(&value);
    let num_blocks = FileIoUringBackend::blocks_needed(value.len());
    let value_len = value.len() as u32;

    let backend_offset = storage
        .put(key_bytes, &value)
        .map_err(|e| ValkeyError::String(e.to_string()))?;

    // Transfer NVMe block ownership from the STORAGE index to Tier::Cold.
    storage.remove_from_index(key_bytes);

    if let Some(wal) = WAL.get() {
        let _ = wal.append(crate::storage::wal::WalOp::Put {
            key_hash: kh,
            offset: backend_offset,
            value_hash: vh,
        });
    }

    if let Ok(mut map) = TIERING_MAP.lock() {
        map.insert(
            kh,
            crate::recovery::TierEntry {
                offset: backend_offset,
                value_hash: vh,
            },
        );
    }

    obj.tier = Tier::Cold {
        key_hash: kh,
        backend_offset,
        num_blocks,
        value_len,
    };

    if let Some(cache) = CACHE.get() {
        cache.delete(key.as_slice());
    }

    Ok(ValkeyValue::SimpleStringStatic("OK"))
}
