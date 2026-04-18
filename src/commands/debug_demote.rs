use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

use crate::storage::backend::StorageBackend;
use crate::storage::file_io_uring::FileIoUringBackend;
use crate::types::hash::{hash_serialize, FlashHashObject, FLASH_HASH_TYPE};
use crate::types::string::{FlashStringObject, FLASH_STRING_TYPE};
use crate::types::Tier;
use crate::{CACHE, STORAGE, TIERING_MAP, WAL};

/// `FLASH.DEBUG.DEMOTE key`
///
/// Force-demote a hot-tier flash-string or flash-hash key to cold tier (NVMe).
/// Intended for integration tests that need to exercise cold-tier read and
/// TTL-expiry paths without real eviction pressure.
///
/// **Production note:** This command should be restricted via ACL in production
/// deployments: `ACL SETUSER default -FLASH.DEBUG.DEMOTE`. The NVMe write runs
/// synchronously on the event loop, which is acceptable for tests but not for
/// production traffic.
///
/// Steps:
///   1. Probe the key as FlashStringObject, then FlashHashObject.
///   2. Serialize the hot value to bytes (raw for strings; `hash_serialize` for hashes).
///   3. Write to NVMe via `storage.put()` (returns byte offset).
///   4. Remove from STORAGE index (`remove_from_index`): ownership transfers to
///      `Tier::Cold`, so the index entry is no longer needed. This prevents a
///      double-free when TTL expiry later calls `free()` (which reclaims via
///      `release_cold_blocks`). Cold reads use `read_at_offset` directly.
///   5. WAL-log the put so recovery can rebuild tiering state.
///   6. Update TIERING_MAP with the new cold entry.
///   7. Mutate the in-memory object to `Tier::Cold`.
///   8. Evict the key from the hot cache.
pub fn flash_debug_demote_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() != 2 {
        return Err(ValkeyError::WrongArity);
    }

    let key = &args[1];
    let storage = STORAGE
        .get()
        .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;

    let key_handle = ctx.open_key_writable(key);

    // ── Try flash-string ──────────────────────────────────────────────────────

    match key_handle.get_value::<FlashStringObject>(&FLASH_STRING_TYPE) {
        Ok(Some(obj)) => {
            let value = match &obj.tier {
                Tier::Hot(v) => v.clone(),
                Tier::Cold { .. } => return Ok(ValkeyValue::SimpleStringStatic("ALREADY_COLD")),
            };
            return demote_bytes(key, storage, &value, &mut obj.tier);
        }
        Ok(None) => return Err(ValkeyError::Str("ERR key does not exist")),
        Err(_) => {} // wrong type for string — fall through to hash probe
    }

    // ── Try flash-hash ────────────────────────────────────────────────────────

    match key_handle.get_value::<FlashHashObject>(&FLASH_HASH_TYPE) {
        Ok(Some(obj)) => {
            let bytes = match &obj.tier {
                Tier::Hot(fields) => hash_serialize(fields),
                Tier::Cold { .. } => return Ok(ValkeyValue::SimpleStringStatic("ALREADY_COLD")),
            };
            demote_bytes(key, storage, &bytes, &mut obj.tier)
        }
        Ok(None) => Err(ValkeyError::Str("ERR key does not exist")),
        Err(_) => Err(ValkeyError::WrongType),
    }
}

// ── Shared demotion logic ─────────────────────────────────────────────────────

fn demote_bytes<T>(
    key: &valkey_module::ValkeyString,
    storage: &FileIoUringBackend,
    bytes: &[u8],
    tier: &mut Tier<T>,
) -> ValkeyResult {
    let key_bytes = key.as_slice();
    let kh = crate::util::key_hash(key_bytes);
    let vh = crate::util::value_hash(bytes);
    let num_blocks = FileIoUringBackend::blocks_needed(bytes.len());
    let value_len = bytes.len() as u32;

    let backend_offset = storage
        .put(key_bytes, bytes)
        .map_err(|e| ValkeyError::String(e.to_string()))?;

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

    *tier = Tier::Cold {
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
