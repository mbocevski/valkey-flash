use valkey_module::{Context, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

use crate::STORAGE;
use crate::demotion::demote_bytes;
use crate::types::Tier;
use crate::types::hash::{FLASH_HASH_TYPE, FlashHashObject, hash_serialize};
use crate::types::list::{FLASH_LIST_TYPE, FlashListObject, list_serialize};
use crate::types::string::{FLASH_STRING_TYPE, FlashStringObject};
use crate::types::zset::{FLASH_ZSET_TYPE, FlashZSetObject, zset_serialize};

/// `FLASH.DEBUG.DEMOTE key`
///
/// Force-demote a hot-tier flash key (string / hash / list / zset) to cold
/// tier (NVMe). Intended for integration tests that need to exercise
/// cold-tier read and TTL-expiry paths deterministically without waiting on
/// real cache-pressure-driven eviction.
///
/// For the automatic production path see `crate::demotion` — a 100 ms
/// event-loop timer that drains the cache's candidate queue whenever the
/// RAM hot tier is over capacity. The shared [`demote_bytes`] helper applies
/// the same atomic NVMe-write + tiering-map + WAL + cache-eviction sequence
/// for both paths.
///
/// **Production note:** This command remains available but should be
/// restricted via ACL in production deployments:
/// `ACL SETUSER default -FLASH.DEBUG.DEMOTE`. The NVMe write runs
/// synchronously on the event loop — acceptable for tests and for low-rate
/// admin use, but not for hot-path traffic.
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
            let result = demote_bytes(key, storage, &value, &mut obj.tier);
            if result.is_ok() {
                ctx.notify_keyspace_event(NotifyEvent::GENERIC, "flash.evict", key);
            }
            return result;
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
            let result = demote_bytes(key, storage, &bytes, &mut obj.tier);
            if result.is_ok() {
                ctx.notify_keyspace_event(NotifyEvent::GENERIC, "flash.evict", key);
            }
            return result;
        }
        Ok(None) => return Err(ValkeyError::Str("ERR key does not exist")),
        Err(_) => {} // wrong type for hash — fall through to list probe
    }

    // ── Try flash-list ────────────────────────────────────────────────────────

    match key_handle.get_value::<FlashListObject>(&FLASH_LIST_TYPE) {
        Ok(Some(obj)) => {
            let bytes = match &obj.tier {
                Tier::Hot(items) => list_serialize(items),
                Tier::Cold { .. } => return Ok(ValkeyValue::SimpleStringStatic("ALREADY_COLD")),
            };
            let result = demote_bytes(key, storage, &bytes, &mut obj.tier);
            if result.is_ok() {
                ctx.notify_keyspace_event(NotifyEvent::GENERIC, "flash.evict", key);
            }
            return result;
        }
        Ok(None) => return Err(ValkeyError::Str("ERR key does not exist")),
        Err(_) => {} // wrong type for list — fall through to zset probe
    }

    // ── Try flash-zset ────────────────────────────────────────────────────────

    match key_handle.get_value::<FlashZSetObject>(&FLASH_ZSET_TYPE) {
        Ok(Some(obj)) => {
            let bytes = match &obj.tier {
                Tier::Hot(inner) => zset_serialize(inner),
                Tier::Cold { .. } => return Ok(ValkeyValue::SimpleStringStatic("ALREADY_COLD")),
            };
            let result = demote_bytes(key, storage, &bytes, &mut obj.tier);
            if result.is_ok() {
                ctx.notify_keyspace_event(NotifyEvent::GENERIC, "flash.evict", key);
            }
            result
        }
        Ok(None) => Err(ValkeyError::Str("ERR key does not exist")),
        Err(_) => Err(ValkeyError::WrongType),
    }
}
