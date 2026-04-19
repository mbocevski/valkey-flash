use std::sync::atomic::{AtomicU64, Ordering};

use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

use crate::CACHE;
use crate::types::Tier;
use crate::types::hash::{FLASH_HASH_TYPE, FlashHashObject, hash_deserialize, hash_serialize};

pub static HGET_HITS: AtomicU64 = AtomicU64::new(0);
pub static HGET_MISSES: AtomicU64 = AtomicU64::new(0);

// ── HGetCompletionHandle ──────────────────────────────────────────────────────

#[cfg(not(test))]
struct HGetCompletionHandle {
    tsc: valkey_module::ThreadSafeContext<valkey_module::BlockedClient<()>>,
    field: Vec<u8>,
    key: Vec<u8>,
    cache: &'static crate::storage::cache::FlashCache,
}

#[cfg(not(test))]
impl HGetCompletionHandle {
    fn new(
        bc: valkey_module::BlockedClient<()>,
        field: Vec<u8>,
        key: Vec<u8>,
        cache: &'static crate::storage::cache::FlashCache,
    ) -> Self {
        HGetCompletionHandle {
            tsc: valkey_module::ThreadSafeContext::with_blocked_client(bc),
            field,
            key,
            cache,
        }
    }
}

#[cfg(not(test))]
impl crate::async_io::CompletionHandle for HGetCompletionHandle {
    fn complete(self: Box<Self>, result: crate::storage::backend::StorageResult<Vec<u8>>) {
        let reply = match result {
            Err(e) => Err(valkey_module::ValkeyError::String(e.to_string())),
            Ok(bytes) => {
                self.cache.put(&self.key, bytes.clone());
                let val = hash_deserialize(&bytes).and_then(|m| m.get(&self.field).cloned());
                Ok(match val {
                    Some(v) => ValkeyValue::StringBuffer(v),
                    None => ValkeyValue::Null,
                })
            }
        };
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.tsc.reply(reply);
        }));
    }
}

// ── Command handler ───────────────────────────────────────────────────────────

/// `FLASH.HGET key field`
///
/// Return the value of `field` in the hash stored at `key`, or nil.
pub fn flash_hget_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() != 3 {
        return Err(ValkeyError::WrongArity);
    }

    let key = &args[1];
    let field = args[2].as_slice().to_vec();
    let cache = CACHE
        .get()
        .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;

    // Type-check the key FIRST: Valkey's open_key triggers the expiry check, so
    // if the key has TTL-expired, open_key returns None and we reply nil.
    // Consulting the cache without this check would return stale data for
    // expired keys (the cache isn't evicted when Valkey drops the key).
    let tier_data: Option<(Option<Vec<u8>>, Vec<u8>)>; // Hot: (field_val, serialized)
    let cold_info: Option<(u64, u32)>;

    {
        let key_handle = ctx.open_key(key);
        let obj = match key_handle.get_value::<FlashHashObject>(&FLASH_HASH_TYPE) {
            Err(_) => return Err(ValkeyError::WrongType),
            Ok(None) => {
                // Key is expired or never existed — ensure stale cache entry
                // doesn't linger (defensive; Valkey's expiry hook should have
                // dropped the object already, which the cache tracks).
                cache.delete(key.as_slice());
                return Ok(ValkeyValue::Null);
            }
            Ok(Some(obj)) => obj,
        };

        // Key exists and is the right type. Now cache-optimize the common case.
        if let Some(cached_bytes) = cache.get(key.as_slice()) {
            HGET_HITS.fetch_add(1, Ordering::Relaxed);
            let val = hash_deserialize(&cached_bytes).and_then(|m| m.get(&field).cloned());
            return Ok(match val {
                Some(v) => ValkeyValue::StringBuffer(v),
                None => ValkeyValue::Null,
            });
        }
        HGET_MISSES.fetch_add(1, Ordering::Relaxed);

        match &obj.tier {
            Tier::Hot(fields) => {
                let val = fields.get(&field).cloned();
                let serialized = hash_serialize(fields);
                tier_data = Some((val, serialized));
                cold_info = None;
            }
            Tier::Cold {
                backend_offset,
                value_len,
                ..
            } => {
                tier_data = None;
                cold_info = Some((*backend_offset, *value_len));
            }
        }
        // key_handle and obj dropped here
    }

    if let Some((field_val, serialized)) = tier_data {
        cache.put(key.as_slice(), serialized);
        return Ok(match field_val {
            Some(v) => ValkeyValue::StringBuffer(v),
            None => ValkeyValue::Null,
        });
    }

    let (backend_offset, value_len) = cold_info.unwrap();

    #[cfg(not(test))]
    {
        let storage = crate::STORAGE
            .get()
            .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;
        let pool = crate::POOL
            .get()
            .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;
        let key_bytes = key.as_slice().to_vec();
        let bc = ctx.block_client();
        let handle = Box::new(HGetCompletionHandle::new(bc, field, key_bytes, cache));
        pool.submit_or_complete(handle, move || {
            storage.read_at_offset(backend_offset, value_len)
        });
        return Ok(ValkeyValue::NoReply);
    }

    #[allow(unused_variables, unreachable_code)]
    {
        let _ = (backend_offset, value_len);
        Ok(ValkeyValue::Null)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_are_readable() {
        let _ = HGET_HITS.load(Ordering::Relaxed);
        let _ = HGET_MISSES.load(Ordering::Relaxed);
    }
}
