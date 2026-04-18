use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

use crate::types::hash::{hash_deserialize, hash_serialize, FlashHashObject, FLASH_HASH_TYPE};
use crate::types::Tier;
use crate::CACHE;

// ── HLenCompletionHandle ──────────────────────────────────────────────────────

#[cfg(not(test))]
struct HLenCompletionHandle {
    tsc: valkey_module::ThreadSafeContext<valkey_module::BlockedClient<()>>,
    key: Vec<u8>,
    cache: &'static crate::storage::cache::FlashCache,
}

#[cfg(not(test))]
impl HLenCompletionHandle {
    fn new(
        bc: valkey_module::BlockedClient<()>,
        key: Vec<u8>,
        cache: &'static crate::storage::cache::FlashCache,
    ) -> Self {
        HLenCompletionHandle {
            tsc: valkey_module::ThreadSafeContext::with_blocked_client(bc),
            key,
            cache,
        }
    }
}

#[cfg(not(test))]
impl crate::async_io::CompletionHandle for HLenCompletionHandle {
    fn complete(self: Box<Self>, result: crate::storage::backend::StorageResult<Vec<u8>>) {
        let reply = match result {
            Err(e) => Err(valkey_module::ValkeyError::String(e.to_string())),
            Ok(bytes) => {
                self.cache.put(&self.key, bytes.clone());
                let count = hash_deserialize(&bytes)
                    .map(|m| m.len() as i64)
                    .unwrap_or(0);
                Ok(ValkeyValue::Integer(count))
            }
        };
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.tsc.reply(reply);
        }));
    }
}

// ── Command handler ───────────────────────────────────────────────────────────

/// `FLASH.HLEN key`
///
/// Returns the number of fields in the hash stored at `key`, or 0 if the key
/// does not exist.
pub fn flash_hlen_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() != 2 {
        return Err(ValkeyError::WrongArity);
    }

    let key = &args[1];
    let cache = CACHE
        .get()
        .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;

    // Cache hit: count fields without touching keyspace.
    if let Some(cached_bytes) = cache.get(key.as_slice()) {
        let count = hash_deserialize(&cached_bytes)
            .map(|m| m.len() as i64)
            .unwrap_or(0);
        return Ok(ValkeyValue::Integer(count));
    }

    let cold_info: Option<(u64, u32)>;
    let hot_count: Option<i64>;

    {
        let key_handle = ctx.open_key(key);
        let obj = match key_handle.get_value::<FlashHashObject>(&FLASH_HASH_TYPE) {
            Err(_) => return Err(ValkeyError::WrongType),
            Ok(None) => return Ok(ValkeyValue::Integer(0)),
            Ok(Some(obj)) => obj,
        };

        match &obj.tier {
            Tier::Hot(fields) => {
                hot_count = Some(fields.len() as i64);
                cold_info = None;
            }
            Tier::Cold {
                backend_offset,
                value_len,
                ..
            } => {
                cold_info = Some((*backend_offset, *value_len));
                hot_count = None;
            }
        }
    }

    if let Some(count) = hot_count {
        let key_handle = ctx.open_key(key);
        if let Ok(Some(obj)) = key_handle.get_value::<FlashHashObject>(&FLASH_HASH_TYPE) {
            if let Tier::Hot(fields) = &obj.tier {
                cache.put(key.as_slice(), hash_serialize(fields));
            }
        }
        return Ok(ValkeyValue::Integer(count));
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
        let handle = Box::new(HLenCompletionHandle::new(bc, key_bytes, cache));
        pool.submit_or_complete(handle, move || {
            storage.read_at_offset(backend_offset, value_len)
        });
        return Ok(ValkeyValue::NoReply);
    }

    #[allow(unused_variables, unreachable_code)]
    {
        let _ = (backend_offset, value_len);
        Ok(ValkeyValue::Integer(0))
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    #[test]
    fn empty_map_has_len_zero() {
        let m: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        assert_eq!(m.len() as i64, 0);
    }

    #[test]
    fn single_field_map_has_len_one() {
        let mut m: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        m.insert(b"f".to_vec(), b"v".to_vec());
        assert_eq!(m.len() as i64, 1);
    }

    #[test]
    fn multi_field_map_len_is_correct() {
        let mut m: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        m.insert(b"a".to_vec(), b"1".to_vec());
        m.insert(b"b".to_vec(), b"2".to_vec());
        m.insert(b"c".to_vec(), b"3".to_vec());
        assert_eq!(m.len() as i64, 3);
    }
}
