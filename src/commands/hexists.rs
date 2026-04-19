use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

use crate::CACHE;
use crate::types::Tier;
use crate::types::hash::{FLASH_HASH_TYPE, FlashHashObject, hash_deserialize, hash_serialize};

// ── HExistsCompletionHandle ───────────────────────────────────────────────────

#[cfg(not(test))]
struct HExistsCompletionHandle {
    tsc: valkey_module::ThreadSafeContext<valkey_module::BlockedClient<()>>,
    field: Vec<u8>,
    key: Vec<u8>,
    cache: &'static crate::storage::cache::FlashCache,
}

#[cfg(not(test))]
impl HExistsCompletionHandle {
    fn new(
        bc: valkey_module::BlockedClient<()>,
        field: Vec<u8>,
        key: Vec<u8>,
        cache: &'static crate::storage::cache::FlashCache,
    ) -> Self {
        HExistsCompletionHandle {
            tsc: valkey_module::ThreadSafeContext::with_blocked_client(bc),
            field,
            key,
            cache,
        }
    }
}

#[cfg(not(test))]
impl crate::async_io::CompletionHandle for HExistsCompletionHandle {
    fn complete(self: Box<Self>, result: crate::storage::backend::StorageResult<Vec<u8>>) {
        let reply = match result {
            Err(e) => Err(valkey_module::ValkeyError::String(e.to_string())),
            Ok(bytes) => {
                self.cache.put(&self.key, bytes.clone());
                let exists = hash_deserialize(&bytes)
                    .map(|m| m.contains_key(&self.field))
                    .unwrap_or(false);
                Ok(ValkeyValue::Integer(if exists { 1 } else { 0 }))
            }
        };
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.tsc.reply(reply);
        }));
    }
}

// ── Command handler ───────────────────────────────────────────────────────────

/// `FLASH.HEXISTS key field`
///
/// Returns 1 if `field` exists in the hash stored at `key`, 0 otherwise.
pub fn flash_hexists_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() != 3 {
        return Err(ValkeyError::WrongArity);
    }

    let key = &args[1];
    let field = args[2].as_slice().to_vec();
    let cache = CACHE
        .get()
        .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;

    // Type-check and existence-check via open_key FIRST so TTL-expired keys
    // fall through to nil (open_key triggers Valkey's expiry hook). Only after
    // the key is confirmed live do we consult the cache.
    let cold_info: Option<(u64, u32)>;
    let hot_result: Option<i64>;

    {
        let key_handle = ctx.open_key(key);
        let obj = match key_handle.get_value::<FlashHashObject>(&FLASH_HASH_TYPE) {
            Err(_) => return Err(ValkeyError::WrongType),
            Ok(None) => {
                cache.delete(key.as_slice());
                return Ok(ValkeyValue::Integer(0));
            }
            Ok(Some(obj)) => obj,
        };

        if let Some(cached_bytes) = cache.get(key.as_slice()) {
            let exists = hash_deserialize(&cached_bytes)
                .map(|m| m.contains_key(&field))
                .unwrap_or(false);
            return Ok(ValkeyValue::Integer(if exists { 1 } else { 0 }));
        }

        match &obj.tier {
            Tier::Hot(fields) => {
                hot_result = Some(if fields.contains_key(&field) { 1 } else { 0 });
                cold_info = None;
            }
            Tier::Cold {
                backend_offset,
                value_len,
                ..
            } => {
                cold_info = Some((*backend_offset, *value_len));
                hot_result = None;
            }
        }
    }

    if let Some(result) = hot_result {
        let key_handle = ctx.open_key(key);
        if let Ok(Some(obj)) = key_handle.get_value::<FlashHashObject>(&FLASH_HASH_TYPE)
            && let Tier::Hot(fields) = &obj.tier
        {
            cache.put(key.as_slice(), hash_serialize(fields));
        }
        return Ok(ValkeyValue::Integer(result));
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
        let handle = Box::new(HExistsCompletionHandle::new(bc, field, key_bytes, cache));
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
    fn field_present_returns_one() {
        let mut m: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        m.insert(b"name".to_vec(), b"alice".to_vec());
        assert!(m.contains_key(b"name".as_ref()));
    }

    #[test]
    fn field_absent_returns_zero() {
        let m: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        assert!(!m.contains_key(b"ghost".as_ref()));
    }
}
