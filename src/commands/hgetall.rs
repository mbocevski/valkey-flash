use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

use crate::types::hash::{
    hash_deserialize_or_warn, hash_serialize, FlashHashObject, FLASH_HASH_TYPE,
};
use crate::types::Tier;
use crate::CACHE;

// ── HGetAllCompletionHandle ───────────────────────────────────────────────────

#[cfg(not(test))]
struct HGetAllCompletionHandle {
    tsc: valkey_module::ThreadSafeContext<valkey_module::BlockedClient<()>>,
    key: Vec<u8>,
    cache: &'static crate::storage::cache::FlashCache,
}

#[cfg(not(test))]
impl HGetAllCompletionHandle {
    fn new(
        bc: valkey_module::BlockedClient<()>,
        key: Vec<u8>,
        cache: &'static crate::storage::cache::FlashCache,
    ) -> Self {
        HGetAllCompletionHandle {
            tsc: valkey_module::ThreadSafeContext::with_blocked_client(bc),
            key,
            cache,
        }
    }
}

#[cfg(not(test))]
impl crate::async_io::CompletionHandle for HGetAllCompletionHandle {
    fn complete(self: Box<Self>, result: crate::storage::backend::StorageResult<Vec<u8>>) {
        let reply = match result {
            Err(e) => Err(valkey_module::ValkeyError::String(e.to_string())),
            Ok(bytes) => {
                self.cache.put(&self.key, bytes.clone());
                Ok(fields_to_array(hash_deserialize_or_warn(&bytes)))
            }
        };
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.tsc.reply(reply);
        }));
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn fields_to_array(fields: std::collections::HashMap<Vec<u8>, Vec<u8>>) -> ValkeyValue {
    let mut pairs: Vec<(&Vec<u8>, &Vec<u8>)> = fields.iter().collect();
    pairs.sort_unstable_by_key(|(a, _)| *a);
    let mut arr = Vec::with_capacity(pairs.len() * 2);
    for (k, v) in pairs {
        arr.push(ValkeyValue::StringBuffer(k.clone()));
        arr.push(ValkeyValue::StringBuffer(v.clone()));
    }
    ValkeyValue::Array(arr)
}

// ── Command handler ───────────────────────────────────────────────────────────

/// `FLASH.HGETALL key`
///
/// Return all field-value pairs in the hash as a flat array:
/// `[field1, val1, field2, val2, ...]`. Returns an empty array for a
/// non-existent key.
pub fn flash_hgetall_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() != 2 {
        return Err(ValkeyError::WrongArity);
    }

    let key = &args[1];
    let cache = CACHE
        .get()
        .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;

    // Cache hit: deserialise and return all pairs.
    if let Some(cached_bytes) = cache.get(key.as_slice()) {
        return Ok(fields_to_array(hash_deserialize_or_warn(&cached_bytes)));
    }

    let cold_info: Option<(u64, u32)>;
    let hot_reply: Option<ValkeyValue>;

    {
        let key_handle = ctx.open_key(key);
        let obj = match key_handle.get_value::<FlashHashObject>(&FLASH_HASH_TYPE) {
            Err(_) => return Err(ValkeyError::WrongType),
            Ok(None) => return Ok(ValkeyValue::Array(vec![])),
            Ok(Some(obj)) => obj,
        };

        match &obj.tier {
            Tier::Hot(fields) => {
                hot_reply = Some(fields_to_array(fields.clone()));
                cold_info = None;
            }
            Tier::Cold {
                backend_offset,
                value_len,
                ..
            } => {
                cold_info = Some((*backend_offset, *value_len));
                hot_reply = None;
            }
        }
    }

    if let Some(reply) = hot_reply {
        let key_handle = ctx.open_key(key);
        if let Ok(Some(obj)) = key_handle.get_value::<FlashHashObject>(&FLASH_HASH_TYPE) {
            if let Tier::Hot(fields) = &obj.tier {
                cache.put(key.as_slice(), hash_serialize(fields));
            }
        }
        return Ok(reply);
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
        let handle = Box::new(HGetAllCompletionHandle::new(bc, key_bytes, cache));
        pool.submit_or_complete(handle, move || {
            storage.read_at_offset(backend_offset, value_len)
        });
        return Ok(ValkeyValue::NoReply);
    }

    #[allow(unused_variables, unreachable_code)]
    {
        let _ = (backend_offset, value_len);
        Ok(ValkeyValue::Array(vec![]))
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn empty_fields_gives_empty_array() {
        let m: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        match fields_to_array(m) {
            ValkeyValue::Array(v) => assert!(v.is_empty()),
            other => panic!("expected Array, got {other:?}"),
        }
    }

    #[test]
    fn fields_to_array_interleaves_field_value() {
        let mut m = HashMap::new();
        m.insert(b"f1".to_vec(), b"v1".to_vec());
        let arr = match fields_to_array(m) {
            ValkeyValue::Array(v) => v,
            other => panic!("expected Array, got {other:?}"),
        };
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0], ValkeyValue::StringBuffer(b"f1".to_vec()));
        assert_eq!(arr[1], ValkeyValue::StringBuffer(b"v1".to_vec()));
    }

    #[test]
    fn fields_to_array_is_sorted() {
        let mut m = HashMap::new();
        m.insert(b"z".to_vec(), b"last".to_vec());
        m.insert(b"a".to_vec(), b"first".to_vec());
        let arr = match fields_to_array(m) {
            ValkeyValue::Array(v) => v,
            other => panic!("expected Array, got {other:?}"),
        };
        // Sorted: a before z
        assert_eq!(arr[0], ValkeyValue::StringBuffer(b"a".to_vec()));
        assert_eq!(arr[2], ValkeyValue::StringBuffer(b"z".to_vec()));
    }
}
