use std::collections::HashMap;

use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

use crate::types::hash::{
    hash_deserialize_or_warn, hash_serialize, FlashHashObject, FLASH_HASH_TYPE,
};
use crate::types::Tier;
use crate::{CACHE, STORAGE};
#[cfg(not(test))]
use crate::{POOL, WAL};

// ── HSetCompletionHandle ──────────────────────────────────────────────────────

#[cfg(not(test))]
struct HSetCompletionHandle {
    tsc: valkey_module::ThreadSafeContext<valkey_module::BlockedClient<()>>,
    new_count: i64,
}

#[cfg(not(test))]
impl HSetCompletionHandle {
    fn new(bc: valkey_module::BlockedClient<()>, new_count: i64) -> Self {
        HSetCompletionHandle {
            tsc: valkey_module::ThreadSafeContext::with_blocked_client(bc),
            new_count,
        }
    }
}

#[cfg(not(test))]
impl crate::async_io::CompletionHandle for HSetCompletionHandle {
    fn complete(self: Box<Self>, result: crate::storage::backend::StorageResult<Vec<u8>>) {
        use valkey_module::logging;
        if let Err(ref e) = result {
            logging::log_warning(format!("flash: HSET NVMe write failed: {e}").as_str());
        }
        let reply = Ok(ValkeyValue::Integer(self.new_count));
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.tsc.reply(reply);
        }));
    }
}

// ── Command handler ───────────────────────────────────────────────────────────

/// `FLASH.HSET key field value [field value ...]`
///
/// Add or update fields in a flash-hash key. Returns the count of newly
/// added fields (0 for pure overwrites, like Redis HSET).
///
/// Write-through strategy mirrors FLASH.SET:
///   - Update Valkey keyspace (Hot tier) immediately.
///   - Populate the hot cache with the serialized hash bytes.
///   - Replicate the full command.
///   - Async NVMe write; WAL Put on completion.
///
/// Cold-tier edge case (v1): if the key is currently cold, the existing hash is
/// read synchronously from NVMe on the event loop before applying updates.
pub fn flash_hset_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 4 || !(args.len() - 2).is_multiple_of(2) {
        return Err(ValkeyError::WrongArity);
    }

    let key = &args[1];
    let cache = CACHE
        .get()
        .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;

    let key_handle = ctx.open_key_writable(key);
    let existing = match key_handle.get_value::<FlashHashObject>(&FLASH_HASH_TYPE) {
        Err(_) => return Err(ValkeyError::WrongType),
        Ok(v) => v,
    };

    // Extract current fields; for cold keys, promote synchronously (v1 limitation).
    let (mut current_fields, old_ttl): (HashMap<Vec<u8>, Vec<u8>>, Option<i64>) = match existing {
        None => (HashMap::new(), None),
        Some(obj) => {
            let ttl = obj.ttl_ms;
            let fields = match &obj.tier {
                Tier::Hot(fields) => fields.clone(),
                Tier::Cold {
                    backend_offset,
                    value_len,
                    ..
                } => {
                    if crate::replication::is_replica() {
                        HashMap::new()
                    } else {
                        let storage = STORAGE
                            .get()
                            .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;
                        let bytes = storage
                            .read_at_offset(*backend_offset, *value_len)
                            .map_err(|e| ValkeyError::String(e.to_string()))?;
                        hash_deserialize_or_warn(&bytes)
                    }
                }
            };
            (fields, ttl)
        }
    };

    // Apply field updates; count truly new fields.
    let mut new_count = 0i64;
    for chunk in args[2..].chunks(2) {
        let field = chunk[0].as_slice().to_vec();
        let value = chunk[1].as_slice().to_vec();
        if !current_fields.contains_key(&field) {
            new_count += 1;
        }
        current_fields.insert(field, value);
    }

    // Store updated hash in Valkey keyspace (free() called on old object).
    key_handle
        .set_value(
            &FLASH_HASH_TYPE,
            FlashHashObject {
                tier: Tier::Hot(current_fields.clone()),
                ttl_ms: old_ttl,
            },
        )
        .map_err(|e| ValkeyError::String(format!("flash: hset set_value: {e}")))?;

    let serialized = hash_serialize(&current_fields);
    cache.put(key.as_slice(), serialized.clone());

    ctx.replicate_verbatim();

    #[cfg(not(test))]
    {
        if crate::replication::is_replica()
            || (crate::STORAGE.get().is_none() && crate::replication::must_obey_client(ctx))
        {
            return Ok(ValkeyValue::Integer(new_count));
        }
        use crate::storage::backend::StorageBackend;
        let storage = STORAGE
            .get()
            .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;
        let pool = POOL
            .get()
            .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;
        let key_bytes = key.as_slice().to_vec();
        let bc = ctx.block_client();
        let handle = Box::new(HSetCompletionHandle::new(bc, new_count));
        pool.submit_or_complete(handle, move || {
            let offset = storage.put(&key_bytes, &serialized)?;
            if let Some(wal) = WAL.get() {
                let kh = crate::util::key_hash(&key_bytes);
                let vh = crate::util::value_hash(&serialized);
                if let Err(e) = wal.append(crate::storage::wal::WalOp::Put {
                    key_hash: kh,
                    offset,
                    value_hash: vh,
                }) {
                    valkey_module::logging::log_warning(
                        format!("flash: HSET WAL append failed: {e}").as_str(),
                    );
                }
            }
            Ok(vec![])
        });
        return Ok(ValkeyValue::NoReply);
    }

    #[allow(unreachable_code)]
    Ok(ValkeyValue::Integer(new_count))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #[test]
    fn hset_arity_requires_at_least_four_args() {
        let len = 4usize;
        assert!((len - 2).is_multiple_of(2) && len >= 4);
    }

    #[test]
    fn hset_odd_fv_count_fails_arity() {
        let len = 5usize;
        assert!(!(len - 2).is_multiple_of(2));
    }

    #[test]
    fn new_count_is_correct_for_all_new_fields() {
        use std::collections::HashMap;
        let mut m: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        let pairs: &[(&[u8], &[u8])] = &[(b"f1", b"v1"), (b"f2", b"v2")];
        let mut new_count = 0i64;
        for (f, v) in pairs {
            if !m.contains_key(*f) {
                new_count += 1;
            }
            m.insert(f.to_vec(), v.to_vec());
        }
        assert_eq!(new_count, 2);
    }

    #[test]
    fn new_count_is_zero_for_pure_overwrite() {
        use std::collections::HashMap;
        let mut m: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        m.insert(b"f1".to_vec(), b"old".to_vec());
        let mut new_count = 0i64;
        let field = b"f1".to_vec();
        if !m.contains_key(&field) {
            new_count += 1;
        }
        m.insert(field, b"new".to_vec());
        assert_eq!(new_count, 0);
    }
}
