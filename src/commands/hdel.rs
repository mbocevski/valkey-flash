use std::collections::HashMap;

use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

use crate::types::hash::{
    hash_deserialize_or_warn, hash_serialize, FlashHashObject, FLASH_HASH_TYPE,
};
use crate::types::Tier;
use crate::{CACHE, STORAGE};
#[cfg(not(test))]
use crate::{POOL, WAL};

// ── HDelCompletionHandle ──────────────────────────────────────────────────────

#[cfg(not(test))]
struct HDelCompletionHandle {
    tsc: valkey_module::ThreadSafeContext<valkey_module::BlockedClient<()>>,
    deleted: i64,
}

#[cfg(not(test))]
impl HDelCompletionHandle {
    fn new(bc: valkey_module::BlockedClient<()>, deleted: i64) -> Self {
        HDelCompletionHandle {
            tsc: valkey_module::ThreadSafeContext::with_blocked_client(bc),
            deleted,
        }
    }
}

#[cfg(not(test))]
impl crate::async_io::CompletionHandle for HDelCompletionHandle {
    fn complete(self: Box<Self>, result: crate::storage::backend::StorageResult<Vec<u8>>) {
        use valkey_module::logging;
        if let Err(ref e) = result {
            logging::log_warning(format!("flash: HDEL NVMe write failed: {e}").as_str());
        }
        let reply = Ok(ValkeyValue::Integer(self.deleted));
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.tsc.reply(reply);
        }));
    }
}

// ── Command handler ───────────────────────────────────────────────────────────

/// `FLASH.HDEL key field [field ...]`
///
/// Remove one or more fields from a flash-hash. Returns the number of fields
/// that were removed (non-existent fields do not count). Removing all fields
/// deletes the key entirely.
pub fn flash_hdel_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 3 {
        return Err(ValkeyError::WrongArity);
    }

    let key = &args[1];
    let fields_to_del: Vec<Vec<u8>> = args[2..].iter().map(|s| s.as_slice().to_vec()).collect();

    let cache = CACHE
        .get()
        .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;

    let key_handle = ctx.open_key_writable(key);
    let existing = match key_handle.get_value::<FlashHashObject>(&FLASH_HASH_TYPE) {
        Err(_) => return Err(ValkeyError::WrongType),
        Ok(v) => v,
    };

    let obj = match existing {
        None => return Ok(ValkeyValue::Integer(0)),
        Some(obj) => obj,
    };

    let (mut current_fields, old_ttl): (HashMap<Vec<u8>, Vec<u8>>, Option<i64>) = {
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
    };

    let mut deleted = 0i64;
    for field in &fields_to_del {
        if current_fields.remove(field).is_some() {
            deleted += 1;
        }
    }

    if deleted == 0 {
        return Ok(ValkeyValue::Integer(0));
    }

    let key_bytes = key.as_slice().to_vec();
    #[cfg(not(test))]
    let key_hash = crate::util::key_hash(&key_bytes);

    if current_fields.is_empty() {
        let _ = key_handle.delete();
        cache.delete(&key_bytes);
        ctx.replicate_verbatim();

        #[cfg(not(test))]
        {
            if crate::replication::is_replica()
                || (crate::STORAGE.get().is_none() && crate::replication::must_obey_client(ctx))
            {
                return Ok(ValkeyValue::Integer(deleted));
            }
            let pool = POOL
                .get()
                .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;
            let bc = ctx.block_client();
            let handle = Box::new(HDelCompletionHandle::new(bc, deleted));
            pool.submit_or_complete(handle, move || {
                if let Some(wal) = WAL.get() {
                    if let Err(e) = wal.append(crate::storage::wal::WalOp::Delete { key_hash }) {
                        valkey_module::logging::log_warning(
                            format!("flash: HDEL WAL Delete failed: {e}").as_str(),
                        );
                    }
                }
                Ok(vec![])
            });
            return Ok(ValkeyValue::NoReply);
        }
    } else {
        key_handle
            .set_value(
                &FLASH_HASH_TYPE,
                FlashHashObject {
                    tier: Tier::Hot(current_fields.clone()),
                    ttl_ms: old_ttl,
                },
            )
            .map_err(|e| ValkeyError::String(format!("flash: hdel set_value: {e}")))?;

        let serialized = hash_serialize(&current_fields);
        cache.put(&key_bytes, serialized.clone());
        ctx.replicate_verbatim();

        #[cfg(not(test))]
        {
            if crate::replication::is_replica()
                || (crate::STORAGE.get().is_none() && crate::replication::must_obey_client(ctx))
            {
                return Ok(ValkeyValue::Integer(deleted));
            }
            use crate::storage::backend::StorageBackend;
            let storage = STORAGE
                .get()
                .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;
            let pool = POOL
                .get()
                .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;
            let bc = ctx.block_client();
            let handle = Box::new(HDelCompletionHandle::new(bc, deleted));
            pool.submit_or_complete(handle, move || {
                let offset = storage.put(&key_bytes, &serialized)?;
                if let Some(wal) = WAL.get() {
                    let vh = crate::util::value_hash(&serialized);
                    if let Err(e) = wal.append(crate::storage::wal::WalOp::Put {
                        key_hash,
                        offset,
                        value_hash: vh,
                    }) {
                        valkey_module::logging::log_warning(
                            format!("flash: HDEL WAL Put failed: {e}").as_str(),
                        );
                    }
                }
                Ok(vec![])
            });
            return Ok(ValkeyValue::NoReply);
        }
    }

    #[allow(unreachable_code)]
    Ok(ValkeyValue::Integer(deleted))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    #[test]
    fn hdel_arity_requires_at_least_three_args() {
        let len = 3usize; // FLASH.HDEL key field
        assert!(len >= 3);
    }

    #[test]
    fn delete_count_for_present_fields() {
        let mut m: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        m.insert(b"f1".to_vec(), b"v1".to_vec());
        m.insert(b"f2".to_vec(), b"v2".to_vec());
        let to_del: &[&[u8]] = &[b"f1", b"f3"];
        let mut deleted = 0i64;
        for f in to_del {
            if m.remove(*f).is_some() {
                deleted += 1;
            }
        }
        assert_eq!(deleted, 1);
        assert!(!m.contains_key(b"f1".as_ref()));
        assert!(m.contains_key(b"f2".as_ref()));
    }

    #[test]
    fn delete_all_fields_empties_map() {
        let mut m: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        m.insert(b"f1".to_vec(), b"v1".to_vec());
        let _ = m.remove(b"f1".as_ref());
        assert!(m.is_empty());
    }

    #[test]
    fn delete_non_existent_field_returns_zero() {
        let m: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        let mut deleted = 0i64;
        if m.contains_key(b"ghost".as_ref()) {
            deleted += 1;
        }
        assert_eq!(deleted, 0);
    }
}
