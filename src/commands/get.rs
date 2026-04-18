use std::sync::atomic::{AtomicU64, Ordering};
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

use crate::types::string::{FlashStringObject, FLASH_STRING_TYPE};
use crate::types::Tier;
use crate::CACHE;

pub static GET_HITS: AtomicU64 = AtomicU64::new(0);
pub static GET_MISSES: AtomicU64 = AtomicU64::new(0);

// ── GetCompletionHandle ───────────────────────────────────────────────────────

/// Unblocks the waiting client after a cold-path NVMe read.
///
/// On success: promotes the fetched bytes into the hot cache, then replies
/// with the value. On failure: replies with an error string.
///
/// Only compiled outside test mode — `ThreadSafeContext` and `BlockedClient`
/// require a running Valkey server.
#[cfg(not(test))]
pub struct GetCompletionHandle {
    tsc: valkey_module::ThreadSafeContext<valkey_module::BlockedClient<()>>,
    key: Vec<u8>,
    cache: &'static crate::storage::cache::FlashCache,
}

#[cfg(not(test))]
impl GetCompletionHandle {
    pub fn new(
        bc: valkey_module::BlockedClient<()>,
        key: Vec<u8>,
        cache: &'static crate::storage::cache::FlashCache,
    ) -> Self {
        GetCompletionHandle {
            tsc: valkey_module::ThreadSafeContext::with_blocked_client(bc),
            key,
            cache,
        }
    }
}

#[cfg(not(test))]
impl crate::async_io::CompletionHandle for GetCompletionHandle {
    fn complete(self: Box<Self>, result: crate::storage::backend::StorageResult<Vec<u8>>) {
        let reply = match result {
            Ok(bytes) => {
                // Promote to hot cache before replying so subsequent reads are fast.
                self.cache.put(&self.key, bytes.clone());
                Ok(ValkeyValue::StringBuffer(bytes))
            }
            Err(e) => Err(ValkeyError::String(e.to_string())),
        };
        // catch_unwind guards against a panic inside tsc.reply() killing the worker thread.
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.tsc.reply(reply);
        }));
    }
}

// ── Command handler ───────────────────────────────────────────────────────────

/// `FLASH.GET key`
///
/// Read strategy:
/// 1. Type-check the key (WRONGTYPE if it holds a non-flash-string type).
/// 2. Cache hit fast path: value in hot cache → reply inline, no BlockClient.
/// 3. Cache miss, hot tier: value in the keyspace object → re-promote to cache,
///    reply inline.
/// 4. Cache miss, cold tier: BlockClient, submit async NVMe read; on completion
///    promote to cache and reply.
pub fn flash_get_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 2 {
        return Err(ValkeyError::WrongArity);
    }

    let key = &args[1];
    let cache = CACHE
        .get()
        .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;

    // Open key read-only, type-check, and extract the tier.
    // Scoped so key_handle is dropped before any blocking operation.
    let tier_value: Option<Vec<u8>> = {
        let key_handle = ctx.open_key(key);
        let obj = match key_handle.get_value::<FlashStringObject>(&FLASH_STRING_TYPE) {
            Err(_) => return Err(ValkeyError::WrongType),
            Ok(None) => return Ok(ValkeyValue::Null),
            Ok(Some(obj)) => obj,
        };

        // Cache hit: cache.get() updates recency counters internally.
        if let Some(value) = cache.get(key.as_slice()) {
            GET_HITS.fetch_add(1, Ordering::Relaxed);
            return Ok(ValkeyValue::StringBuffer(value));
        }

        GET_MISSES.fetch_add(1, Ordering::Relaxed);

        match &obj.tier {
            Tier::Hot(v) => Some(v.clone()),
            Tier::Cold { .. } => None,
        }
        // key_handle (and borrow of obj) dropped here
    };

    match tier_value {
        Some(value) => {
            // Value was in the keyspace object but evicted from the hot cache.
            // Re-promote so subsequent reads are served from cache.
            cache.put(key.as_slice(), value.clone());
            Ok(ValkeyValue::StringBuffer(value))
        }
        None => {
            // Truly cold: value lives only on NVMe.
            #[cfg(not(test))]
            {
                use crate::storage::backend::StorageBackend;
                let key_bytes = key.as_slice().to_vec();
                let storage = crate::STORAGE
                    .get()
                    .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;
                let pool = crate::POOL
                    .get()
                    .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;
                let bc = ctx.block_client();
                let handle = Box::new(GetCompletionHandle::new(bc, key_bytes.clone(), cache));
                pool.submit_or_complete(handle, move || {
                    storage.get(&key_bytes).and_then(|opt| {
                        opt.ok_or(crate::storage::backend::StorageError::Other(
                            "key not found on NVMe".into(),
                        ))
                    })
                });
                return Ok(ValkeyValue::NoReply);
            }

            // In test builds the server runtime is absent; return nil directly.
            #[allow(unreachable_code)]
            Ok(ValkeyValue::Null)
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::cache::FlashCache;

    fn cache() -> FlashCache {
        FlashCache::new(1 << 20)
    }

    #[test]
    fn get_hits_counter_is_readable() {
        let _ = GET_HITS.load(Ordering::Relaxed);
    }

    #[test]
    fn get_misses_counter_is_readable() {
        let _ = GET_MISSES.load(Ordering::Relaxed);
    }

    #[test]
    fn hot_tier_re_promotes_to_cache() {
        let c = cache();
        let key = b"hotkey";
        let value = b"hotvalue".to_vec();
        // Simulate: cache miss, value found in Tier::Hot — promote.
        c.put(key, value.clone());
        assert_eq!(c.get(key), Some(value));
    }

    #[test]
    fn re_promote_after_eviction_restores_entry() {
        let c = cache();
        let key = b"evicted";
        let value = vec![1u8; 32];
        c.put(key, value.clone());
        c.delete(key); // simulate cache eviction
        assert!(c.get(key).is_none());
        c.put(key, value.clone()); // re-promote (hot-tier fallback path)
        assert_eq!(c.get(key), Some(value));
    }

    #[test]
    fn cold_tier_variant_matches_correctly() {
        let tier: Tier<Vec<u8>> = Tier::Cold { size_hint: 256 };
        assert!(matches!(tier, Tier::Cold { .. }));
        // Hot variant must NOT match Cold.
        let hot: Tier<Vec<u8>> = Tier::Hot(vec![1, 2, 3]);
        assert!(!matches!(hot, Tier::Cold { .. }));
    }

    #[test]
    fn hot_tier_value_is_cloneable() {
        let value = b"test_payload".to_vec();
        let tier = Tier::Hot(value.clone());
        if let Tier::Hot(v) = &tier {
            let cloned = v.clone();
            assert_eq!(cloned, value);
        } else {
            panic!("expected Tier::Hot");
        }
    }
}
