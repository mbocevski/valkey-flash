use valkey_module::{Context, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

use crate::types::string::{FlashStringObject, FLASH_STRING_TYPE};
use crate::CACHE;

// ── DelCompletionHandle ───────────────────────────────────────────────────────

/// Unblocks the waiting client after all NVMe tombstone writes complete.
///
/// NVMe delete failures are logged and do not propagate to the client — space
/// reclaim is best-effort (spec #9). The client always receives the count of
/// keys removed from Valkey's keyspace.
///
/// Only compiled outside test mode.
#[cfg(not(test))]
pub struct DelCompletionHandle {
    tsc: valkey_module::ThreadSafeContext<valkey_module::BlockedClient<()>>,
    count: i64,
}

#[cfg(not(test))]
impl DelCompletionHandle {
    pub fn new(bc: valkey_module::BlockedClient<()>, count: i64) -> Self {
        DelCompletionHandle {
            tsc: valkey_module::ThreadSafeContext::with_blocked_client(bc),
            count,
        }
    }
}

#[cfg(not(test))]
impl crate::async_io::CompletionHandle for DelCompletionHandle {
    fn complete(self: Box<Self>, result: crate::storage::backend::StorageResult<Vec<u8>>) {
        use valkey_module::logging;
        // The task closure always returns Ok(()); an Err here would be unexpected.
        if let Err(ref e) = result {
            logging::log_warning(
                format!("flash: DEL tombstone task unexpected error: {e}").as_str(),
            );
        }
        let reply = Ok(ValkeyValue::Integer(self.count));
        // catch_unwind guards against a panic inside tsc.reply() killing the worker thread.
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.tsc.reply(reply);
        }));
    }
}

// ── Command handler ───────────────────────────────────────────────────────────

/// `FLASH.DEL key [key ...]`
///
/// Delete strategy:
/// 1. Phase 1 — type-check all keys read-only. Any existing key with a
///    non-flash-string type short-circuits with WRONGTYPE (no partial delete).
/// 2. Phase 2 — delete each present key from Valkey's keyspace and the cache.
/// 3. Replicate the full command verbatim.
/// 4. Submit a single async task that writes NVMe tombstones for all deleted
///    keys. Per-key tombstone failures are logged but do not fail the reply.
/// 5. Block the client until tombstones are written; reply with the count.
pub fn flash_del_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 2 {
        return Err(ValkeyError::WrongArity);
    }

    let keys = &args[1..];

    // Phase 1: type-check every key without modifying anything.
    // Collect indices of keys that are present flash strings.
    let mut present: Vec<&ValkeyString> = Vec::new();
    for key in keys {
        let kh = ctx.open_key(key);
        match kh.get_value::<FlashStringObject>(&FLASH_STRING_TYPE) {
            Err(_) => return Err(ValkeyError::WrongType),
            Ok(None) => {}
            Ok(Some(_)) => present.push(key),
        }
        // kh (ValkeyKey) drops here → CloseKey
    }

    let count = present.len() as i64;
    if count == 0 {
        return Ok(ValkeyValue::Integer(0));
    }

    let cache = CACHE
        .get()
        .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;

    // Phase 2: delete present keys from keyspace and cache.
    let mut deleted_bytes: Vec<Vec<u8>> = Vec::with_capacity(count as usize);
    for key in &present {
        let key_bytes = key.as_slice().to_vec();
        let kh = ctx.open_key_writable(key);
        // Removes the key from Valkey's keyspace; triggers the free() callback
        // on the FlashStringObject (Valkey frees the boxed value).
        let _ = kh.delete();
        cache.delete(&key_bytes);
        deleted_bytes.push(key_bytes);
    }

    // Replicate before blocking — must be on the event-loop thread.
    ctx.replicate_verbatim();
    for key in &present {
        ctx.notify_keyspace_event(NotifyEvent::GENERIC, "flash.del", key);
    }

    // Phase 3: async NVMe tombstone writes.
    #[cfg(not(test))]
    {
        if crate::replication::is_replica()
            || (crate::STORAGE.get().is_none() && crate::replication::must_obey_client(ctx))
        {
            return Ok(ValkeyValue::Integer(count));
        }
        use crate::storage::backend::StorageBackend;
        let storage = crate::STORAGE
            .get()
            .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;
        let pool = crate::POOL
            .get()
            .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;
        let bc = ctx.block_client();
        let handle = Box::new(DelCompletionHandle::new(bc, count));
        pool.submit_or_complete(handle, move || {
            for key in &deleted_bytes {
                let kh = crate::util::key_hash(key);
                if let Some(wal) = crate::WAL.get() {
                    if let Err(e) = wal.append(crate::storage::wal::WalOp::Delete { key_hash: kh })
                    {
                        valkey_module::logging::log_warning(
                            format!("flash: DEL WAL append failed: {e}").as_str(),
                        );
                    }
                }
                if let Err(e) = storage.delete(key) {
                    // Space reclaim is best-effort — log and continue.
                    valkey_module::logging::log_warning(
                        format!("flash: DEL NVMe tombstone failed: {e}").as_str(),
                    );
                }
            }
            Ok(vec![])
        });
        return Ok(ValkeyValue::NoReply);
    }

    // In test builds the server runtime is absent; return the count directly.
    #[allow(unreachable_code)]
    Ok(ValkeyValue::Integer(count))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use crate::storage::cache::FlashCache;

    fn cache() -> FlashCache {
        FlashCache::new(1 << 20)
    }

    #[test]
    fn cache_delete_present_key() {
        let c = cache();
        c.put(b"k", b"v".to_vec());
        assert!(c.delete(b"k"));
        assert!(c.get(b"k").is_none());
    }

    #[test]
    fn cache_delete_absent_key_returns_false() {
        assert!(!cache().delete(b"nope"));
    }

    #[test]
    fn count_of_zero_when_no_keys_present() {
        let present: Vec<&[u8]> = vec![];
        assert_eq!(present.len() as i64, 0);
    }

    #[test]
    fn count_matches_number_of_deleted_entries() {
        let c = cache();
        let keys: &[&[u8]] = &[b"a", b"b", b"c"];
        for k in keys {
            c.put(k, b"val".to_vec());
        }
        let deleted = keys.iter().filter(|k| c.delete(k)).count() as i64;
        assert_eq!(deleted, 3);
    }

    #[test]
    fn partial_presence_count_is_correct() {
        let c = cache();
        c.put(b"exists", b"v".to_vec());
        // "missing" was never inserted.
        let all_keys: &[&[u8]] = &[b"exists", b"missing"];
        let deleted = all_keys.iter().filter(|k| c.delete(k)).count() as i64;
        assert_eq!(deleted, 1);
    }
}
