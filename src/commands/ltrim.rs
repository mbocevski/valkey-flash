use valkey_module::{Context, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

use crate::CACHE;
use crate::commands::list_common::{current_time_ms, promote_cold_list, resolve_range};
use crate::types::Tier;
use crate::types::list::{FLASH_LIST_TYPE, FlashListObject, list_serialize};
#[cfg(not(test))]
use crate::{POOL, STORAGE, WAL};

// ── LTrimCompletionHandle ─────────────────────────────────────────────────────

#[cfg(not(test))]
struct LTrimCompletionHandle {
    tsc: valkey_module::ThreadSafeContext<valkey_module::BlockedClient<()>>,
}

#[cfg(not(test))]
impl crate::async_io::CompletionHandle for LTrimCompletionHandle {
    fn complete(self: Box<Self>, result: crate::storage::backend::StorageResult<Vec<u8>>) {
        use valkey_module::logging;
        if let Err(ref e) = result {
            logging::log_warning(format!("flash: LTRIM NVMe write failed: {e}").as_str());
        }
        let reply = Ok(ValkeyValue::SimpleStringStatic("OK"));
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.tsc.reply(reply);
        }));
    }
}

// ── Command handler ───────────────────────────────────────────────────────────

/// `FLASH.LTRIM key start stop`
///
/// Trim the list to only keep elements in [start, stop] (inclusive).
/// Negative indices count from the tail. Returns OK.
pub fn flash_ltrim_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() != 4 {
        return Err(ValkeyError::WrongArity);
    }

    let key = &args[1];
    let start: i64 = std::str::from_utf8(args[2].as_slice())
        .map_err(|_| ValkeyError::Str("ERR value is not an integer or out of range"))?
        .parse()
        .map_err(|_| ValkeyError::Str("ERR value is not an integer or out of range"))?;
    let stop: i64 = std::str::from_utf8(args[3].as_slice())
        .map_err(|_| ValkeyError::Str("ERR value is not an integer or out of range"))?
        .parse()
        .map_err(|_| ValkeyError::Str("ERR value is not an integer or out of range"))?;

    let cache = CACHE
        .get()
        .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;

    let key_handle = ctx.open_key_writable(key);
    let existing = match key_handle.get_value::<FlashListObject>(&FLASH_LIST_TYPE) {
        Err(_) => return Err(ValkeyError::WrongType),
        Ok(None) => return Ok(ValkeyValue::SimpleStringStatic("OK")),
        Ok(Some(obj)) => obj,
    };

    let (items, old_ttl) = {
        let ttl = existing.ttl_ms;
        let list = match &existing.tier {
            Tier::Hot(l) => l.clone(),
            Tier::Cold {
                backend_offset,
                value_len,
                ..
            } => promote_cold_list(*backend_offset, *value_len)?,
        };
        (list, ttl)
    };

    let (s, e) = resolve_range(start, stop, items.len());
    let new_items: std::collections::VecDeque<Vec<u8>> = items.range(s..e).cloned().collect();

    if new_items.is_empty() {
        let _ = key_handle.delete();
        cache.delete(key.as_slice());
        ctx.replicate_verbatim();
        ctx.notify_keyspace_event(NotifyEvent::LIST, "flash.ltrim", key);

        #[cfg(not(test))]
        {
            if crate::replication::is_replica()
                || (STORAGE.get().is_none() && crate::replication::must_obey_client(ctx))
            {
                return Ok(ValkeyValue::SimpleStringStatic("OK"));
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
            let handle = Box::new(LTrimCompletionHandle {
                tsc: valkey_module::ThreadSafeContext::with_blocked_client(bc),
            });
            pool.submit_or_complete(handle, move || {
                storage.delete(&key_bytes).ok();
                Ok(vec![])
            });
            return Ok(ValkeyValue::NoReply);
        }

        #[allow(unreachable_code)]
        return Ok(ValkeyValue::SimpleStringStatic("OK"));
    }

    key_handle
        .set_value(
            &FLASH_LIST_TYPE,
            FlashListObject {
                tier: Tier::Hot(new_items.clone()),
                ttl_ms: old_ttl,
            },
        )
        .map_err(|e| ValkeyError::String(format!("flash: ltrim set_value: {e}")))?;

    if let Some(abs_ms) = old_ttl {
        use std::time::Duration;
        let remaining_ms = (abs_ms - current_time_ms()).max(1) as u64;
        key_handle
            .set_expire(Duration::from_millis(remaining_ms))
            .map_err(|e| ValkeyError::String(format!("flash: ltrim set_expire: {e}")))?;
    }

    let serialized = list_serialize(&new_items);
    cache.put(key.as_slice(), serialized.clone());

    ctx.replicate_verbatim();
    ctx.notify_keyspace_event(NotifyEvent::LIST, "flash.ltrim", key);

    #[cfg(not(test))]
    {
        if crate::replication::is_replica()
            || (STORAGE.get().is_none() && crate::replication::must_obey_client(ctx))
        {
            return Ok(ValkeyValue::SimpleStringStatic("OK"));
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
        let handle = Box::new(LTrimCompletionHandle {
            tsc: valkey_module::ThreadSafeContext::with_blocked_client(bc),
        });
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
                        format!("flash: LTRIM WAL append failed: {e}").as_str(),
                    );
                }
            }
            Ok(vec![])
        });
        return Ok(ValkeyValue::NoReply);
    }

    #[allow(unreachable_code)]
    Ok(ValkeyValue::SimpleStringStatic("OK"))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    #[test]
    fn ltrim_keeps_middle() {
        let mut items = VecDeque::new();
        for c in b"abcde" {
            items.push_back(vec![*c]);
        }
        let (s, e) = resolve_range(1, 3, items.len());
        let trimmed: VecDeque<Vec<u8>> = items.range(s..e).cloned().collect();
        assert_eq!(trimmed.len(), 3);
        assert_eq!(trimmed[0], b"b");
        assert_eq!(trimmed[2], b"d");
    }

    #[test]
    fn ltrim_out_of_range_produces_empty() {
        let mut items = VecDeque::new();
        items.push_back(b"a".to_vec());
        let (s, e) = resolve_range(5, 10, items.len());
        let trimmed: VecDeque<Vec<u8>> = items.range(s..e).cloned().collect();
        assert!(trimmed.is_empty());
    }

    #[test]
    fn ltrim_full_range() {
        let mut items = VecDeque::new();
        items.push_back(b"x".to_vec());
        let (s, e) = resolve_range(0, -1, items.len());
        let trimmed: VecDeque<Vec<u8>> = items.range(s..e).cloned().collect();
        assert_eq!(trimmed.len(), 1);
    }
}
