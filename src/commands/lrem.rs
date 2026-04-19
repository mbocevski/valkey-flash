use valkey_module::{Context, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

use crate::CACHE;
use crate::commands::list_common::{current_time_ms, promote_cold_list};
use crate::types::Tier;
use crate::types::list::{FLASH_LIST_TYPE, FlashListObject, list_serialize};
#[cfg(not(test))]
use crate::{POOL, STORAGE, WAL};

// ── LRemCompletionHandle ──────────────────────────────────────────────────────

#[cfg(not(test))]
struct LRemCompletionHandle {
    tsc: valkey_module::ThreadSafeContext<valkey_module::BlockedClient<()>>,
    removed: i64,
}

#[cfg(not(test))]
impl crate::async_io::CompletionHandle for LRemCompletionHandle {
    fn complete(self: Box<Self>, result: crate::storage::backend::StorageResult<Vec<u8>>) {
        use valkey_module::logging;
        if let Err(ref e) = result {
            logging::log_warning(format!("flash: LREM NVMe write failed: {e}").as_str());
        }
        let reply = Ok(ValkeyValue::Integer(self.removed));
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.tsc.reply(reply);
        }));
    }
}

// ── Command handler ───────────────────────────────────────────────────────────

/// `FLASH.LREM key count value`
///
/// Remove occurrences of value:
/// - count > 0: from head to tail, remove up to count
/// - count < 0: from tail to head, remove up to |count|
/// - count = 0: remove all
///
/// Returns the number of elements removed.
pub fn flash_lrem_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() != 4 {
        return Err(ValkeyError::WrongArity);
    }

    let key = &args[1];
    let count: i64 = std::str::from_utf8(args[2].as_slice())
        .map_err(|_| ValkeyError::Str("ERR value is not an integer or out of range"))?
        .parse()
        .map_err(|_| ValkeyError::Str("ERR value is not an integer or out of range"))?;
    let target = args[3].as_slice().to_vec();

    let cache = CACHE
        .get()
        .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;

    let key_handle = ctx.open_key_writable(key);
    let existing = match key_handle.get_value::<FlashListObject>(&FLASH_LIST_TYPE) {
        Err(_) => return Err(ValkeyError::WrongType),
        Ok(None) => return Ok(ValkeyValue::Integer(0)),
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

    // Build new list by removing matching elements.
    let mut new_items = std::collections::VecDeque::with_capacity(items.len());
    let max = if count == 0 {
        usize::MAX
    } else {
        count.unsigned_abs() as usize
    };
    let mut removed = 0usize;

    if count >= 0 {
        // Head-to-tail.
        for elem in &items {
            if elem == &target && removed < max {
                removed += 1;
            } else {
                new_items.push_back(elem.clone());
            }
        }
    } else {
        // Tail-to-head: reverse, filter, reverse back.
        let reversed: Vec<_> = items.iter().rev().collect();
        let mut kept: Vec<Vec<u8>> = Vec::with_capacity(items.len());
        for elem in reversed {
            if elem == &target && removed < max {
                removed += 1;
            } else {
                kept.push(elem.clone());
            }
        }
        new_items.extend(kept.into_iter().rev());
    }

    let removed_count = removed as i64;

    if new_items.is_empty() {
        let _ = key_handle.delete();
        cache.delete(key.as_slice());
        ctx.replicate_verbatim();
        ctx.notify_keyspace_event(NotifyEvent::LIST, "flash.lrem", key);

        #[cfg(not(test))]
        {
            if crate::replication::is_replica()
                || (STORAGE.get().is_none() && crate::replication::must_obey_client(ctx))
            {
                return Ok(ValkeyValue::Integer(removed_count));
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
            let handle = Box::new(LRemCompletionHandle {
                tsc: valkey_module::ThreadSafeContext::with_blocked_client(bc),
                removed: removed_count,
            });
            pool.submit_or_complete(handle, move || {
                storage.delete(&key_bytes).ok();
                Ok(vec![])
            });
            return Ok(ValkeyValue::NoReply);
        }

        #[allow(unreachable_code)]
        return Ok(ValkeyValue::Integer(removed_count));
    }

    key_handle
        .set_value(
            &FLASH_LIST_TYPE,
            FlashListObject {
                tier: Tier::Hot(new_items.clone()),
                ttl_ms: old_ttl,
            },
        )
        .map_err(|e| ValkeyError::String(format!("flash: lrem set_value: {e}")))?;

    if let Some(abs_ms) = old_ttl {
        use std::time::Duration;
        let remaining_ms = (abs_ms - current_time_ms()).max(1) as u64;
        key_handle
            .set_expire(Duration::from_millis(remaining_ms))
            .map_err(|e| ValkeyError::String(format!("flash: lrem set_expire: {e}")))?;
    }

    let serialized = list_serialize(&new_items);
    cache.put(key.as_slice(), serialized.clone());

    ctx.replicate_verbatim();
    ctx.notify_keyspace_event(NotifyEvent::LIST, "flash.lrem", key);

    #[cfg(not(test))]
    {
        if crate::replication::is_replica()
            || (STORAGE.get().is_none() && crate::replication::must_obey_client(ctx))
        {
            return Ok(ValkeyValue::Integer(removed_count));
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
        let handle = Box::new(LRemCompletionHandle {
            tsc: valkey_module::ThreadSafeContext::with_blocked_client(bc),
            removed: removed_count,
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
                        format!("flash: LREM WAL append failed: {e}").as_str(),
                    );
                }
            }
            Ok(vec![])
        });
        return Ok(ValkeyValue::NoReply);
    }

    #[allow(unreachable_code)]
    Ok(ValkeyValue::Integer(removed_count))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    fn rem_head(items: &[&[u8]], target: &[u8], count: usize) -> (Vec<Vec<u8>>, usize) {
        let max = if count == 0 { usize::MAX } else { count };
        let mut out = Vec::new();
        let mut removed = 0;
        for elem in items {
            if *elem == target && removed < max {
                removed += 1;
            } else {
                out.push(elem.to_vec());
            }
        }
        (out, removed)
    }

    #[test]
    fn lrem_removes_from_head() {
        let items: &[&[u8]] = &[b"a", b"b", b"a", b"c", b"a"];
        let (out, removed) = rem_head(items, b"a", 2);
        assert_eq!(removed, 2);
        assert_eq!(out, vec![b"b".to_vec(), b"c".to_vec(), b"a".to_vec()]);
    }

    #[test]
    fn lrem_removes_all_when_count_zero() {
        let items: &[&[u8]] = &[b"x", b"x", b"y", b"x"];
        let (out, removed) = rem_head(items, b"x", 0);
        assert_eq!(removed, 3);
        assert_eq!(out, vec![b"y".to_vec()]);
    }

    #[test]
    fn lrem_removes_from_tail() {
        let items: Vec<Vec<u8>> = vec![b"a".to_vec(), b"b".to_vec(), b"a".to_vec()];
        let max = 1usize;
        let mut removed = 0;
        let mut kept = Vec::new();
        for elem in items.iter().rev() {
            if elem == b"a" && removed < max {
                removed += 1;
            } else {
                kept.push(elem.clone());
            }
        }
        let result: Vec<Vec<u8>> = kept.into_iter().rev().collect();
        assert_eq!(result, vec![b"a".to_vec(), b"b".to_vec()]);
        assert_eq!(removed, 1);
    }

    #[test]
    fn lrem_missing_target_removes_nothing() {
        let items: &[&[u8]] = &[b"a", b"b"];
        let (out, removed) = rem_head(items, b"z", 1);
        assert_eq!(removed, 0);
        assert_eq!(out.len(), 2);
    }
}
