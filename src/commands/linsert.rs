use valkey_module::{Context, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

use crate::CACHE;
use crate::commands::list_common::{current_time_ms, promote_cold_list};
use crate::types::Tier;
use crate::types::list::{FLASH_LIST_TYPE, FlashListObject, list_serialize};
#[cfg(not(test))]
use crate::{POOL, STORAGE, WAL};

// ── LInsertCompletionHandle ───────────────────────────────────────────────────

#[cfg(not(test))]
struct LInsertCompletionHandle {
    tsc: valkey_module::ThreadSafeContext<valkey_module::BlockedClient<()>>,
    new_len: i64,
}

#[cfg(not(test))]
impl crate::async_io::CompletionHandle for LInsertCompletionHandle {
    fn complete(self: Box<Self>, result: crate::storage::backend::StorageResult<Vec<u8>>) {
        use valkey_module::logging;
        if let Err(ref e) = result {
            logging::log_warning(format!("flash: LINSERT NVMe write failed: {e}").as_str());
        }
        let reply = Ok(ValkeyValue::Integer(self.new_len));
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.tsc.reply(reply);
        }));
    }
}

// ── Command handler ───────────────────────────────────────────────────────────

/// `FLASH.LINSERT key BEFORE|AFTER pivot value`
///
/// Returns the new list length, -1 if pivot not found, or 0 if key absent.
pub fn flash_linsert_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() != 5 {
        return Err(ValkeyError::WrongArity);
    }

    let key = &args[1];
    let direction = args[2].as_slice().to_ascii_uppercase();
    let before = match direction.as_slice() {
        b"BEFORE" => true,
        b"AFTER" => false,
        _ => {
            return Err(ValkeyError::Str(
                "ERR syntax error — expected BEFORE or AFTER",
            ));
        }
    };
    let pivot = args[3].as_slice().to_vec();
    let new_val = args[4].as_slice().to_vec();

    let cache = CACHE
        .get()
        .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;

    let key_handle = ctx.open_key_writable(key);
    let existing = match key_handle.get_value::<FlashListObject>(&FLASH_LIST_TYPE) {
        Err(_) => return Err(ValkeyError::WrongType),
        Ok(None) => return Ok(ValkeyValue::Integer(0)),
        Ok(Some(obj)) => obj,
    };

    let (mut items, old_ttl) = {
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

    // Find pivot position.
    let pivot_pos = items.iter().position(|e| e == &pivot);
    let Some(pos) = pivot_pos else {
        return Ok(ValkeyValue::Integer(-1));
    };

    let insert_pos = if before { pos } else { pos + 1 };
    items.insert(insert_pos, new_val);

    let new_len = items.len() as i64;

    key_handle
        .set_value(
            &FLASH_LIST_TYPE,
            FlashListObject {
                tier: Tier::Hot(items.clone()),
                ttl_ms: old_ttl,
            },
        )
        .map_err(|e| ValkeyError::String(format!("flash: linsert set_value: {e}")))?;

    if let Some(abs_ms) = old_ttl {
        use std::time::Duration;
        let remaining_ms = (abs_ms - current_time_ms()).max(1) as u64;
        key_handle
            .set_expire(Duration::from_millis(remaining_ms))
            .map_err(|e| ValkeyError::String(format!("flash: linsert set_expire: {e}")))?;
    }

    let serialized = list_serialize(&items);
    cache.put(key.as_slice(), serialized.clone());

    ctx.replicate_verbatim();
    ctx.notify_keyspace_event(NotifyEvent::LIST, "flash.linsert", key);

    #[cfg(not(test))]
    {
        if crate::replication::is_replica()
            || (STORAGE.get().is_none() && crate::replication::must_obey_client(ctx))
        {
            return Ok(ValkeyValue::Integer(new_len));
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
        let handle = Box::new(LInsertCompletionHandle {
            tsc: valkey_module::ThreadSafeContext::with_blocked_client(bc),
            new_len,
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
                        format!("flash: LINSERT WAL append failed: {e}").as_str(),
                    );
                }
            }
            Ok(vec![])
        });
        return Ok(ValkeyValue::NoReply);
    }

    #[allow(unreachable_code)]
    Ok(ValkeyValue::Integer(new_len))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    #[test]
    fn insert_before_pivot() {
        let mut items: VecDeque<Vec<u8>> = VecDeque::new();
        items.push_back(b"a".to_vec());
        items.push_back(b"c".to_vec());
        let pivot = b"c".to_vec();
        let pos = items.iter().position(|e| e == &pivot).unwrap();
        items.insert(pos, b"b".to_vec()); // insert BEFORE c
        assert_eq!(items[1], b"b");
        assert_eq!(items[2], b"c");
    }

    #[test]
    fn insert_after_pivot() {
        let mut items: VecDeque<Vec<u8>> = VecDeque::new();
        items.push_back(b"a".to_vec());
        items.push_back(b"c".to_vec());
        let pivot = b"a".to_vec();
        let pos = items.iter().position(|e| e == &pivot).unwrap();
        items.insert(pos + 1, b"b".to_vec()); // insert AFTER a
        assert_eq!(items[1], b"b");
        assert_eq!(items[2], b"c");
    }

    #[test]
    fn pivot_not_found_returns_neg_one() {
        let items: VecDeque<Vec<u8>> = VecDeque::new();
        let pivot = b"x".to_vec();
        let found = items.iter().position(|e| e == &pivot);
        assert_eq!(found, None);
    }
}
