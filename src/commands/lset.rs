use valkey_module::{Context, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

use crate::CACHE;
use crate::commands::list_common::{current_time_ms, promote_cold_list, resolve_index};
use crate::types::Tier;
use crate::types::list::{FLASH_LIST_TYPE, FlashListObject, list_serialize};
#[cfg(not(test))]
use crate::{POOL, STORAGE, WAL};

// ── LSetCompletionHandle ──────────────────────────────────────────────────────

#[cfg(not(test))]
struct LSetCompletionHandle {
    tsc: valkey_module::ThreadSafeContext<valkey_module::BlockedClient<()>>,
}

#[cfg(not(test))]
impl crate::async_io::CompletionHandle for LSetCompletionHandle {
    fn complete(self: Box<Self>, result: crate::storage::backend::StorageResult<Vec<u8>>) {
        use valkey_module::logging;
        if let Err(ref e) = result {
            logging::log_warning(format!("flash: LSET NVMe write failed: {e}").as_str());
        }
        let reply = Ok(ValkeyValue::SimpleStringStatic("OK"));
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.tsc.reply(reply);
        }));
    }
}

// ── Command handler ───────────────────────────────────────────────────────────

/// `FLASH.LSET key index value`
///
/// Set the element at index to value. Negative indices count from the tail.
/// Returns an error if the key does not exist or the index is out of range.
pub fn flash_lset_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() != 4 {
        return Err(ValkeyError::WrongArity);
    }

    let key = &args[1];
    let index: i64 = std::str::from_utf8(args[2].as_slice())
        .map_err(|_| ValkeyError::Str("ERR value is not an integer or out of range"))?
        .parse()
        .map_err(|_| ValkeyError::Str("ERR value is not an integer or out of range"))?;
    let new_val = args[3].as_slice().to_vec();

    let cache = CACHE
        .get()
        .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;

    let key_handle = ctx.open_key_writable(key);
    let existing = match key_handle.get_value::<FlashListObject>(&FLASH_LIST_TYPE) {
        Err(_) => return Err(ValkeyError::WrongType),
        Ok(None) => return Err(ValkeyError::Str("ERR no such key")),
        Ok(Some(obj)) => obj,
    };

    let (mut items, old_ttl) = {
        let ttl = crate::util_expire::preserve_ttl(ctx, key, existing.ttl_ms);
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

    let idx =
        resolve_index(index, items.len()).ok_or(ValkeyError::Str("ERR index out of range"))?;
    items[idx] = new_val;

    key_handle
        .set_value(
            &FLASH_LIST_TYPE,
            FlashListObject {
                tier: Tier::Hot(items.clone()),
                ttl_ms: old_ttl,
            },
        )
        .map_err(|e| ValkeyError::String(format!("flash: lset set_value: {e}")))?;

    if let Some(abs_ms) = old_ttl {
        use std::time::Duration;
        let remaining_ms = (abs_ms - current_time_ms()).max(1) as u64;
        key_handle
            .set_expire(Duration::from_millis(remaining_ms))
            .map_err(|e| ValkeyError::String(format!("flash: lset set_expire: {e}")))?;
    }

    let serialized = list_serialize(&items);
    cache.put(key.as_slice(), serialized.clone());

    ctx.replicate_verbatim();
    ctx.notify_keyspace_event(NotifyEvent::LIST, "flash.lset", key);

    #[cfg(not(test))]
    {
        if crate::replication::must_run_sync(ctx)
            || (crate::STORAGE.get().is_none() && crate::replication::must_obey_client(ctx))
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
        let handle = Box::new(LSetCompletionHandle {
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
                        format!("flash: LSET WAL append failed: {e}").as_str(),
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
    fn lset_valid_index() {
        let mut items: VecDeque<Vec<u8>> = VecDeque::new();
        items.push_back(b"a".to_vec());
        items.push_back(b"b".to_vec());
        let idx = resolve_index(1, items.len()).unwrap();
        items[idx] = b"z".to_vec();
        assert_eq!(items[1], b"z");
    }

    #[test]
    fn lset_negative_index() {
        let mut items: VecDeque<Vec<u8>> = VecDeque::new();
        items.push_back(b"a".to_vec());
        items.push_back(b"b".to_vec());
        let idx = resolve_index(-1, items.len()).unwrap();
        items[idx] = b"z".to_vec();
        assert_eq!(items[1], b"z");
    }

    #[test]
    fn lset_out_of_range_returns_none() {
        let items: VecDeque<Vec<u8>> = VecDeque::new();
        assert_eq!(resolve_index(0, items.len()), None);
    }
}
