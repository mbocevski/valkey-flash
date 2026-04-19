use std::collections::VecDeque;

use valkey_module::{Context, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

use crate::CACHE;
use crate::commands::list_common::promote_cold_list;
use crate::types::Tier;
use crate::types::list::{FLASH_LIST_TYPE, FlashListObject, list_serialize};
#[cfg(not(test))]
use crate::{POOL, STORAGE, WAL};

// ── PopCompletionHandle ───────────────────────────────────────────────────────

#[cfg(not(test))]
struct PopCompletionHandle {
    tsc: valkey_module::ThreadSafeContext<valkey_module::BlockedClient<()>>,
    popped: Option<Vec<Vec<u8>>>,
}

#[cfg(not(test))]
impl crate::async_io::CompletionHandle for PopCompletionHandle {
    fn complete(self: Box<Self>, result: crate::storage::backend::StorageResult<Vec<u8>>) {
        use valkey_module::logging;
        if let Err(ref e) = result {
            logging::log_warning(format!("flash: POP NVMe write failed: {e}").as_str());
        }
        let reply = match self.popped {
            None => Ok(ValkeyValue::Null),
            Some(ref elems) if elems.len() == 1 => Ok(ValkeyValue::StringBuffer(elems[0].clone())),
            Some(elems) => Ok(ValkeyValue::Array(
                elems.into_iter().map(ValkeyValue::StringBuffer).collect(),
            )),
        };
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.tsc.reply(reply);
        }));
    }
}

// ── Shared pop logic ──────────────────────────────────────────────────────────

fn do_pop(ctx: &Context, args: Vec<ValkeyString>, pop_left: bool, _cmd_name: &str) -> ValkeyResult {
    if args.len() < 2 || args.len() > 3 {
        return Err(ValkeyError::WrongArity);
    }

    let key = &args[1];

    // Optional count argument.
    let count: Option<usize> = if args.len() == 3 {
        let s = std::str::from_utf8(args[2].as_slice())
            .map_err(|_| ValkeyError::Str("ERR value is not an integer or out of range"))?;
        let n: i64 = s
            .parse()
            .map_err(|_| ValkeyError::Str("ERR value is not an integer or out of range"))?;
        if n < 0 {
            return Err(ValkeyError::String(
                "ERR value is not an integer or out of range".to_string(),
            ));
        }
        Some(n as usize)
    } else {
        None
    };

    let cache = CACHE
        .get()
        .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;

    let key_handle = ctx.open_key_writable(key);
    let existing = match key_handle.get_value::<FlashListObject>(&FLASH_LIST_TYPE) {
        Err(_) => return Err(ValkeyError::WrongType),
        Ok(v) => v,
    };

    let (mut items, old_ttl): (VecDeque<Vec<u8>>, Option<i64>) = match existing {
        None => return Ok(ValkeyValue::Null),
        Some(obj) => {
            let ttl = obj.ttl_ms;
            let list = match &obj.tier {
                Tier::Hot(l) => l.clone(),
                Tier::Cold {
                    backend_offset,
                    value_len,
                    ..
                } => promote_cold_list(*backend_offset, *value_len)?,
            };
            (list, ttl)
        }
    };

    if items.is_empty() {
        return Ok(ValkeyValue::Null);
    }

    // Pop elements.
    let n = count.unwrap_or(1).min(items.len());
    let mut popped: Vec<Vec<u8>> = Vec::with_capacity(n);
    for _ in 0..n {
        let elem = if pop_left {
            items.pop_front().unwrap()
        } else {
            items.pop_back().unwrap()
        };
        popped.push(elem);
    }

    // If list is now empty, delete the key.
    if items.is_empty() {
        let _ = key_handle.delete();
        cache.delete(key.as_slice());

        ctx.replicate_verbatim();
        ctx.notify_keyspace_event(
            NotifyEvent::LIST,
            if pop_left { "flash.lpop" } else { "flash.rpop" },
            key,
        );

        let reply = build_pop_reply(count, popped.clone());

        #[cfg(not(test))]
        {
            if crate::replication::is_replica()
                || (STORAGE.get().is_none() && crate::replication::must_obey_client(ctx))
            {
                return Ok(reply);
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
            let handle = Box::new(PopCompletionHandle {
                tsc: valkey_module::ThreadSafeContext::with_blocked_client(bc),
                popped: Some(popped),
            });
            pool.submit_or_complete(handle, move || {
                storage.delete(&key_bytes).ok();
                Ok(vec![])
            });
            return Ok(ValkeyValue::NoReply);
        }

        #[allow(unreachable_code)]
        return Ok(reply);
    }

    let serialized = list_serialize(&items);
    key_handle
        .set_value(
            &FLASH_LIST_TYPE,
            FlashListObject {
                tier: Tier::Hot(items),
                ttl_ms: old_ttl,
            },
        )
        .map_err(|e| ValkeyError::String(format!("flash: pop set_value: {e}")))?;

    // Restore TTL (set_value clears the Valkey-level expiry).
    if let Some(abs_ms) = old_ttl {
        use crate::commands::list_common::current_time_ms;
        use std::time::Duration;
        let remaining_ms = (abs_ms - current_time_ms()).max(1) as u64;
        key_handle
            .set_expire(Duration::from_millis(remaining_ms))
            .map_err(|e| ValkeyError::String(format!("flash: pop set_expire: {e}")))?;
    }

    cache.put(key.as_slice(), serialized.clone());

    ctx.replicate_verbatim();
    ctx.notify_keyspace_event(
        NotifyEvent::LIST,
        if pop_left { "flash.lpop" } else { "flash.rpop" },
        key,
    );

    let reply = build_pop_reply(count, popped.clone());

    #[cfg(not(test))]
    {
        if crate::replication::is_replica()
            || (STORAGE.get().is_none() && crate::replication::must_obey_client(ctx))
        {
            return Ok(reply);
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
        let handle = Box::new(PopCompletionHandle {
            tsc: valkey_module::ThreadSafeContext::with_blocked_client(bc),
            popped: Some(popped),
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
                        format!("flash: POP WAL append failed: {e}").as_str(),
                    );
                }
            }
            Ok(vec![])
        });
        return Ok(ValkeyValue::NoReply);
    }

    #[allow(unreachable_code)]
    Ok(reply)
}

fn build_pop_reply(count: Option<usize>, popped: Vec<Vec<u8>>) -> ValkeyValue {
    match count {
        None => {
            // Single-element mode (no count arg).
            popped
                .into_iter()
                .next()
                .map_or(ValkeyValue::Null, ValkeyValue::StringBuffer)
        }
        Some(_) => {
            // Array mode (count arg present).
            ValkeyValue::Array(popped.into_iter().map(ValkeyValue::StringBuffer).collect())
        }
    }
}

// ── Command handlers ──────────────────────────────────────────────────────────

/// `FLASH.LPOP key [count]`
pub fn flash_lpop_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    do_pop(ctx, args, true, "FLASH.LPOP")
}

/// `FLASH.RPOP key [count]`
pub fn flash_rpop_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    do_pop(ctx, args, false, "FLASH.RPOP")
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lpop_pops_from_front() {
        let mut items: VecDeque<Vec<u8>> = VecDeque::new();
        items.push_back(b"a".to_vec());
        items.push_back(b"b".to_vec());
        let front = items.pop_front().unwrap();
        assert_eq!(front, b"a");
        assert_eq!(items.len(), 1);
    }

    #[test]
    fn rpop_pops_from_back() {
        let mut items: VecDeque<Vec<u8>> = VecDeque::new();
        items.push_back(b"a".to_vec());
        items.push_back(b"b".to_vec());
        let back = items.pop_back().unwrap();
        assert_eq!(back, b"b");
        assert_eq!(items.len(), 1);
    }

    #[test]
    fn pop_count_limits_to_available() {
        let mut items: VecDeque<Vec<u8>> = VecDeque::new();
        items.push_back(b"x".to_vec());
        let n = 5usize.min(items.len());
        assert_eq!(n, 1);
    }

    #[test]
    fn build_pop_reply_no_count_returns_single() {
        let reply = build_pop_reply(None, vec![b"val".to_vec()]);
        assert!(matches!(reply, ValkeyValue::StringBuffer(_)));
    }

    #[test]
    fn build_pop_reply_with_count_returns_array() {
        let reply = build_pop_reply(Some(2), vec![b"a".to_vec(), b"b".to_vec()]);
        assert!(matches!(reply, ValkeyValue::Array(_)));
    }
}
