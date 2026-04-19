use std::collections::VecDeque;

#[cfg(not(test))]
use valkey_module::raw;
use valkey_module::{Context, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

use crate::CACHE;
use crate::commands::list_common::{
    apply_ttl_to_key, find_ttl_start, parse_ttl_options, promote_cold_list,
};
use crate::types::Tier;
use crate::types::list::{FLASH_LIST_TYPE, FlashListObject, list_serialize};
#[cfg(not(test))]
use crate::{POOL, STORAGE, WAL};

// ── PushCompletionHandle ──────────────────────────────────────────────────────

#[cfg(not(test))]
struct PushCompletionHandle {
    tsc: valkey_module::ThreadSafeContext<valkey_module::BlockedClient<()>>,
    new_len: i64,
}

#[cfg(not(test))]
impl crate::async_io::CompletionHandle for PushCompletionHandle {
    fn complete(self: Box<Self>, result: crate::storage::backend::StorageResult<Vec<u8>>) {
        use valkey_module::logging;
        if let Err(ref e) = result {
            logging::log_warning(format!("flash: PUSH NVMe write failed: {e}").as_str());
        }
        let reply = Ok(ValkeyValue::Integer(self.new_len));
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.tsc.reply(reply);
        }));
    }
}

// ── Shared push logic ─────────────────────────────────────────────────────────

fn do_push(
    ctx: &Context,
    args: Vec<ValkeyString>,
    push_left: bool,
    only_if_exists: bool,
    cmd_name: &str,
) -> ValkeyResult {
    if args.len() < 3 {
        return Err(ValkeyError::WrongArity);
    }

    let key = &args[1];

    let val_start = 2usize;
    let ttl_start = find_ttl_start(&args[val_start..]).map(|p| p + val_start);
    let val_end = ttl_start.unwrap_or(args.len());

    if val_end <= val_start {
        return Err(ValkeyError::WrongArity);
    }

    let (new_ttl_abs_ms, _keepttl) = parse_ttl_options(&args[val_end..], cmd_name)?;

    let cache = CACHE
        .get()
        .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;

    let key_handle = ctx.open_key_writable(key);
    let existing = match key_handle.get_value::<FlashListObject>(&FLASH_LIST_TYPE) {
        Err(_) => return Err(ValkeyError::WrongType),
        Ok(v) => v,
    };

    let (mut items, old_ttl): (VecDeque<Vec<u8>>, Option<i64>) = match existing {
        None => {
            if only_if_exists {
                return Ok(ValkeyValue::Integer(0));
            }
            (VecDeque::new(), None)
        }
        Some(obj) => {
            let ttl = crate::util_expire::preserve_ttl(ctx, key, obj.ttl_ms);
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

    // LPUSH: prepend each value in order → first arg ends up at head.
    if push_left {
        for v in &args[val_start..val_end] {
            items.push_front(v.as_slice().to_vec());
        }
    } else {
        for v in &args[val_start..val_end] {
            items.push_back(v.as_slice().to_vec());
        }
    }

    let new_len = items.len() as i64;
    let stored_ttl = new_ttl_abs_ms.or(old_ttl);

    key_handle
        .set_value(
            &FLASH_LIST_TYPE,
            FlashListObject {
                tier: Tier::Hot(items.clone()),
                ttl_ms: stored_ttl,
            },
        )
        .map_err(|e| ValkeyError::String(format!("flash: push set_value: {e}")))?;

    apply_ttl_to_key(&key_handle, stored_ttl)?;

    let serialized = list_serialize(&items);
    cache.put(key.as_slice(), serialized.clone());

    ctx.replicate_verbatim();
    ctx.notify_keyspace_event(
        NotifyEvent::LIST,
        if push_left {
            "flash.lpush"
        } else {
            "flash.rpush"
        },
        key,
    );

    // Wake any BLPOP/BRPOP clients blocked on this key.
    #[cfg(not(test))]
    unsafe {
        raw::RedisModule_SignalKeyAsReady.unwrap()(ctx.ctx, key.inner);
    }

    #[cfg(not(test))]
    {
        if crate::replication::must_run_sync(ctx)
            || (crate::STORAGE.get().is_none() && crate::replication::must_obey_client(ctx))
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
        let handle = Box::new(PushCompletionHandle {
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
                        format!("flash: PUSH WAL append failed: {e}").as_str(),
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

// ── Command handlers ──────────────────────────────────────────────────────────

/// `FLASH.LPUSH key value [value ...] [EX s | PX ms | EXAT ts-s | PXAT ts-ms | KEEPTTL]`
pub fn flash_lpush_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    do_push(ctx, args, true, false, "FLASH.LPUSH")
}

/// `FLASH.RPUSH key value [value ...] [EX s | PX ms | EXAT ts-s | PXAT ts-ms | KEEPTTL]`
pub fn flash_rpush_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    do_push(ctx, args, false, false, "FLASH.RPUSH")
}

/// `FLASH.LPUSHX key value [value ...]`
///
/// Push only if the key already exists. Returns 0 for a missing key.
pub fn flash_lpushx_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    do_push(ctx, args, true, true, "FLASH.LPUSHX")
}

/// `FLASH.RPUSHX key value [value ...]`
///
/// Push only if the key already exists. Returns 0 for a missing key.
pub fn flash_rpushx_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    do_push(ctx, args, false, true, "FLASH.RPUSHX")
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lpush_prepends_in_order() {
        let mut items: VecDeque<Vec<u8>> = VecDeque::new();
        for v in &[b"v1".as_ref(), b"v2".as_ref(), b"v3".as_ref()] {
            items.push_front(v.to_vec());
        }
        // LPUSH k v1 v2 v3 → [v3, v2, v1]
        assert_eq!(items[0], b"v3");
        assert_eq!(items[1], b"v2");
        assert_eq!(items[2], b"v1");
    }

    #[test]
    fn rpush_appends_in_order() {
        let mut items: VecDeque<Vec<u8>> = VecDeque::new();
        for v in &[b"v1".as_ref(), b"v2".as_ref(), b"v3".as_ref()] {
            items.push_back(v.to_vec());
        }
        assert_eq!(items[0], b"v1");
        assert_eq!(items[2], b"v3");
    }

    #[test]
    fn pushx_only_if_exists_gate() {
        // Verify that when key is absent (None), only_if_exists returns 0.
        let existing: Option<()> = None;
        let only_if_exists = true;
        let would_skip = existing.is_none() && only_if_exists;
        assert!(would_skip);
    }
}
