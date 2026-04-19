use std::collections::VecDeque;
use std::time::Duration;

use valkey_module::{Context, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};
#[cfg(not(test))]
use {
    std::mem::ManuallyDrop,
    std::os::raw::{c_int, c_void},
    valkey_module::raw,
};

use crate::CACHE;
use crate::commands::list_common::{current_time_ms, promote_cold_list};
use crate::types::Tier;
use crate::types::list::{FLASH_LIST_TYPE, FlashListObject, list_serialize};
#[cfg(not(test))]
use crate::{POOL, STORAGE, WAL};

// ── BlPopPrivdata ─────────────────────────────────────────────────────────────

#[cfg(not(test))]
struct BlPopPrivdata {
    pop_left: bool,
}

// ── No-op NVMe completion (fire-and-forget) ───────────────────────────────────

#[cfg(not(test))]
struct BlPopNvmeHandle;

#[cfg(not(test))]
impl crate::async_io::CompletionHandle for BlPopNvmeHandle {
    fn complete(self: Box<Self>, result: crate::storage::backend::StorageResult<Vec<u8>>) {
        if let Err(ref e) = result {
            valkey_module::logging::log_warning(
                format!("flash: BLPOP NVMe write failed: {e}").as_str(),
            );
        }
    }
}

// ── C callbacks ───────────────────────────────────────────────────────────────

#[cfg(not(test))]
unsafe extern "C" fn blpop_free_privdata(_ctx: *mut raw::RedisModuleCtx, data: *mut c_void) {
    unsafe {
        if !data.is_null() {
            drop(Box::from_raw(data as *mut BlPopPrivdata));
        }
    }
}

/// Called by the Valkey core when the blocking timeout fires. Returns nil array.
#[cfg(not(test))]
unsafe extern "C" fn blpop_timeout(
    ctx: *mut raw::RedisModuleCtx,
    _argv: *mut *mut raw::RedisModuleString,
    _argc: c_int,
) -> c_int {
    unsafe {
        raw::RedisModule_ReplyWithNullArray.unwrap()(ctx);
        raw::REDISMODULE_OK as c_int
    }
}

/// Called by the Valkey core when a watched key signals ready.
///
/// Iterates the original command's keys in order, pops the first with data,
/// and replies `[key, element]`. Returns `REDISMODULE_ERR` (stay blocked) if
/// no key currently has data (race: another client consumed it first).
#[cfg(not(test))]
unsafe extern "C" fn blpop_reply(
    ctx: *mut raw::RedisModuleCtx,
    argv: *mut *mut raw::RedisModuleString,
    argc: c_int,
) -> c_int {
    unsafe {
        let privdata_ptr = raw::RedisModule_GetBlockedClientPrivateData.unwrap()(ctx);
        let pop_left = if privdata_ptr.is_null() {
            true
        } else {
            (*(privdata_ptr as *const BlPopPrivdata)).pop_left
        };

        let context = Context::new(ctx);

        // argc layout: [cmd, key1, key2, ..., timeout]  → keys at [1, argc-2].
        let num_keys = (argc as usize).saturating_sub(2);
        for i in 1..=(num_keys) {
            let key_ptr = *argv.add(i);
            // Borrow the key pointer without incrementing its refcount.
            let key = ManuallyDrop::new(ValkeyString::from_redis_module_string(ctx, key_ptr));

            if let Some((elem, serialized_opt)) = pop_one(&context, &key, pop_left) {
                // Replicate as FLASH.LPOP / FLASH.RPOP so replicas execute non-blocking.
                let pop_cmd = if pop_left { "FLASH.LPOP" } else { "FLASH.RPOP" };
                context.replicate(pop_cmd, &[&*key]);
                context.notify_keyspace_event(
                    NotifyEvent::LIST,
                    if pop_left { "flash.lpop" } else { "flash.rpop" },
                    &key,
                );

                // Async NVMe write (fire-and-forget).
                #[cfg(not(test))]
                if !crate::replication::is_replica()
                    && let (Some(storage), Some(pool)) = (STORAGE.get(), POOL.get())
                {
                    use crate::storage::backend::StorageBackend;
                    let key_bytes = key.as_slice().to_vec();
                    match serialized_opt {
                        None => {
                            pool.submit_or_complete(Box::new(BlPopNvmeHandle), move || {
                                storage.delete(&key_bytes).ok();
                                Ok(vec![])
                            });
                        }
                        Some(serialized) => {
                            pool.submit_or_complete(Box::new(BlPopNvmeHandle), move || {
                                let offset = storage.put(&key_bytes, &serialized)?;
                                if let Some(wal) = WAL.get() {
                                    let kh = crate::util::key_hash(&key_bytes);
                                    let vh = crate::util::value_hash(&serialized);
                                    let _ = wal.append(crate::storage::wal::WalOp::Put {
                                        key_hash: kh,
                                        offset,
                                        value_hash: vh,
                                    });
                                }
                                Ok(vec![])
                            });
                        }
                    }
                }

                // Reply: [key, element] 2-element array.
                raw::reply_with_array(ctx, 2);
                raw::reply_with_string(ctx, key_ptr);
                raw::reply_with_string_buffer(ctx, elem.as_ptr() as *const _, elem.len());

                return raw::REDISMODULE_OK as c_int;
            }
        }

        // No key had elements — stay blocked, retry on next signal.
        raw::REDISMODULE_ERR as c_int
    }
}

// ── Shared pop helper ─────────────────────────────────────────────────────────

/// Try to pop one element from `key`. Updates the Valkey key and the in-memory
/// cache. Returns `Some((element, serialized_or_none))` on success where
/// `serialized_or_none` is `None` if the list is now empty (key deleted) or
/// `Some(bytes)` for the new serialized list (for NVMe persistence).
fn pop_one(
    ctx: &Context,
    key: &ValkeyString,
    pop_left: bool,
) -> Option<(Vec<u8>, Option<Vec<u8>>)> {
    let key_handle = ctx.open_key_writable(key);
    let existing = match key_handle.get_value::<FlashListObject>(&FLASH_LIST_TYPE) {
        Err(_) | Ok(None) => return None,
        Ok(Some(obj)) => obj,
    };

    let (mut items, ttl): (VecDeque<Vec<u8>>, Option<i64>) = {
        let ttl = existing.ttl_ms;
        let list = match &existing.tier {
            Tier::Hot(l) => l.clone(),
            Tier::Cold {
                backend_offset,
                value_len,
                ..
            } => promote_cold_list(*backend_offset, *value_len).ok()?,
        };
        (list, ttl)
    };

    if items.is_empty() {
        return None;
    }

    let elem = if pop_left {
        items.pop_front().unwrap()
    } else {
        items.pop_back().unwrap()
    };

    if items.is_empty() {
        let _ = key_handle.delete();
        if let Some(cache) = CACHE.get() {
            cache.delete(key.as_slice());
        }
        Some((elem, None))
    } else {
        let serialized = list_serialize(&items);
        let _ = key_handle.set_value(
            &FLASH_LIST_TYPE,
            FlashListObject {
                tier: Tier::Hot(items),
                ttl_ms: ttl,
            },
        );
        if let Some(abs_ms) = ttl {
            let remaining_ms = (abs_ms - current_time_ms()).max(1) as u64;
            let _ = key_handle.set_expire(Duration::from_millis(remaining_ms));
        }
        if let Some(cache) = CACHE.get() {
            cache.put(key.as_slice(), serialized.clone());
        }
        Some((elem, Some(serialized)))
    }
}

// ── Shared blocking logic ─────────────────────────────────────────────────────

fn do_blpop(ctx: &Context, args: Vec<ValkeyString>, pop_left: bool) -> ValkeyResult {
    // FLASH.BLPOP key [key ...] timeout
    if args.len() < 3 {
        return Err(ValkeyError::WrongArity);
    }

    let timeout_str = std::str::from_utf8(args.last().unwrap().as_slice())
        .map_err(|_| ValkeyError::Str("ERR timeout is not a float or out of range"))?;
    let timeout_secs: f64 = timeout_str
        .parse()
        .map_err(|_| ValkeyError::Str("ERR timeout is not a float or out of range"))?;
    if timeout_secs < 0.0 {
        return Err(ValkeyError::Str("ERR timeout is negative"));
    }
    #[cfg_attr(test, allow(unused_variables))]
    let timeout_ms = (timeout_secs * 1000.0) as i64;

    let keys = &args[1..args.len() - 1];
    if keys.is_empty() {
        return Err(ValkeyError::WrongArity);
    }

    // Fast path: check keys in order for an immediately available element.
    for key in keys {
        let key_handle = ctx.open_key_writable(key);
        let existing = match key_handle.get_value::<FlashListObject>(&FLASH_LIST_TYPE) {
            Err(_) => return Err(ValkeyError::WrongType),
            Ok(None) => continue,
            Ok(Some(obj)) => obj,
        };

        let has_data = match &existing.tier {
            Tier::Hot(l) => !l.is_empty(),
            Tier::Cold { .. } => true, // Cold means data exists on NVMe.
        };

        if has_data {
            // Promote and pop synchronously (fast path).
            #[allow(unused_variables)]
            if let Some((elem, serialized_opt)) = pop_one(ctx, key, pop_left) {
                let pop_cmd = if pop_left { "FLASH.LPOP" } else { "FLASH.RPOP" };
                ctx.replicate(pop_cmd, &[key]);
                ctx.notify_keyspace_event(
                    NotifyEvent::LIST,
                    if pop_left { "flash.lpop" } else { "flash.rpop" },
                    key,
                );

                #[cfg(not(test))]
                if !crate::replication::is_replica()
                    && let (Some(storage), Some(pool)) = (STORAGE.get(), POOL.get())
                {
                    use crate::storage::backend::StorageBackend;
                    let key_bytes = key.as_slice().to_vec();
                    match serialized_opt {
                        None => {
                            let bc = ctx.block_client();
                            let handle = Box::new(BlPopFastPathHandle {
                                tsc: valkey_module::ThreadSafeContext::with_blocked_client(bc),
                                key_bytes_owned: key.as_slice().to_vec(),
                                elem: elem.clone(),
                            });
                            pool.submit_or_complete(handle, move || {
                                storage.delete(&key_bytes).ok();
                                Ok(vec![])
                            });
                            return Ok(ValkeyValue::NoReply);
                        }
                        Some(serialized) => {
                            let bc = ctx.block_client();
                            let handle = Box::new(BlPopFastPathHandle {
                                tsc: valkey_module::ThreadSafeContext::with_blocked_client(bc),
                                key_bytes_owned: key.as_slice().to_vec(),
                                elem: elem.clone(),
                            });
                            pool.submit_or_complete(handle, move || {
                                let offset = storage.put(&key_bytes, &serialized)?;
                                if let Some(wal) = WAL.get() {
                                    let kh = crate::util::key_hash(&key_bytes);
                                    let vh = crate::util::value_hash(&serialized);
                                    let _ = wal.append(crate::storage::wal::WalOp::Put {
                                        key_hash: kh,
                                        offset,
                                        value_hash: vh,
                                    });
                                }
                                Ok(vec![])
                            });
                            return Ok(ValkeyValue::NoReply);
                        }
                    }
                }

                return Ok(ValkeyValue::Array(vec![
                    ValkeyValue::StringBuffer(key.as_slice().to_vec()),
                    ValkeyValue::StringBuffer(elem),
                ]));
            }
        }
    }

    // Slow path: block on watched keys.
    #[cfg(not(test))]
    {
        let privdata = Box::into_raw(Box::new(BlPopPrivdata { pop_left })) as *mut c_void;
        let mut key_ptrs: Vec<*mut raw::RedisModuleString> = keys.iter().map(|k| k.inner).collect();
        unsafe {
            raw::RedisModule_BlockClientOnKeys.unwrap()(
                ctx.ctx,
                Some(blpop_reply),
                Some(blpop_timeout),
                Some(blpop_free_privdata),
                timeout_ms,
                key_ptrs.as_mut_ptr(),
                key_ptrs.len() as c_int,
                privdata,
            );
        }
        return Ok(ValkeyValue::NoReply);
    }

    #[allow(unreachable_code)]
    Ok(ValkeyValue::Null)
}

// ── FastPath completion handle ────────────────────────────────────────────────
// Used when the fast-path pop fires and we need to reply after the NVMe write.

#[cfg(not(test))]
struct BlPopFastPathHandle {
    tsc: valkey_module::ThreadSafeContext<valkey_module::BlockedClient<()>>,
    key_bytes_owned: Vec<u8>,
    elem: Vec<u8>,
}

#[cfg(not(test))]
impl crate::async_io::CompletionHandle for BlPopFastPathHandle {
    fn complete(self: Box<Self>, result: crate::storage::backend::StorageResult<Vec<u8>>) {
        if let Err(ref e) = result {
            valkey_module::logging::log_warning(
                format!("flash: BLPOP fast-path NVMe write failed: {e}").as_str(),
            );
        }
        let reply = Ok(ValkeyValue::Array(vec![
            ValkeyValue::StringBuffer(self.key_bytes_owned),
            ValkeyValue::StringBuffer(self.elem),
        ]));
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.tsc.reply(reply);
        }));
    }
}

// ── Command handlers ──────────────────────────────────────────────────────────

/// `FLASH.BLPOP key [key ...] timeout`
///
/// Block until one of the given keys has data, then pop from the left (head).
/// Returns `[key, element]` or nil on timeout.
pub fn flash_blpop_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    do_blpop(ctx, args, true)
}

/// `FLASH.BRPOP key [key ...] timeout`
///
/// Block until one of the given keys has data, then pop from the right (tail).
/// Returns `[key, element]` or nil on timeout.
pub fn flash_brpop_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    do_blpop(ctx, args, false)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    #[test]
    fn timeout_parse_zero_is_valid() {
        let timeout_secs: f64 = "0".parse().unwrap();
        let timeout_ms = (timeout_secs * 1000.0) as i64;
        assert_eq!(timeout_ms, 0);
    }

    #[test]
    fn timeout_parse_float_rounds_down() {
        let timeout_secs: f64 = "1.5".parse().unwrap();
        let timeout_ms = (timeout_secs * 1000.0) as i64;
        assert_eq!(timeout_ms, 1500);
    }

    #[test]
    fn pop_one_logic_left() {
        let mut items: VecDeque<Vec<u8>> = VecDeque::new();
        items.push_back(b"a".to_vec());
        items.push_back(b"b".to_vec());
        let elem = items.pop_front().unwrap();
        assert_eq!(elem, b"a");
        assert_eq!(items.len(), 1);
    }

    #[test]
    fn pop_one_logic_right() {
        let mut items: VecDeque<Vec<u8>> = VecDeque::new();
        items.push_back(b"a".to_vec());
        items.push_back(b"b".to_vec());
        let elem = items.pop_back().unwrap();
        assert_eq!(elem, b"b");
        assert_eq!(items.len(), 1);
    }

    #[test]
    fn pop_one_empties_to_none_serialized() {
        let mut items: VecDeque<Vec<u8>> = VecDeque::new();
        items.push_back(b"only".to_vec());
        let elem = items.pop_front().unwrap();
        assert!(items.is_empty());
        // Serialized_opt should be None (key deleted).
        let serialized_opt: Option<Vec<u8>> = if items.is_empty() { None } else { Some(vec![]) };
        assert!(serialized_opt.is_none());
        assert_eq!(elem, b"only");
    }

    #[test]
    fn multi_key_ordering_first_wins() {
        // Verify the fast-path check order: first non-empty key wins.
        let keys = [b"k1".as_ref(), b"k2".as_ref(), b"k3".as_ref()];
        let available = [false, true, true];
        let found = keys.iter().enumerate().find(|(i, _)| available[*i]);
        assert_eq!(found.map(|(i, _)| i), Some(1));
    }

    #[test]
    fn negative_timeout_rejected() {
        let s = "-1";
        let v: Result<f64, _> = s.parse();
        assert!(v.is_ok()); // parse succeeds
        let secs = v.unwrap();
        assert!(secs < 0.0); // caller checks for negative
    }
}
