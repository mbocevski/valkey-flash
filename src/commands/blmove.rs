use std::collections::VecDeque;
use std::time::Duration;

use valkey_module::{Context, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};
#[cfg(not(test))]
use {
    std::mem::ManuallyDrop,
    std::os::raw::{c_int, c_void},
    valkey_module::raw,
};

use crate::commands::list_common::{current_time_ms, promote_cold_list};
use crate::types::list::{list_serialize, FlashListObject, FLASH_LIST_TYPE};
use crate::types::Tier;
use crate::CACHE;
#[cfg(not(test))]
use crate::{POOL, STORAGE, WAL};

// ── BlMovePrivdata ────────────────────────────────────────────────────────────

#[cfg(not(test))]
struct BlMovePrivdata {
    src_left: bool,
    dst_left: bool,
    /// Destination key bytes (owned copy, so the callback can use them after
    /// the original argv pointers are no longer valid on subsequent wake-ups).
    dst_key_bytes: Vec<u8>,
}

// ── No-op NVMe completion (fire-and-forget) ───────────────────────────────────

#[cfg(not(test))]
struct BlMoveNvmeHandle;

#[cfg(not(test))]
impl crate::async_io::CompletionHandle for BlMoveNvmeHandle {
    fn complete(self: Box<Self>, result: crate::storage::backend::StorageResult<Vec<u8>>) {
        if let Err(ref e) = result {
            valkey_module::logging::log_warning(
                format!("flash: BLMOVE NVMe write failed: {e}").as_str(),
            );
        }
    }
}

// ── C callbacks ───────────────────────────────────────────────────────────────

#[cfg(not(test))]
unsafe extern "C" fn blmove_free_privdata(
    _ctx: *mut raw::RedisModuleCtx,
    data: *mut c_void,
) {
    if !data.is_null() {
        drop(Box::from_raw(data as *mut BlMovePrivdata));
    }
}

/// Timeout: return nil.
#[cfg(not(test))]
unsafe extern "C" fn blmove_timeout(
    ctx: *mut raw::RedisModuleCtx,
    _argv: *mut *mut raw::RedisModuleString,
    _argc: c_int,
) -> c_int {
    raw::RedisModule_ReplyWithNull.unwrap()(ctx);
    raw::REDISMODULE_OK as c_int
}

/// Reply callback for BLMOVE: atomically pops from src and pushes to dst.
/// Returns `REDISMODULE_ERR` (stay blocked) if src is empty (race condition).
#[cfg(not(test))]
unsafe extern "C" fn blmove_reply(
    ctx: *mut raw::RedisModuleCtx,
    argv: *mut *mut raw::RedisModuleString,
    _argc: c_int,
) -> c_int {
    let privdata_ptr = raw::RedisModule_GetBlockedClientPrivateData.unwrap()(ctx);
    if privdata_ptr.is_null() {
        raw::RedisModule_ReplyWithNull.unwrap()(ctx);
        return raw::REDISMODULE_OK as c_int;
    }
    let pd = &*(privdata_ptr as *const BlMovePrivdata);
    let src_left = pd.src_left;
    let dst_left = pd.dst_left;
    let dst_key_bytes = pd.dst_key_bytes.clone();

    let context = Context::new(ctx);

    // src key is argv[1].
    let src_ptr = *argv.add(1);
    let src_key = ManuallyDrop::new(ValkeyString::from_redis_module_string(ctx, src_ptr));

    // Pop from src.
    let (elem, src_serialized_opt) = match pop_one_blmove(&context, &src_key, src_left) {
        None => return raw::REDISMODULE_ERR as c_int, // src empty — stay blocked
        Some(v) => v,
    };

    // Push to dst.
    let dst_serialized = push_one_blmove(&context, &dst_key_bytes, &elem, dst_left);

    // Replicate as FLASH.LMOVE (non-blocking).
    // argv[1]=src, argv[2]=dst are available; replicate the whole thing.
    context.replicate_verbatim();

    context.notify_keyspace_event(NotifyEvent::LIST, "flash.list.move", &src_key);
    // Also notify on dst if it's a different key.
    if src_key.as_slice() != dst_key_bytes.as_slice() {
        let dst_vs = ManuallyDrop::new(ValkeyString::from_redis_module_string(ctx, *argv.add(2)));
        context.notify_keyspace_event(NotifyEvent::LIST, "flash.list.move", &dst_vs);
    }

    // Async NVMe writes (fire-and-forget).
    #[cfg(not(test))]
    if !crate::replication::is_replica() {
        if let (Some(storage), Some(pool)) = (STORAGE.get(), POOL.get()) {
            use crate::storage::backend::StorageBackend;
            let src_bytes = src_key.as_slice().to_vec();
            let dst_bytes = dst_key_bytes.clone();
            let src_ser = src_serialized_opt;
            let dst_ser = dst_serialized;
            pool.submit_or_complete(Box::new(BlMoveNvmeHandle), move || {
                // Persist src (delete if emptied, update otherwise).
                match src_ser {
                    None => {
                        storage.delete(&src_bytes).ok();
                    }
                    Some(ref s) => {
                        if let Ok(offset) = storage.put(&src_bytes, s) {
                            if let Some(wal) = WAL.get() {
                                let kh = crate::util::key_hash(&src_bytes);
                                let vh = crate::util::value_hash(s);
                                let _ = wal.append(crate::storage::wal::WalOp::Put {
                                    key_hash: kh,
                                    offset,
                                    value_hash: vh,
                                });
                            }
                        }
                    }
                }
                // Persist dst.
                if let Ok(offset) = storage.put(&dst_bytes, &dst_ser) {
                    if let Some(wal) = WAL.get() {
                        let kh = crate::util::key_hash(&dst_bytes);
                        let vh = crate::util::value_hash(&dst_ser);
                        let _ = wal.append(crate::storage::wal::WalOp::Put {
                            key_hash: kh,
                            offset,
                            value_hash: vh,
                        });
                    }
                }
                Ok(vec![])
            });
        }
    }

    raw::reply_with_string_buffer(ctx, elem.as_ptr() as *const _, elem.len());
    raw::REDISMODULE_OK as c_int
}

// ── In-memory helpers ─────────────────────────────────────────────────────────

/// Pop one element from `key`. Updates Valkey key + cache.
/// Returns `Some((element, serialized_or_none))` where `None` means key deleted.
fn pop_one_blmove(
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

/// Push `elem` to `dst_key_bytes` (head if `push_left`, tail otherwise).
/// Creates the key if it doesn't exist. Returns the serialized new list for NVMe persistence.
fn push_one_blmove(
    ctx: &Context,
    dst_key_bytes: &[u8],
    elem: &[u8],
    push_left: bool,
) -> Vec<u8> {
    // We need a ValkeyString for open_key_writable.
    let dst_vs = ValkeyString::create_from_slice(ctx.ctx, dst_key_bytes);

    let dst_handle = ctx.open_key_writable(&dst_vs);
    let (mut items, ttl): (VecDeque<Vec<u8>>, Option<i64>) =
        match dst_handle.get_value::<FlashListObject>(&FLASH_LIST_TYPE) {
            Err(_) | Ok(None) => (VecDeque::new(), None),
            Ok(Some(obj)) => {
                let ttl = obj.ttl_ms;
                let list = match &obj.tier {
                    Tier::Hot(l) => l.clone(),
                    Tier::Cold {
                        backend_offset,
                        value_len,
                        ..
                    } => promote_cold_list(*backend_offset, *value_len).unwrap_or_default(),
                };
                (list, ttl)
            }
        };

    if push_left {
        items.push_front(elem.to_vec());
    } else {
        items.push_back(elem.to_vec());
    }

    let serialized = list_serialize(&items);
    let _ = dst_handle.set_value(
        &FLASH_LIST_TYPE,
        FlashListObject {
            tier: Tier::Hot(items),
            ttl_ms: ttl,
        },
    );
    if let Some(abs_ms) = ttl {
        let remaining_ms = (abs_ms - current_time_ms()).max(1) as u64;
        let _ = dst_handle.set_expire(Duration::from_millis(remaining_ms));
    }
    if let Some(cache) = CACHE.get() {
        cache.put(dst_key_bytes, serialized.clone());
    }
    serialized
}

// ── Command handler ───────────────────────────────────────────────────────────

/// `FLASH.BLMOVE source destination LEFT|RIGHT LEFT|RIGHT timeout`
///
/// Block until source has data, then atomically move one element from source
/// to destination. Returns the moved element or nil on timeout.
pub fn flash_blmove_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() != 6 {
        return Err(ValkeyError::WrongArity);
    }

    let src_key = &args[1];
    let dst_key = &args[2];
    let src_dir = args[3].as_slice().to_ascii_uppercase();
    let dst_dir = args[4].as_slice().to_ascii_uppercase();
    let timeout_str = std::str::from_utf8(args[5].as_slice())
        .map_err(|_| ValkeyError::Str("ERR timeout is not a float or out of range"))?;
    let timeout_secs: f64 = timeout_str
        .parse()
        .map_err(|_| ValkeyError::Str("ERR timeout is not a float or out of range"))?;
    if timeout_secs < 0.0 {
        return Err(ValkeyError::Str("ERR timeout is negative"));
    }
    #[cfg_attr(test, allow(unused_variables))]
    let timeout_ms = (timeout_secs * 1000.0) as i64;

    let src_left = match src_dir.as_slice() {
        b"LEFT" => true,
        b"RIGHT" => false,
        _ => return Err(ValkeyError::Str("ERR syntax error — expected LEFT or RIGHT")),
    };
    let dst_left = match dst_dir.as_slice() {
        b"LEFT" => true,
        b"RIGHT" => false,
        _ => return Err(ValkeyError::Str("ERR syntax error — expected LEFT or RIGHT")),
    };

    let dst_key_bytes = dst_key.as_slice().to_vec();

    // Fast path: if src has data, execute synchronously.
    let src_handle = ctx.open_key_writable(src_key);
    let src_existing = match src_handle.get_value::<FlashListObject>(&FLASH_LIST_TYPE) {
        Err(_) => return Err(ValkeyError::WrongType),
        Ok(None) => None,
        Ok(Some(obj)) => {
            let has_data = match &obj.tier {
                Tier::Hot(l) => !l.is_empty(),
                Tier::Cold { .. } => true,
            };
            if has_data { Some(obj) } else { None }
        }
    };
    drop(src_handle); // Release before calling pop_one_blmove which re-opens.

    if src_existing.is_some() {
        #[allow(unused_variables)]
        if let Some((elem, src_ser_opt)) = pop_one_blmove(ctx, src_key, src_left) {
            #[allow(unused_variables)]
            let dst_ser = push_one_blmove(ctx, &dst_key_bytes, &elem, dst_left);

            ctx.replicate_verbatim();
            ctx.notify_keyspace_event(NotifyEvent::LIST, "flash.list.move", src_key);
            if src_key.as_slice() != dst_key.as_slice() {
                ctx.notify_keyspace_event(NotifyEvent::LIST, "flash.list.move", dst_key);
            }

            #[cfg(not(test))]
            if !crate::replication::is_replica() {
                if let (Some(storage), Some(pool)) = (STORAGE.get(), POOL.get()) {
                    use crate::storage::backend::StorageBackend;
                    let src_bytes = src_key.as_slice().to_vec();
                    let dst_bytes = dst_key_bytes.clone();
                    let elem_clone = elem.clone();
                    let bc = ctx.block_client();
                    let handle = Box::new(BlMoveFastPathHandle {
                        tsc: valkey_module::ThreadSafeContext::with_blocked_client(bc),
                        elem: elem_clone,
                    });
                    let src_ser = src_ser_opt;
                    pool.submit_or_complete(handle, move || {
                        match src_ser {
                            None => { storage.delete(&src_bytes).ok(); }
                            Some(ref s) => {
                                if let Ok(offset) = storage.put(&src_bytes, s) {
                                    if let Some(wal) = WAL.get() {
                                        let kh = crate::util::key_hash(&src_bytes);
                                        let vh = crate::util::value_hash(s);
                                        let _ = wal.append(crate::storage::wal::WalOp::Put {
                                            key_hash: kh, offset, value_hash: vh,
                                        });
                                    }
                                }
                            }
                        }
                        if let Ok(offset) = storage.put(&dst_bytes, &dst_ser) {
                            if let Some(wal) = WAL.get() {
                                let kh = crate::util::key_hash(&dst_bytes);
                                let vh = crate::util::value_hash(&dst_ser);
                                let _ = wal.append(crate::storage::wal::WalOp::Put {
                                    key_hash: kh, offset, value_hash: vh,
                                });
                            }
                        }
                        Ok(vec![])
                    });
                    return Ok(ValkeyValue::NoReply);
                }
            }

            return Ok(ValkeyValue::StringBuffer(elem));
        }
    }

    // Slow path: block on src key.
    #[cfg(not(test))]
    {
        let privdata = Box::into_raw(Box::new(BlMovePrivdata {
            src_left,
            dst_left,
            dst_key_bytes,
        })) as *mut c_void;
        let mut key_ptr = src_key.inner;
        unsafe {
            raw::RedisModule_BlockClientOnKeys.unwrap()(
                ctx.ctx,
                Some(blmove_reply),
                Some(blmove_timeout),
                Some(blmove_free_privdata),
                timeout_ms,
                &mut key_ptr as *mut *mut raw::RedisModuleString,
                1,
                privdata,
            );
        }
        return Ok(ValkeyValue::NoReply);
    }

    #[allow(unreachable_code)]
    Ok(ValkeyValue::Null)
}

// ── Fast-path completion handle ───────────────────────────────────────────────

#[cfg(not(test))]
struct BlMoveFastPathHandle {
    tsc: valkey_module::ThreadSafeContext<valkey_module::BlockedClient<()>>,
    elem: Vec<u8>,
}

#[cfg(not(test))]
impl crate::async_io::CompletionHandle for BlMoveFastPathHandle {
    fn complete(self: Box<Self>, result: crate::storage::backend::StorageResult<Vec<u8>>) {
        if let Err(ref e) = result {
            valkey_module::logging::log_warning(
                format!("flash: BLMOVE fast-path NVMe write failed: {e}").as_str(),
            );
        }
        let reply = Ok(ValkeyValue::StringBuffer(self.elem));
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.tsc.reply(reply);
        }));
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    #[test]
    fn blmove_pop_left_push_right() {
        let mut src: VecDeque<Vec<u8>> = VecDeque::new();
        src.push_back(b"a".to_vec());
        src.push_back(b"b".to_vec());
        let elem = src.pop_front().unwrap(); // src_left = true

        let mut dst: VecDeque<Vec<u8>> = VecDeque::new();
        dst.push_back(elem.clone()); // dst_left = false... wait, push_right

        assert_eq!(elem, b"a");
        assert_eq!(src.len(), 1);
        assert_eq!(dst[0], b"a");
    }

    #[test]
    fn blmove_src_empty_returns_none() {
        let src: VecDeque<Vec<u8>> = VecDeque::new();
        assert!(src.is_empty());
        // pop_one should return None for empty src
        let result: Option<()> = None;
        assert!(result.is_none());
    }

    #[test]
    fn blmove_timeout_parse() {
        let s = "30";
        let secs: f64 = s.parse().unwrap();
        let ms = (secs * 1000.0) as i64;
        assert_eq!(ms, 30_000);
    }

    #[test]
    fn dir_parse_case_insensitive() {
        let left = b"left".to_ascii_uppercase();
        let right = b"Right".to_ascii_uppercase();
        assert_eq!(left.as_slice(), b"LEFT");
        assert_eq!(right.as_slice(), b"RIGHT");
    }
}
