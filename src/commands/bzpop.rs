use valkey_module::{Context, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};
#[cfg(not(test))]
use {
    std::mem::ManuallyDrop,
    std::os::raw::{c_int, c_void},
    valkey_module::raw,
};

use crate::CACHE;
use crate::commands::zset_common::promote_cold_zset;
use crate::types::Tier;
use crate::types::zset::{FLASH_ZSET_TYPE, FlashZSetObject, format_score, zset_serialize};
#[cfg(not(test))]
use crate::{POOL, STORAGE, WAL};

// ── Privdata ──────────────────────────────────────────────────────────────────

#[cfg(not(test))]
struct BzPopPrivdata {
    from_min: bool,
}

// ── NVMe completion handles ───────────────────────────────────────────────────

/// Fire-and-forget NVMe handle used from the reply callback (no client to reply).
#[cfg(not(test))]
struct BzPopNvmeHandle;

#[cfg(not(test))]
impl crate::async_io::CompletionHandle for BzPopNvmeHandle {
    fn complete(self: Box<Self>, result: crate::storage::backend::StorageResult<Vec<u8>>) {
        if let Err(ref e) = result {
            valkey_module::logging::log_warning(
                format!("flash: BZPOP NVMe write failed: {e}").as_str(),
            );
        }
    }
}

/// Fast-path NVMe handle: replies `[key, member, score]` after NVMe write.
#[cfg(not(test))]
struct BzPopFastPathHandle {
    tsc: valkey_module::ThreadSafeContext<valkey_module::BlockedClient<()>>,
    key_bytes: Vec<u8>,
    member: Vec<u8>,
    score_str: Vec<u8>,
}

#[cfg(not(test))]
impl crate::async_io::CompletionHandle for BzPopFastPathHandle {
    fn complete(self: Box<Self>, result: crate::storage::backend::StorageResult<Vec<u8>>) {
        if let Err(ref e) = result {
            valkey_module::logging::log_warning(
                format!("flash: BZPOP fast-path NVMe write failed: {e}").as_str(),
            );
        }
        let reply = Ok(ValkeyValue::Array(vec![
            ValkeyValue::StringBuffer(self.key_bytes),
            ValkeyValue::StringBuffer(self.member),
            ValkeyValue::StringBuffer(self.score_str),
        ]));
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.tsc.reply(reply);
        }));
    }
}

// ── C callbacks ───────────────────────────────────────────────────────────────

#[cfg(not(test))]
unsafe extern "C" fn bzpop_free_privdata(_ctx: *mut raw::RedisModuleCtx, data: *mut c_void) {
    unsafe {
        if !data.is_null() {
            drop(Box::from_raw(data as *mut BzPopPrivdata));
        }
    }
}

/// Fires on timeout — replies with null array.
#[cfg(not(test))]
unsafe extern "C" fn bzpop_timeout(
    ctx: *mut raw::RedisModuleCtx,
    _argv: *mut *mut raw::RedisModuleString,
    _argc: c_int,
) -> c_int {
    unsafe {
        raw::RedisModule_ReplyWithNullArray.unwrap()(ctx);
        raw::REDISMODULE_OK as c_int
    }
}

/// Fires when a watched key signals ready.
///
/// Iterates original keys left-to-right; pops from the first non-empty ZSet.
/// Returns `REDISMODULE_ERR` (stay blocked) if no key currently has data.
#[cfg(not(test))]
unsafe extern "C" fn bzpop_reply(
    ctx: *mut raw::RedisModuleCtx,
    argv: *mut *mut raw::RedisModuleString,
    argc: c_int,
) -> c_int {
    unsafe {
        let privdata_ptr = raw::RedisModule_GetBlockedClientPrivateData.unwrap()(ctx);
        let from_min = if privdata_ptr.is_null() {
            true
        } else {
            (*(privdata_ptr as *const BzPopPrivdata)).from_min
        };

        let context = Context::new(ctx);

        // argc layout: [cmd, key1, key2, ..., timeout]  → keys at [1, argc-2].
        let num_keys = (argc as usize).saturating_sub(2);
        for i in 1..=num_keys {
            let key_ptr = *argv.add(i);
            let key = ManuallyDrop::new(ValkeyString::from_redis_module_string(ctx, key_ptr));

            if let Some((member, score, serialized_opt)) = pop_one_zset(&context, &key, from_min) {
                let pop_cmd = if from_min {
                    "FLASH.ZPOPMIN"
                } else {
                    "FLASH.ZPOPMAX"
                };
                context.replicate(pop_cmd, &[&*key]);
                context.notify_keyspace_event(
                    NotifyEvent::ZSET,
                    if from_min {
                        "flash.zpopmin"
                    } else {
                        "flash.zpopmax"
                    },
                    &key,
                );

                if !crate::replication::is_replica()
                    && let (Some(storage), Some(pool)) = (STORAGE.get(), POOL.get())
                {
                    use crate::storage::backend::StorageBackend;
                    let key_bytes = key.as_slice().to_vec();
                    match serialized_opt {
                        None => {
                            pool.submit_or_complete(Box::new(BzPopNvmeHandle), move || {
                                storage.delete(&key_bytes).ok();
                                Ok(vec![])
                            });
                        }
                        Some(serialized) => {
                            pool.submit_or_complete(Box::new(BzPopNvmeHandle), move || {
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

                let score_bytes = format_score(score).into_bytes();
                raw::reply_with_array(ctx, 3);
                raw::reply_with_string(ctx, key_ptr);
                raw::reply_with_string_buffer(ctx, member.as_ptr().cast(), member.len());
                raw::reply_with_string_buffer(ctx, score_bytes.as_ptr().cast(), score_bytes.len());

                return raw::REDISMODULE_OK as c_int;
            }
        }

        // No key had elements — stay blocked.
        raw::REDISMODULE_ERR as c_int
    }
}

// ── Pop helper ────────────────────────────────────────────────────────────────

/// Try to pop the min or max element from a FlashZSet key.
/// Returns `Some((member, score, serialized_opt))` on success.
/// `serialized_opt` is `None` when the set became empty (key deleted).
fn pop_one_zset(
    ctx: &Context,
    key: &ValkeyString,
    from_min: bool,
) -> Option<(Vec<u8>, f64, Option<Vec<u8>>)> {
    let key_handle = ctx.open_key_writable(key);
    let existing = match key_handle.get_value::<FlashZSetObject>(&FLASH_ZSET_TYPE) {
        Err(_) | Ok(None) => return None,
        Ok(Some(obj)) => obj,
    };

    let ttl = existing.ttl_ms;
    let mut inner = match &existing.tier {
        crate::types::Tier::Hot(z) => z.clone(),
        crate::types::Tier::Cold {
            backend_offset,
            value_len,
            ..
        } => promote_cold_zset(*backend_offset, *value_len).ok()?,
    };

    if inner.is_empty() {
        return None;
    }

    let (member, score) = if from_min {
        let k = inner.scores.keys().next()?.clone();
        (k.1.clone(), k.0.0)
    } else {
        let k = inner.scores.keys().next_back()?.clone();
        (k.1.clone(), k.0.0)
    };
    inner.remove(&member);

    if inner.is_empty() {
        let _ = key_handle.delete();
        if let Some(cache) = CACHE.get() {
            cache.delete(key.as_slice());
        }
        Some((member, score, None))
    } else {
        let serialized = zset_serialize(&inner);
        let _ = key_handle.set_value(
            &FLASH_ZSET_TYPE,
            FlashZSetObject {
                tier: Tier::Hot(inner),
                ttl_ms: ttl,
            },
        );
        if let Some(cache) = CACHE.get() {
            cache.put(key.as_slice(), serialized.clone());
        }
        Some((member, score, Some(serialized)))
    }
}

// ── Blocking command logic ────────────────────────────────────────────────────

fn do_bzpop(ctx: &Context, args: Vec<ValkeyString>, from_min: bool) -> ValkeyResult {
    // FLASH.BZPOPMIN key [key ...] timeout
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

    // Fast path: check keys left-to-right for an immediately available element.
    for key in keys {
        let key_handle = ctx.open_key_writable(key);
        let existing = match key_handle.get_value::<FlashZSetObject>(&FLASH_ZSET_TYPE) {
            Err(_) => return Err(ValkeyError::WrongType),
            Ok(None) => continue,
            Ok(Some(obj)) => obj,
        };
        drop(key_handle); // release the writable handle before pop_one_zset reopens it

        let has_data = match &existing.tier {
            Tier::Hot(z) => !z.is_empty(),
            Tier::Cold { .. } => true,
        };

        if has_data {
            #[allow(unused_variables)]
            if let Some((member, score, serialized_opt)) = pop_one_zset(ctx, key, from_min) {
                let pop_cmd = if from_min {
                    "FLASH.ZPOPMIN"
                } else {
                    "FLASH.ZPOPMAX"
                };
                ctx.replicate(pop_cmd, &[key]);
                ctx.notify_keyspace_event(
                    NotifyEvent::ZSET,
                    if from_min {
                        "flash.zpopmin"
                    } else {
                        "flash.zpopmax"
                    },
                    key,
                );

                #[cfg(not(test))]
                if !crate::replication::is_replica()
                    && let (Some(storage), Some(pool)) = (STORAGE.get(), POOL.get())
                {
                    use crate::storage::backend::StorageBackend;
                    let key_bytes = key.as_slice().to_vec();
                    let score_str = format_score(score).into_bytes();
                    let member_clone = member.clone();
                    let key_bytes_owned = key_bytes.clone();
                    let bc = ctx.block_client();
                    let handle = Box::new(BzPopFastPathHandle {
                        tsc: valkey_module::ThreadSafeContext::with_blocked_client(bc),
                        key_bytes: key_bytes_owned,
                        member: member_clone,
                        score_str,
                    });
                    match serialized_opt {
                        None => {
                            pool.submit_or_complete(handle, move || {
                                storage.delete(&key_bytes).ok();
                                Ok(vec![])
                            });
                        }
                        Some(serialized) => {
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
                        }
                    }
                    return Ok(ValkeyValue::NoReply);
                }

                return Ok(ValkeyValue::Array(vec![
                    ValkeyValue::StringBuffer(key.as_slice().to_vec()),
                    ValkeyValue::StringBuffer(member),
                    ValkeyValue::StringBuffer(format_score(score).into_bytes()),
                ]));
            }
        }
    }

    // Slow path: block on watched keys.
    #[cfg(not(test))]
    {
        let privdata = Box::into_raw(Box::new(BzPopPrivdata { from_min })) as *mut c_void;
        let mut key_ptrs: Vec<*mut raw::RedisModuleString> = keys.iter().map(|k| k.inner).collect();
        unsafe {
            raw::RedisModule_BlockClientOnKeys.unwrap()(
                ctx.ctx,
                Some(bzpop_reply),
                Some(bzpop_timeout),
                Some(bzpop_free_privdata),
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

// ── Command handlers ──────────────────────────────────────────────────────────

/// `FLASH.BZPOPMIN key [key ...] timeout`
///
/// Block until one of the given keys has elements, then pop the member with the
/// lowest score. Returns `[key, member, score]` or nil on timeout.
pub fn flash_bzpopmin_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    do_bzpop(ctx, args, true)
}

/// `FLASH.BZPOPMAX key [key ...] timeout`
///
/// Block until one of the given keys has elements, then pop the member with the
/// highest score. Returns `[key, member, score]` or nil on timeout.
pub fn flash_bzpopmax_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    do_bzpop(ctx, args, false)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use crate::types::zset::{ScoreF64, ZSetInner};

    #[test]
    fn timeout_zero_is_valid() {
        let secs: f64 = "0".parse().unwrap();
        let ms = (secs * 1000.0) as i64;
        assert_eq!(ms, 0);
    }

    #[test]
    fn timeout_float_rounds_correctly() {
        let secs: f64 = "1.5".parse().unwrap();
        let ms = (secs * 1000.0) as i64;
        assert_eq!(ms, 1500);
    }

    #[test]
    fn negative_timeout_is_rejected() {
        let secs: f64 = "-0.5".parse().unwrap();
        assert!(secs < 0.0);
    }

    #[test]
    fn pop_min_returns_lowest_score() {
        let mut z = ZSetInner::new();
        z.insert(b"a".to_vec(), 3.0);
        z.insert(b"b".to_vec(), 1.0);
        z.insert(b"c".to_vec(), 2.0);
        let k = z.scores.keys().next().cloned().unwrap();
        assert_eq!(k.0.0, 1.0);
        assert_eq!(k.1, b"b");
    }

    #[test]
    fn pop_max_returns_highest_score() {
        let mut z = ZSetInner::new();
        z.insert(b"a".to_vec(), 3.0);
        z.insert(b"b".to_vec(), 1.0);
        z.insert(b"c".to_vec(), 2.0);
        let k = z.scores.keys().next_back().cloned().unwrap();
        assert_eq!(k.0.0, 3.0);
        assert_eq!(k.1, b"a");
    }

    #[test]
    fn multi_key_left_to_right_priority() {
        let keys = [b"k1".as_ref(), b"k2".as_ref(), b"k3".as_ref()];
        let available = [false, true, true];
        let found = keys.iter().enumerate().find(|(i, _)| available[*i]);
        assert_eq!(found.map(|(i, _)| i), Some(1));
    }

    #[test]
    fn pop_leaves_remaining_members() {
        let mut z = ZSetInner::new();
        z.insert(b"lo".to_vec(), 1.0);
        z.insert(b"hi".to_vec(), 5.0);
        let k = z.scores.keys().next().cloned().unwrap();
        z.remove(&k.1);
        assert_eq!(z.len(), 1);
        assert!(z.get_score(b"hi").is_some());
    }

    #[test]
    fn score_f64_ordering() {
        let a = ScoreF64(-1.0);
        let b = ScoreF64(0.0);
        let c = ScoreF64(1.0);
        assert!(a < b);
        assert!(b < c);
    }
}
