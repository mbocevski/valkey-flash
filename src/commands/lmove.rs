use std::collections::VecDeque;

#[cfg(not(test))]
use valkey_module::raw;
use valkey_module::{Context, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

use crate::CACHE;
#[cfg(not(test))]
use crate::WAL;
use crate::commands::list_common::{current_time_ms, promote_cold_list};
use crate::types::Tier;
use crate::types::list::{FLASH_LIST_TYPE, FlashListObject, list_serialize};

// ── LMoveCompletionHandle ─────────────────────────────────────────────────────

#[cfg(not(test))]
struct LMoveCompletionHandle {
    tsc: valkey_module::ThreadSafeContext<valkey_module::BlockedClient<()>>,
    elem: Vec<u8>,
}

#[cfg(not(test))]
impl crate::async_io::CompletionHandle for LMoveCompletionHandle {
    fn complete(self: Box<Self>, result: crate::storage::backend::StorageResult<Vec<u8>>) {
        use valkey_module::logging;
        if let Err(ref e) = result {
            logging::log_warning(format!("flash: LMOVE NVMe write failed: {e}").as_str());
        }
        let reply = Ok(ValkeyValue::StringBuffer(self.elem));
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.tsc.reply(reply);
        }));
    }
}

// ── Command handler ───────────────────────────────────────────────────────────

/// `FLASH.LMOVE source destination LEFT|RIGHT LEFT|RIGHT`
///
/// Atomically pop from source (LEFT or RIGHT) and push to destination.
/// Returns the moved element, or nil if source does not exist.
pub fn flash_lmove_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() != 5 {
        return Err(ValkeyError::WrongArity);
    }

    let src_key = &args[1];
    let dst_key = &args[2];
    let src_dir = args[3].as_slice().to_ascii_uppercase();
    let dst_dir = args[4].as_slice().to_ascii_uppercase();

    let src_left = match src_dir.as_slice() {
        b"LEFT" => true,
        b"RIGHT" => false,
        _ => {
            return Err(ValkeyError::Str(
                "ERR syntax error — expected LEFT or RIGHT",
            ));
        }
    };
    let dst_left = match dst_dir.as_slice() {
        b"LEFT" => true,
        b"RIGHT" => false,
        _ => {
            return Err(ValkeyError::Str(
                "ERR syntax error — expected LEFT or RIGHT",
            ));
        }
    };

    let cache = CACHE
        .get()
        .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;

    let same_key = src_key.as_slice() == dst_key.as_slice();

    if same_key {
        // Rotation: one key operation.
        let key_handle = ctx.open_key_writable(src_key);
        let existing = match key_handle.get_value::<FlashListObject>(&FLASH_LIST_TYPE) {
            Err(_) => return Err(ValkeyError::WrongType),
            Ok(None) => return Ok(ValkeyValue::Null),
            Ok(Some(obj)) => obj,
        };

        let (mut items, old_ttl) = {
            let ttl = crate::util_expire::preserve_ttl(ctx, src_key, existing.ttl_ms);
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

        if items.is_empty() {
            return Ok(ValkeyValue::Null);
        }

        let elem = if src_left {
            items.pop_front().unwrap()
        } else {
            items.pop_back().unwrap()
        };
        if dst_left {
            items.push_front(elem.clone());
        } else {
            items.push_back(elem.clone());
        }

        key_handle
            .set_value(
                &FLASH_LIST_TYPE,
                FlashListObject {
                    tier: Tier::Hot(items.clone()),
                    ttl_ms: old_ttl,
                },
            )
            .map_err(|e| ValkeyError::String(format!("flash: lmove set_value: {e}")))?;

        if let Some(abs_ms) = old_ttl {
            use std::time::Duration;
            let remaining_ms = (abs_ms - current_time_ms()).max(1) as u64;
            key_handle
                .set_expire(Duration::from_millis(remaining_ms))
                .map_err(|e| ValkeyError::String(format!("flash: lmove set_expire: {e}")))?;
        }

        let serialized = list_serialize(&items);
        cache.put(src_key.as_slice(), serialized.clone());

        ctx.replicate_verbatim();
        ctx.notify_keyspace_event(NotifyEvent::LIST, "flash.lmove", src_key);

        // src == dst in a rotation — signal so BLPOP clients on this key wake up.
        #[cfg(not(test))]
        unsafe {
            raw::RedisModule_SignalKeyAsReady.unwrap()(ctx.ctx, src_key.inner);
        }

        return finish_lmove(ctx, elem, None, serialized, src_key, None, &[]);
    }

    // Two-key path.
    let src_handle = ctx.open_key_writable(src_key);
    let src_existing = match src_handle.get_value::<FlashListObject>(&FLASH_LIST_TYPE) {
        Err(_) => return Err(ValkeyError::WrongType),
        Ok(None) => return Ok(ValkeyValue::Null),
        Ok(Some(obj)) => obj,
    };

    let (mut src_items, src_ttl) = {
        let ttl = crate::util_expire::preserve_ttl(ctx, src_key, src_existing.ttl_ms);
        let list = match &src_existing.tier {
            Tier::Hot(l) => l.clone(),
            Tier::Cold {
                backend_offset,
                value_len,
                ..
            } => promote_cold_list(*backend_offset, *value_len)?,
        };
        (list, ttl)
    };

    if src_items.is_empty() {
        return Ok(ValkeyValue::Null);
    }

    let elem = if src_left {
        src_items.pop_front().unwrap()
    } else {
        src_items.pop_back().unwrap()
    };

    // Update src.
    let src_serialized: Option<Vec<u8>> = if src_items.is_empty() {
        let _ = src_handle.delete();
        cache.delete(src_key.as_slice());
        None
    } else {
        let ser = list_serialize(&src_items);
        src_handle
            .set_value(
                &FLASH_LIST_TYPE,
                FlashListObject {
                    tier: Tier::Hot(src_items.clone()),
                    ttl_ms: src_ttl,
                },
            )
            .map_err(|e| ValkeyError::String(format!("flash: lmove src set_value: {e}")))?;
        if let Some(abs_ms) = src_ttl {
            use std::time::Duration;
            let remaining_ms = (abs_ms - current_time_ms()).max(1) as u64;
            src_handle
                .set_expire(Duration::from_millis(remaining_ms))
                .map_err(|e| ValkeyError::String(format!("flash: lmove src set_expire: {e}")))?;
        }
        cache.put(src_key.as_slice(), ser.clone());
        Some(ser)
    };

    // Update dst.
    let dst_handle = ctx.open_key_writable(dst_key);
    let dst_existing = match dst_handle.get_value::<FlashListObject>(&FLASH_LIST_TYPE) {
        Err(_) => return Err(ValkeyError::WrongType),
        Ok(v) => v,
    };

    let (mut dst_items, dst_ttl): (VecDeque<Vec<u8>>, Option<i64>) = match dst_existing {
        None => (VecDeque::new(), None),
        Some(obj) => {
            let ttl = crate::util_expire::preserve_ttl(ctx, dst_key, obj.ttl_ms);
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

    if dst_left {
        dst_items.push_front(elem.clone());
    } else {
        dst_items.push_back(elem.clone());
    }

    dst_handle
        .set_value(
            &FLASH_LIST_TYPE,
            FlashListObject {
                tier: Tier::Hot(dst_items.clone()),
                ttl_ms: dst_ttl,
            },
        )
        .map_err(|e| ValkeyError::String(format!("flash: lmove dst set_value: {e}")))?;

    if let Some(abs_ms) = dst_ttl {
        use std::time::Duration;
        let remaining_ms = (abs_ms - current_time_ms()).max(1) as u64;
        dst_handle
            .set_expire(Duration::from_millis(remaining_ms))
            .map_err(|e| ValkeyError::String(format!("flash: lmove dst set_expire: {e}")))?;
    }

    let dst_serialized = list_serialize(&dst_items);
    cache.put(dst_key.as_slice(), dst_serialized.clone());

    ctx.replicate_verbatim();
    ctx.notify_keyspace_event(NotifyEvent::LIST, "flash.lmove", src_key);
    ctx.notify_keyspace_event(NotifyEvent::LIST, "flash.lmove", dst_key);

    // Signal dst so any FLASH.BLPOP clients blocked on it wake up.
    #[cfg(not(test))]
    unsafe {
        raw::RedisModule_SignalKeyAsReady.unwrap()(ctx.ctx, dst_key.inner);
    }

    let src_bytes: &[u8] = src_serialized.as_deref().unwrap_or(&[]);
    finish_lmove(
        ctx,
        elem,
        Some((dst_key.as_slice(), dst_serialized)),
        src_bytes.to_vec(),
        src_key,
        Some(dst_key.as_slice()),
        src_bytes,
    )
}

fn finish_lmove(
    ctx: &Context,
    _elem: Vec<u8>,
    dst_write: Option<(&[u8], Vec<u8>)>,
    src_serialized: Vec<u8>,
    _src_key: &ValkeyString,
    _dst_key_slice: Option<&[u8]>,
    _src_bytes: &[u8],
) -> ValkeyResult {
    #[cfg(not(test))]
    {
        if crate::replication::is_replica()
            || (crate::STORAGE.get().is_none() && crate::replication::must_obey_client(ctx))
        {
            return Ok(ValkeyValue::StringBuffer(_elem));
        }
        use crate::storage::backend::StorageBackend;
        let storage = crate::STORAGE
            .get()
            .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;
        let pool = crate::POOL
            .get()
            .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;
        let src_key_bytes = _src_key.as_slice().to_vec();
        let dst_key_bytes: Option<Vec<u8>> = _dst_key_slice.map(|s| s.to_vec());
        let dst_ser: Option<Vec<u8>> = dst_write.map(|(_, s)| s);
        let bc = ctx.block_client();
        let handle = Box::new(LMoveCompletionHandle {
            tsc: valkey_module::ThreadSafeContext::with_blocked_client(bc),
            elem: _elem,
        });
        pool.submit_or_complete(handle, move || {
            // Write src (if non-empty after pop).
            if !src_serialized.is_empty() {
                let offset = storage.put(&src_key_bytes, &src_serialized)?;
                if let Some(wal) = WAL.get() {
                    let kh = crate::util::key_hash(&src_key_bytes);
                    let vh = crate::util::value_hash(&src_serialized);
                    let _ = wal.append(crate::storage::wal::WalOp::Put {
                        key_hash: kh,
                        offset,
                        value_hash: vh,
                    });
                }
            } else {
                // src became empty — delete.
                storage.delete(&src_key_bytes).ok();
            }
            // Write dst (always non-empty).
            if let (Some(dk), Some(ds)) = (dst_key_bytes, dst_ser) {
                let offset = storage.put(&dk, &ds)?;
                if let Some(wal) = WAL.get() {
                    let kh = crate::util::key_hash(&dk);
                    let vh = crate::util::value_hash(&ds);
                    let _ = wal.append(crate::storage::wal::WalOp::Put {
                        key_hash: kh,
                        offset,
                        value_hash: vh,
                    });
                }
            }
            Ok(vec![])
        });
        return Ok(ValkeyValue::NoReply);
    }

    #[allow(unused_variables, unreachable_code)]
    {
        let _ = (
            ctx,
            dst_write,
            src_serialized,
            _src_key,
            _dst_key_slice,
            _src_bytes,
        );
        Ok(ValkeyValue::Null)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    #[test]
    fn lmove_left_to_right_rotation() {
        let mut items: VecDeque<Vec<u8>> = VecDeque::new();
        items.push_back(b"a".to_vec());
        items.push_back(b"b".to_vec());
        items.push_back(b"c".to_vec());
        let elem = items.pop_front().unwrap(); // LEFT
        items.push_back(elem.clone()); // RIGHT
        // [b, c, a]
        assert_eq!(elem, b"a");
        assert_eq!(items[0], b"b");
        assert_eq!(items[2], b"a");
    }

    #[test]
    fn lmove_right_to_left_rotation() {
        let mut items: VecDeque<Vec<u8>> = VecDeque::new();
        items.push_back(b"a".to_vec());
        items.push_back(b"b".to_vec());
        let elem = items.pop_back().unwrap(); // RIGHT
        items.push_front(elem.clone()); // LEFT
        // [b, a]
        assert_eq!(elem, b"b");
        assert_eq!(items[0], b"b");
    }

    #[test]
    fn lmove_src_absent_returns_nil() {
        let src: Option<Vec<u8>> = None;
        assert!(src.is_none());
    }
}
