use valkey_module::{Context, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

use crate::commands::list_common::current_time_ms;
use crate::commands::zset_common::{finish_zset_write, parse_score, promote_cold_zset, ZSetReply};
use crate::types::zset::{format_score, zset_serialize, FlashZSetObject, ZSetInner, FLASH_ZSET_TYPE};
use crate::types::Tier;
use crate::CACHE;

// ── helpers ───────────────────────────────────────────────────────────────────

/// Open a ZSet key and promote Cold tier if needed.
/// Returns (inner, old_ttl, key_handle).
#[allow(clippy::type_complexity)]
fn open_zset_writable(
    ctx: &Context,
    key: &ValkeyString,
) -> Result<
    (
        Option<(ZSetInner, Option<i64>)>,
        valkey_module::key::ValkeyKeyWritable,
    ),
    ValkeyError,
> {
    let key_handle = ctx.open_key_writable(key);
    let existing = match key_handle.get_value::<FlashZSetObject>(&FLASH_ZSET_TYPE) {
        Err(_) => return Err(ValkeyError::WrongType),
        Ok(v) => v,
    };
    let data = match existing {
        None => None,
        Some(obj) => {
            let ttl = obj.ttl_ms;
            let inner = match &obj.tier {
                Tier::Hot(z) => z.clone(),
                Tier::Cold {
                    backend_offset,
                    value_len,
                    ..
                } => promote_cold_zset(*backend_offset, *value_len)?,
            };
            Some((inner, ttl))
        }
    };
    Ok((data, key_handle))
}

/// Commit a modified ZSet back to Valkey + cache, or delete if empty.
/// Returns the serialized bytes (empty = key deleted).
fn commit_zset(
    _ctx: &Context,
    key: &ValkeyString,
    key_handle: valkey_module::key::ValkeyKeyWritable,
    inner: ZSetInner,
    old_ttl: Option<i64>,
    cache: &crate::storage::cache::FlashCache,
) -> Result<Vec<u8>, ValkeyError> {
    if inner.is_empty() {
        let _ = key_handle.delete();
        cache.delete(key.as_slice());
        return Ok(vec![]);
    }
    key_handle
        .set_value(
            &FLASH_ZSET_TYPE,
            FlashZSetObject {
                tier: Tier::Hot(inner.clone()),
                ttl_ms: old_ttl,
            },
        )
        .map_err(|e| ValkeyError::String(format!("flash: zset commit set_value: {e}")))?;
    if let Some(abs_ms) = old_ttl {
        use std::time::Duration;
        let remaining_ms = (abs_ms - current_time_ms()).max(1) as u64;
        key_handle
            .set_expire(Duration::from_millis(remaining_ms))
            .map_err(|e| ValkeyError::String(format!("flash: zset commit set_expire: {e}")))?;
    }
    let serialized = zset_serialize(&inner);
    cache.put(key.as_slice(), serialized.clone());
    Ok(serialized)
}

// ── FLASH.ZPOPMIN / FLASH.ZPOPMAX ─────────────────────────────────────────────

fn zpop_impl(ctx: &Context, args: Vec<ValkeyString>, from_min: bool) -> ValkeyResult {
    let cmd = if from_min { "FLASH.ZPOPMIN" } else { "FLASH.ZPOPMAX" };
    if args.len() < 2 || args.len() > 3 {
        return Err(ValkeyError::WrongArity);
    }
    let key = &args[1];
    let count: Option<usize> = if args.len() == 3 {
        let n: i64 = std::str::from_utf8(args[2].as_slice())
            .ok()
            .and_then(|s| s.parse().ok())
            .ok_or(ValkeyError::Str("ERR value is not an integer or out of range"))?;
        if n < 0 {
            return Err(ValkeyError::Str("ERR value is out of range, must be positive"));
        }
        Some(n as usize)
    } else {
        None
    };

    let cache = CACHE
        .get()
        .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;
    let (data, key_handle) = open_zset_writable(ctx, key)?;

    let (mut inner, old_ttl) = match data {
        None => {
            return Ok(ValkeyValue::Array(vec![]));
        }
        Some(d) => d,
    };

    if inner.is_empty() {
        return Ok(ValkeyValue::Array(vec![]));
    }

    let pop_count = count.unwrap_or(1).min(inner.len());
    let mut popped: Vec<(Vec<u8>, f64)> = Vec::with_capacity(pop_count);

    for _ in 0..pop_count {
        if inner.is_empty() {
            break;
        }
        if from_min {
            // Pop the entry with the lowest (score, member)
            if let Some(key_ref) = inner.scores.keys().next().cloned() {
                let score = key_ref.0 .0;
                let member = key_ref.1.clone();
                inner.remove(&member);
                popped.push((member, score));
            }
        } else {
            // Pop the entry with the highest (score, member)
            if let Some(key_ref) = inner.scores.keys().next_back().cloned() {
                let score = key_ref.0 .0;
                let member = key_ref.1.clone();
                inner.remove(&member);
                popped.push((member, score));
            }
        }
    }

    // Build flat [member, score, member, score, ...] pairs.
    let mut flat: Vec<Vec<u8>> = Vec::with_capacity(popped.len() * 2);
    for (member, score) in &popped {
        flat.push(member.clone());
        flat.push(format_score(*score).into_bytes());
    }
    let reply = ZSetReply::FlatPairs(flat);

    let serialized = commit_zset(ctx, key, key_handle, inner, old_ttl, cache)?;

    ctx.replicate_verbatim();
    let event = if from_min { "flash.zpopmin" } else { "flash.zpopmax" };
    ctx.notify_keyspace_event(NotifyEvent::ZSET, event, key);

    let _ = cmd;
    finish_zset_write(ctx, key.as_slice().to_vec(), serialized, reply)
}

pub fn flash_zpopmin_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    zpop_impl(ctx, args, true)
}

pub fn flash_zpopmax_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    zpop_impl(ctx, args, false)
}

// ── FLASH.ZREM ────────────────────────────────────────────────────────────────

/// `FLASH.ZREM key member [member ...]`
pub fn flash_zrem_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 3 {
        return Err(ValkeyError::WrongArity);
    }
    let key = &args[1];
    let cache = CACHE
        .get()
        .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;
    let (data, key_handle) = open_zset_writable(ctx, key)?;

    let (mut inner, old_ttl) = match data {
        None => return Ok(ValkeyValue::Integer(0)),
        Some(d) => d,
    };

    let mut removed = 0i64;
    for member_arg in &args[2..] {
        if inner.remove(member_arg.as_slice()).is_some() {
            removed += 1;
        }
    }

    if removed == 0 {
        return Ok(ValkeyValue::Integer(0));
    }

    let serialized = commit_zset(ctx, key, key_handle, inner, old_ttl, cache)?;
    ctx.replicate_verbatim();
    ctx.notify_keyspace_event(NotifyEvent::ZSET, "flash.zrem", key);

    finish_zset_write(
        ctx,
        key.as_slice().to_vec(),
        serialized,
        ZSetReply::Integer(removed),
    )
}

// ── FLASH.ZINCRBY ─────────────────────────────────────────────────────────────

/// `FLASH.ZINCRBY key increment member`
pub fn flash_zincrby_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() != 4 {
        return Err(ValkeyError::WrongArity);
    }
    let key = &args[1];
    let delta = parse_score(args[2].as_slice())?;
    let member = args[3].as_slice().to_vec();

    let cache = CACHE
        .get()
        .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;
    let key_handle = ctx.open_key_writable(key);
    let existing = match key_handle.get_value::<FlashZSetObject>(&FLASH_ZSET_TYPE) {
        Err(_) => return Err(ValkeyError::WrongType),
        Ok(v) => v,
    };

    let (mut inner, old_ttl): (ZSetInner, Option<i64>) = match existing {
        None => (ZSetInner::new(), None),
        Some(obj) => {
            let ttl = obj.ttl_ms;
            let inner = match &obj.tier {
                Tier::Hot(z) => z.clone(),
                Tier::Cold {
                    backend_offset,
                    value_len,
                    ..
                } => promote_cold_zset(*backend_offset, *value_len)?,
            };
            (inner, ttl)
        }
    };

    let old_score = inner.get_score(&member).unwrap_or(0.0);
    let new_score = old_score + delta;
    if new_score.is_nan() {
        return Err(ValkeyError::Str(
            "ERR resulting score is not a number (NaN)",
        ));
    }
    inner.insert(member.clone(), new_score);

    key_handle
        .set_value(
            &FLASH_ZSET_TYPE,
            FlashZSetObject {
                tier: Tier::Hot(inner.clone()),
                ttl_ms: old_ttl,
            },
        )
        .map_err(|e| ValkeyError::String(format!("flash: zincrby set_value: {e}")))?;

    if let Some(abs_ms) = old_ttl {
        use std::time::Duration;
        let remaining_ms = (abs_ms - current_time_ms()).max(1) as u64;
        key_handle
            .set_expire(Duration::from_millis(remaining_ms))
            .map_err(|e| ValkeyError::String(format!("flash: zincrby set_expire: {e}")))?;
    }

    let serialized = zset_serialize(&inner);
    cache.put(key.as_slice(), serialized.clone());
    ctx.replicate_verbatim();
    ctx.notify_keyspace_event(NotifyEvent::ZSET, "flash.zincrby", key);

    #[cfg(not(test))]
    unsafe {
        valkey_module::raw::RedisModule_SignalKeyAsReady.unwrap()(ctx.ctx, key.inner);
    }

    let reply = ZSetReply::BulkString(format_score(new_score).into_bytes());
    finish_zset_write(ctx, key.as_slice().to_vec(), serialized, reply)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zpopmin_order() {
        // Verify we'd pop smallest score first
        let mut z = ZSetInner::new();
        z.insert(b"b".to_vec(), 2.0);
        z.insert(b"a".to_vec(), 1.0);
        let first = z.scores.keys().next().cloned().unwrap();
        assert_eq!(first.0 .0, 1.0);
        assert_eq!(first.1, b"a");
    }

    #[test]
    fn zpopmax_order() {
        let mut z = ZSetInner::new();
        z.insert(b"b".to_vec(), 2.0);
        z.insert(b"a".to_vec(), 1.0);
        let last = z.scores.keys().next_back().cloned().unwrap();
        assert_eq!(last.0 .0, 2.0);
        assert_eq!(last.1, b"b");
    }

    #[test]
    fn zincrby_delta() {
        let base = 3.0f64;
        let delta = -1.0f64;
        assert_eq!(base + delta, 2.0);
    }

    #[test]
    fn zrem_count() {
        let mut z = ZSetInner::new();
        z.insert(b"a".to_vec(), 1.0);
        z.insert(b"b".to_vec(), 2.0);
        let r1 = z.remove(b"a");
        let r2 = z.remove(b"missing");
        assert!(r1.is_some());
        assert!(r2.is_none());
    }
}
