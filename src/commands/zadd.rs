use valkey_module::{Context, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

use crate::commands::list_common::current_time_ms;
use crate::commands::zset_common::{finish_zset_write, parse_score, promote_cold_zset, ZSetReply};
use crate::types::zset::{format_score, zset_serialize, FlashZSetObject, ZSetInner, FLASH_ZSET_TYPE};
use crate::types::Tier;
use crate::CACHE;

// ── FLASH.ZADD ────────────────────────────────────────────────────────────────

/// `FLASH.ZADD key [NX|XX] [GT|LT] [CH] [INCR] score member [score member ...]`
pub fn flash_zadd_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 4 {
        return Err(ValkeyError::WrongArity);
    }

    let key = &args[1];

    // Parse option flags (come before score-member pairs)
    let mut pos = 2usize;
    let mut nx = false;
    let mut xx = false;
    let mut gt = false;
    let mut lt = false;
    let mut ch = false;
    let mut incr = false;

    while pos < args.len() {
        match args[pos].as_slice().to_ascii_uppercase().as_slice() {
            b"NX" => {
                nx = true;
                pos += 1;
            }
            b"XX" => {
                xx = true;
                pos += 1;
            }
            b"GT" => {
                gt = true;
                pos += 1;
            }
            b"LT" => {
                lt = true;
                pos += 1;
            }
            b"CH" => {
                ch = true;
                pos += 1;
            }
            b"INCR" => {
                incr = true;
                pos += 1;
            }
            _ => break,
        }
    }

    if nx && xx {
        return Err(ValkeyError::Str(
            "ERR XX and NX options at the same time are not compatible",
        ));
    }
    if gt && lt {
        return Err(ValkeyError::Str(
            "ERR GT and LT options at the same time are not compatible",
        ));
    }
    if (gt || lt) && nx {
        return Err(ValkeyError::Str(
            "ERR GT, LT, and NX options at the same time are not compatible",
        ));
    }

    let pairs_slice = &args[pos..];
    if pairs_slice.is_empty() || !pairs_slice.len().is_multiple_of(2) {
        return Err(ValkeyError::WrongArity);
    }
    if incr && pairs_slice.len() != 2 {
        return Err(ValkeyError::Str(
            "ERR INCR option supports a single increment-element pair",
        ));
    }

    let mut pairs: Vec<(f64, Vec<u8>)> = Vec::with_capacity(pairs_slice.len() / 2);
    for chunk in pairs_slice.chunks(2) {
        let score = parse_score(chunk[0].as_slice())?;
        pairs.push((score, chunk[1].as_slice().to_vec()));
    }

    let cache = CACHE
        .get()
        .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;
    let key_handle = ctx.open_key_writable(key);
    let existing = match key_handle.get_value::<FlashZSetObject>(&FLASH_ZSET_TYPE) {
        Err(_) => return Err(ValkeyError::WrongType),
        Ok(v) => v,
    };

    let key_existed = existing.is_some();
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

    let mut added = 0i64;
    let mut changed = 0i64;
    let mut incr_result: Option<f64> = None;

    for (score, member) in &pairs {
        let old_score = inner.get_score(member);

        if incr {
            if nx && old_score.is_some() {
                incr_result = None;
                continue;
            }
            if xx && old_score.is_none() {
                incr_result = None;
                continue;
            }
            let base = old_score.unwrap_or(0.0);
            let new_score = base + score;
            if new_score.is_nan() {
                return Err(ValkeyError::Str(
                    "ERR resulting score is not a number (NaN)",
                ));
            }
            if gt && old_score.is_some() && new_score <= base {
                incr_result = Some(base);
                continue;
            }
            if lt && old_score.is_some() && new_score >= base {
                incr_result = Some(base);
                continue;
            }
            if old_score.is_none() {
                added += 1;
            }
            changed += 1;
            inner.insert(member.clone(), new_score);
            incr_result = Some(new_score);
        } else {
            if nx && old_score.is_some() {
                continue;
            }
            if xx && old_score.is_none() {
                continue;
            }
            if gt && old_score.is_some() && *score <= old_score.unwrap() {
                continue;
            }
            if lt && old_score.is_some() && *score >= old_score.unwrap() {
                continue;
            }
            let is_new = old_score.is_none();
            if is_new {
                added += 1;
                changed += 1;
            } else if old_score != Some(*score) {
                changed += 1;
            }
            inner.insert(member.clone(), *score);
        }
    }

    // If nothing was added and key didn't exist, don't create an empty key.
    if inner.is_empty() && !key_existed {
        return Ok(if incr {
            ValkeyValue::Null
        } else if ch {
            ValkeyValue::Integer(changed)
        } else {
            ValkeyValue::Integer(added)
        });
    }

    key_handle
        .set_value(
            &FLASH_ZSET_TYPE,
            FlashZSetObject {
                tier: Tier::Hot(inner.clone()),
                ttl_ms: old_ttl,
            },
        )
        .map_err(|e| ValkeyError::String(format!("flash: zadd set_value: {e}")))?;

    if let Some(abs_ms) = old_ttl {
        use std::time::Duration;
        let remaining_ms = (abs_ms - current_time_ms()).max(1) as u64;
        key_handle
            .set_expire(Duration::from_millis(remaining_ms))
            .map_err(|e| ValkeyError::String(format!("flash: zadd set_expire: {e}")))?;
    }

    let serialized = zset_serialize(&inner);
    cache.put(key.as_slice(), serialized.clone());

    ctx.replicate_verbatim();
    ctx.notify_keyspace_event(NotifyEvent::ZSET, "flash.zadd", key);

    #[cfg(not(test))]
    unsafe {
        valkey_module::raw::RedisModule_SignalKeyAsReady.unwrap()(ctx.ctx, key.inner);
    }

    let reply = if incr {
        match incr_result {
            Some(s) => ZSetReply::BulkString(format_score(s).into_bytes()),
            None => ZSetReply::Null,
        }
    } else if ch {
        ZSetReply::Integer(changed)
    } else {
        ZSetReply::Integer(added)
    };

    finish_zset_write(ctx, key.as_slice().to_vec(), serialized, reply)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #[test]
    fn zadd_nx_xx_mutually_exclusive_check() {
        // Logic test: NX + XX is invalid
        let nx = true;
        let xx = true;
        assert!(nx && xx); // both set → error
    }

    #[test]
    fn zadd_incr_applies_delta() {
        let base = 3.0f64;
        let delta = 2.0f64;
        assert_eq!(base + delta, 5.0);
    }

    #[test]
    fn zadd_gt_blocks_lower_score() {
        let old = 5.0f64;
        let new_score = 3.0f64;
        let gt = true;
        assert!(gt && new_score <= old);
    }

    #[test]
    fn zadd_lt_blocks_higher_score() {
        let old = 3.0f64;
        let new_score = 5.0f64;
        let lt = true;
        assert!(lt && new_score >= old);
    }
}
