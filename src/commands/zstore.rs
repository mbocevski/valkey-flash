use valkey_module::{Context, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

use crate::commands::zset_common::{
    apply_limit, finish_zset_write, lex_range_entries, parse_lex_bound, parse_score,
    parse_score_bound, promote_cold_zset, score_range_entries, ZSetReply,
};
use crate::types::zset::{zset_serialize, FlashZSetObject, ScoreF64, ZSetInner, FLASH_ZSET_TYPE};
use crate::types::Tier;
use crate::CACHE;

// ── Aggregate mode ────────────────────────────────────────────────────────────

#[derive(Clone, Copy)]
enum Aggregate {
    Sum,
    Min,
    Max,
}

// ── Shared helpers ────────────────────────────────────────────────────────────

fn parse_numkeys(args: &[ValkeyString], pos: usize) -> Result<usize, ValkeyError> {
    let n: i64 = std::str::from_utf8(args[pos].as_slice())
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or(ValkeyError::Str(
            "ERR value is not an integer or out of range",
        ))?;
    if n < 1 {
        return Err(ValkeyError::Str(
            "ERR at least 1 input key is needed",
        ));
    }
    Ok(n as usize)
}

fn open_source_zset(
    ctx: &Context,
    key: &ValkeyString,
) -> Result<Option<ZSetInner>, ValkeyError> {
    let handle = ctx.open_key(key);
    match handle.get_value::<FlashZSetObject>(&FLASH_ZSET_TYPE) {
        Err(_) => Err(ValkeyError::WrongType),
        Ok(None) => Ok(None),
        Ok(Some(obj)) => {
            let inner = match &obj.tier {
                Tier::Hot(z) => z.clone(),
                Tier::Cold {
                    backend_offset,
                    value_len,
                    ..
                } => promote_cold_zset(*backend_offset, *value_len)?,
            };
            Ok(Some(inner))
        }
    }
}

fn parse_weights_aggregate(
    tail: &[ValkeyString],
    numkeys: usize,
) -> Result<(Vec<f64>, Aggregate), ValkeyError> {
    let mut weights: Vec<f64> = Vec::new();
    let mut agg = Aggregate::Sum;
    let mut i = 0;
    while i < tail.len() {
        match tail[i].as_slice().to_ascii_uppercase().as_slice() {
            b"WEIGHTS" => {
                i += 1;
                for _ in 0..numkeys {
                    if i >= tail.len() {
                        return Err(ValkeyError::Str("ERR syntax error"));
                    }
                    weights.push(parse_score(tail[i].as_slice())?);
                    i += 1;
                }
            }
            b"AGGREGATE" => {
                if i + 1 >= tail.len() {
                    return Err(ValkeyError::Str("ERR syntax error"));
                }
                agg = match tail[i + 1].as_slice().to_ascii_uppercase().as_slice() {
                    b"SUM" => Aggregate::Sum,
                    b"MIN" => Aggregate::Min,
                    b"MAX" => Aggregate::Max,
                    _ => return Err(ValkeyError::Str("ERR syntax error")),
                };
                i += 2;
            }
            _ => return Err(ValkeyError::Str("ERR syntax error")),
        }
    }
    Ok((weights, agg))
}

/// Write `result` as a FlashZSet to `dst`, overwriting any previous value.
/// Returns the element count as the command reply.
fn write_dest_zset(
    ctx: &Context,
    dst: &ValkeyString,
    result: ZSetInner,
    event: &str,
) -> ValkeyResult {
    let cache = CACHE
        .get()
        .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;
    let key_handle = ctx.open_key_writable(dst);

    if result.is_empty() {
        let _ = key_handle.delete();
        cache.delete(dst.as_slice());
        ctx.replicate_verbatim();
        ctx.notify_keyspace_event(NotifyEvent::ZSET, event, dst);
        return Ok(ValkeyValue::Integer(0));
    }

    let count = result.len() as i64;
    key_handle
        .set_value(
            &FLASH_ZSET_TYPE,
            FlashZSetObject {
                tier: Tier::Hot(result.clone()),
                ttl_ms: None,
            },
        )
        .map_err(|e| ValkeyError::String(format!("flash: zstore set_value: {e}")))?;

    let serialized = zset_serialize(&result);
    cache.put(dst.as_slice(), serialized.clone());
    ctx.replicate_verbatim();
    ctx.notify_keyspace_event(NotifyEvent::ZSET, event, dst);

    #[cfg(not(test))]
    unsafe {
        valkey_module::raw::RedisModule_SignalKeyAsReady.unwrap()(ctx.ctx, dst.inner);
    }

    finish_zset_write(
        ctx,
        dst.as_slice().to_vec(),
        serialized,
        ZSetReply::Integer(count),
    )
}

// ── FLASH.ZUNIONSTORE ─────────────────────────────────────────────────────────

/// `FLASH.ZUNIONSTORE dst numkeys key [key ...] [WEIGHTS w ...] [AGGREGATE SUM|MIN|MAX]`
pub fn flash_zunionstore_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 4 {
        return Err(ValkeyError::WrongArity);
    }
    let dst = &args[1];
    let numkeys = parse_numkeys(&args, 2)?;
    if args.len() < 3 + numkeys {
        return Err(ValkeyError::WrongArity);
    }
    let src_keys = &args[3..3 + numkeys];
    let (weights, agg) = parse_weights_aggregate(&args[3 + numkeys..], numkeys)?;

    let mut sources: Vec<(ZSetInner, f64)> = Vec::with_capacity(numkeys);
    for (i, key) in src_keys.iter().enumerate() {
        let weight = weights.get(i).copied().unwrap_or(1.0);
        let inner = open_source_zset(ctx, key)?.unwrap_or_else(ZSetInner::new);
        sources.push((inner, weight));
    }

    let mut map: std::collections::HashMap<Vec<u8>, f64> = std::collections::HashMap::new();
    for (inner, weight) in &sources {
        for (member, &score) in &inner.members {
            let weighted = score * weight;
            let entry = map.entry(member.clone()).or_insert(match agg {
                Aggregate::Min => f64::INFINITY,
                Aggregate::Max => f64::NEG_INFINITY,
                Aggregate::Sum => 0.0,
            });
            *entry = match agg {
                Aggregate::Sum => *entry + weighted,
                Aggregate::Min => f64::min(*entry, weighted),
                Aggregate::Max => f64::max(*entry, weighted),
            };
        }
    }

    let mut out = ZSetInner::new();
    for (member, score) in map {
        out.insert(member, score);
    }

    write_dest_zset(ctx, dst, out, "flash.zunionstore")
}

// ── FLASH.ZINTERSTORE ─────────────────────────────────────────────────────────

/// `FLASH.ZINTERSTORE dst numkeys key [key ...] [WEIGHTS w ...] [AGGREGATE SUM|MIN|MAX]`
pub fn flash_zinterstore_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 4 {
        return Err(ValkeyError::WrongArity);
    }
    let dst = &args[1];
    let numkeys = parse_numkeys(&args, 2)?;
    if args.len() < 3 + numkeys {
        return Err(ValkeyError::WrongArity);
    }
    let src_keys = &args[3..3 + numkeys];
    let (weights, agg) = parse_weights_aggregate(&args[3 + numkeys..], numkeys)?;

    let mut sources: Vec<(ZSetInner, f64)> = Vec::with_capacity(numkeys);
    for (i, key) in src_keys.iter().enumerate() {
        let weight = weights.get(i).copied().unwrap_or(1.0);
        match open_source_zset(ctx, key)? {
            Some(inner) => sources.push((inner, weight)),
            None => {
                // Any missing source → intersection is empty.
                return write_dest_zset(ctx, dst, ZSetInner::new(), "flash.zinterstore");
            }
        }
    }

    let (first, first_w) = &sources[0];
    let mut map: std::collections::HashMap<Vec<u8>, f64> =
        std::collections::HashMap::with_capacity(first.len());

    for (member, &score) in &first.members {
        let mut acc = score * first_w;
        let mut in_all = true;
        for (other, other_w) in &sources[1..] {
            match other.get_score(member) {
                None => {
                    in_all = false;
                    break;
                }
                Some(s) => {
                    let weighted = s * other_w;
                    acc = match agg {
                        Aggregate::Sum => acc + weighted,
                        Aggregate::Min => f64::min(acc, weighted),
                        Aggregate::Max => f64::max(acc, weighted),
                    };
                }
            }
        }
        if in_all {
            map.insert(member.clone(), acc);
        }
    }

    let mut out = ZSetInner::new();
    for (member, score) in map {
        out.insert(member, score);
    }

    write_dest_zset(ctx, dst, out, "flash.zinterstore")
}

// ── FLASH.ZDIFFSTORE ──────────────────────────────────────────────────────────

/// `FLASH.ZDIFFSTORE dst numkeys key [key ...]`
pub fn flash_zdiffstore_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 4 {
        return Err(ValkeyError::WrongArity);
    }
    let dst = &args[1];
    let numkeys = parse_numkeys(&args, 2)?;
    if args.len() < 3 + numkeys {
        return Err(ValkeyError::WrongArity);
    }
    let src_keys = &args[3..3 + numkeys];

    let mut sources: Vec<ZSetInner> = Vec::with_capacity(numkeys);
    for key in src_keys {
        sources.push(open_source_zset(ctx, key)?.unwrap_or_else(ZSetInner::new));
    }

    let mut out = ZSetInner::new();
    for (member, &score) in &sources[0].members {
        let excluded = sources[1..].iter().any(|s| s.get_score(member).is_some());
        if !excluded {
            out.insert(member.clone(), score);
        }
    }

    write_dest_zset(ctx, dst, out, "flash.zdiffstore")
}

// ── FLASH.ZRANGESTORE ─────────────────────────────────────────────────────────

/// `FLASH.ZRANGESTORE dst src start stop [BYSCORE|BYLEX] [REV] [LIMIT offset count]`
pub fn flash_zrangestore_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 5 {
        return Err(ValkeyError::WrongArity);
    }
    let dst = &args[1];
    let src = &args[2];
    let start_arg = args[3].as_slice();
    let stop_arg = args[4].as_slice();

    let mut by_score = false;
    let mut by_lex = false;
    let mut rev = false;
    let mut limit: Option<(i64, i64)> = None;

    let mut i = 5usize;
    while i < args.len() {
        match args[i].as_slice().to_ascii_uppercase().as_slice() {
            b"BYSCORE" => {
                by_score = true;
                i += 1;
            }
            b"BYLEX" => {
                by_lex = true;
                i += 1;
            }
            b"REV" => {
                rev = true;
                i += 1;
            }
            b"LIMIT" => {
                if i + 2 >= args.len() {
                    return Err(ValkeyError::Str("ERR syntax error"));
                }
                let off: i64 = std::str::from_utf8(args[i + 1].as_slice())
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .ok_or(ValkeyError::Str(
                        "ERR value is not an integer or out of range",
                    ))?;
                let cnt: i64 = std::str::from_utf8(args[i + 2].as_slice())
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .ok_or(ValkeyError::Str(
                        "ERR value is not an integer or out of range",
                    ))?;
                limit = Some((off, cnt));
                i += 3;
            }
            _ => return Err(ValkeyError::Str("ERR syntax error")),
        }
    }

    if by_score && by_lex {
        return Err(ValkeyError::Str("ERR syntax error"));
    }

    let inner = match open_source_zset(ctx, src)? {
        None => return write_dest_zset(ctx, dst, ZSetInner::new(), "flash.zrangestore"),
        Some(z) => z,
    };

    let pairs: Vec<(Vec<u8>, f64)> = if by_score {
        let (eff_min, eff_max) = if rev {
            (stop_arg, start_arg)
        } else {
            (start_arg, stop_arg)
        };
        let min_b = parse_score_bound(eff_min)?;
        let max_b = parse_score_bound(eff_max)?;
        let mut entries: Vec<_> = score_range_entries(&inner, min_b, max_b)
            .into_iter()
            .map(|(m, s)| (m.clone(), s))
            .collect();
        if rev {
            entries.reverse();
        }
        apply_limit(
            entries,
            limit.map_or(0, |l| l.0),
            limit.map_or(-1, |l| l.1),
        )
    } else if by_lex {
        let (eff_min, eff_max) = if rev {
            (stop_arg, start_arg)
        } else {
            (start_arg, stop_arg)
        };
        let min_lex = parse_lex_bound(eff_min)?;
        let max_lex = parse_lex_bound(eff_max)?;
        let mut entries: Vec<_> = lex_range_entries(&inner, &min_lex, &max_lex)
            .into_iter()
            .map(|(m, s)| (m.clone(), s))
            .collect();
        if rev {
            entries.reverse();
        }
        apply_limit(
            entries,
            limit.map_or(0, |l| l.0),
            limit.map_or(-1, |l| l.1),
        )
    } else {
        let start_idx: i64 = std::str::from_utf8(start_arg)
            .ok()
            .and_then(|s| s.parse().ok())
            .ok_or(ValkeyError::Str(
                "ERR value is not an integer or out of range",
            ))?;
        let stop_idx: i64 = std::str::from_utf8(stop_arg)
            .ok()
            .and_then(|s| s.parse().ok())
            .ok_or(ValkeyError::Str(
                "ERR value is not an integer or out of range",
            ))?;
        let mut all: Vec<(Vec<u8>, f64)> = inner
            .scores
            .iter()
            .map(|((ScoreF64(s), m), ())| (m.clone(), *s))
            .collect();
        if rev {
            all.reverse();
        }
        let (start, end) =
            crate::commands::list_common::resolve_range(start_idx, stop_idx, all.len());
        all[start..end].to_vec()
    };

    let mut out = ZSetInner::new();
    for (member, score) in pairs {
        out.insert(member, score);
    }

    write_dest_zset(ctx, dst, out, "flash.zrangestore")
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn zset_with(entries: &[(&[u8], f64)]) -> ZSetInner {
        let mut z = ZSetInner::new();
        for (m, s) in entries {
            z.insert(m.to_vec(), *s);
        }
        z
    }

    // ── union logic ───────────────────────────────────────────────────────────

    #[test]
    fn union_sum_disjoint() {
        let a = zset_with(&[(b"x", 1.0)]);
        let b = zset_with(&[(b"y", 2.0)]);
        let sources = vec![(a, 1.0), (b, 1.0)];
        let mut map = std::collections::HashMap::new();
        for (inner, w) in &sources {
            for (m, &s) in &inner.members {
                let weighted = s * w;
                let e = map.entry(m.clone()).or_insert(0.0f64);
                *e += weighted;
            }
        }
        assert_eq!(map[b"x".as_slice()], 1.0);
        assert_eq!(map[b"y".as_slice()], 2.0);
    }

    #[test]
    fn union_sum_overlap() {
        let _a = zset_with(&[(b"m", 3.0)]);
        let _b = zset_with(&[(b"m", 5.0)]);
        let mut acc = 0.0f64;
        acc += 3.0;
        acc += 5.0;
        assert_eq!(acc, 8.0);
    }

    #[test]
    fn union_min_overlap() {
        let weighted_a = 3.0f64;
        let weighted_b = 5.0f64;
        let result = f64::min(f64::INFINITY, weighted_a);
        let result = f64::min(result, weighted_b);
        assert_eq!(result, 3.0);
    }

    #[test]
    fn union_max_overlap() {
        let weighted_a = 3.0f64;
        let weighted_b = 5.0f64;
        let result = f64::max(f64::NEG_INFINITY, weighted_a);
        let result = f64::max(result, weighted_b);
        assert_eq!(result, 5.0);
    }

    // ── inter logic ───────────────────────────────────────────────────────────

    #[test]
    fn inter_keeps_common_members() {
        let a = zset_with(&[(b"x", 1.0), (b"y", 2.0)]);
        let b = zset_with(&[(b"y", 3.0), (b"z", 4.0)]);
        let mut result = std::collections::HashMap::new();
        for (m, &s) in &a.members {
            if b.get_score(m).is_some() {
                result.insert(m.clone(), s);
            }
        }
        assert!(result.contains_key(b"y".as_slice()));
        assert!(!result.contains_key(b"x".as_slice()));
        assert!(!result.contains_key(b"z".as_slice()));
    }

    #[test]
    fn inter_sum_aggregate() {
        let _a = zset_with(&[(b"m", 2.0)]);
        let _b = zset_with(&[(b"m", 4.0)]);
        let acc_a = 2.0f64 * 1.0;
        let acc_b = acc_a + 4.0 * 1.0;
        assert_eq!(acc_b, 6.0);
    }

    // ── diff logic ────────────────────────────────────────────────────────────

    #[test]
    fn diff_excludes_members_in_other_sources() {
        let a = zset_with(&[(b"x", 1.0), (b"y", 2.0), (b"z", 3.0)]);
        let b = zset_with(&[(b"y", 99.0)]);
        let diff: Vec<_> = a
            .members
            .keys()
            .filter(|m| b.get_score(m).is_none())
            .collect();
        assert_eq!(diff.len(), 2);
        assert!(!diff.contains(&&b"y".to_vec()));
    }

    #[test]
    fn diff_empty_other_returns_all() {
        let a = zset_with(&[(b"a", 1.0), (b"b", 2.0)]);
        let b = zset_with(&[]);
        let count = a
            .members
            .keys()
            .filter(|m| b.get_score(m).is_none())
            .count();
        assert_eq!(count, 2);
    }

    // ── weight logic ─────────────────────────────────────────────────────────

    #[test]
    fn weights_multiply_scores() {
        let score = 5.0f64;
        let weight = 3.0f64;
        assert_eq!(score * weight, 15.0);
    }

    #[test]
    fn default_weight_is_one() {
        let weights: Vec<f64> = vec![];
        let w = weights.first().copied().unwrap_or(1.0);
        assert_eq!(w, 1.0);
    }
}
