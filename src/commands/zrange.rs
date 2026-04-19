use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

use crate::commands::list_common::resolve_range;
use crate::commands::zset_common::{
    apply_limit, build_range_reply, lex_range_entries, parse_lex_bound, parse_score_bound,
    promote_cold_zset, score_range_entries,
};
use crate::types::Tier;
use crate::types::zset::{FLASH_ZSET_TYPE, FlashZSetObject, ScoreF64, ZSetInner};

// ── helpers ───────────────────────────────────────────────────────────────────

#[allow(dead_code)]
fn parse_limit(args: &[ValkeyString], pos: &mut usize) -> Result<Option<(i64, i64)>, ValkeyError> {
    if *pos + 2 >= args.len() {
        return Err(ValkeyError::Str("ERR syntax error"));
    }
    let offset: i64 = std::str::from_utf8(args[*pos + 1].as_slice())
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or(ValkeyError::Str(
            "ERR value is not an integer or out of range",
        ))?;
    let count: i64 = std::str::from_utf8(args[*pos + 2].as_slice())
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or(ValkeyError::Str(
            "ERR value is not an integer or out of range",
        ))?;
    *pos += 3;
    Ok(Some((offset, count)))
}

fn open_zset_read(ctx: &Context, key: &ValkeyString) -> Result<Option<ZSetInner>, ValkeyError> {
    let key_handle = ctx.open_key(key);
    match key_handle.get_value::<FlashZSetObject>(&FLASH_ZSET_TYPE) {
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

// ── FLASH.ZRANGE ──────────────────────────────────────────────────────────────

/// Unified `FLASH.ZRANGE key min max [BYSCORE | BYLEX] [REV] [LIMIT offset count] [WITHSCORES]`
pub fn flash_zrange_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 4 {
        return Err(ValkeyError::WrongArity);
    }
    let key = &args[1];
    let min_arg = args[2].as_slice();
    let max_arg = args[3].as_slice();

    let mut by_score = false;
    let mut by_lex = false;
    let mut rev = false;
    let mut with_scores = false;
    let mut limit: Option<(i64, i64)> = None;

    let mut i = 4usize;
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
            b"WITHSCORES" => {
                with_scores = true;
                i += 1;
            }
            b"LIMIT" => {
                if i + 2 >= args.len() {
                    return Err(ValkeyError::Str("ERR syntax error"));
                }
                let offset: i64 = std::str::from_utf8(args[i + 1].as_slice())
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .ok_or(ValkeyError::Str(
                        "ERR value is not an integer or out of range",
                    ))?;
                let count: i64 = std::str::from_utf8(args[i + 2].as_slice())
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .ok_or(ValkeyError::Str(
                        "ERR value is not an integer or out of range",
                    ))?;
                limit = Some((offset, count));
                i += 3;
            }
            _ => return Err(ValkeyError::Str("ERR syntax error")),
        }
    }

    if by_score && by_lex {
        return Err(ValkeyError::Str("ERR syntax error"));
    }
    if limit.is_some() && !by_score && !by_lex {
        return Err(ValkeyError::Str(
            "ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX",
        ));
    }
    if with_scores && by_lex {
        return Err(ValkeyError::Str(
            "ERR syntax error, WITHSCORES not supported in combination with BYLEX",
        ));
    }

    let inner = match open_zset_read(ctx, key)? {
        None => return Ok(ValkeyValue::Array(vec![])),
        Some(z) => z,
    };

    let pairs: Vec<(&Vec<u8>, f64)> = if by_score {
        // For BYSCORE+REV, arguments are swapped: max first, min second.
        let (effective_min, effective_max) = if rev {
            (max_arg, min_arg)
        } else {
            (min_arg, max_arg)
        };
        let min_bound = parse_score_bound(effective_min)?;
        let max_bound = parse_score_bound(effective_max)?;
        let mut entries = score_range_entries(&inner, min_bound, max_bound);
        if rev {
            entries.reverse();
        }
        if let Some((offset, count)) = limit {
            entries = apply_limit(entries, offset, count);
        }
        entries
    } else if by_lex {
        // For BYLEX+REV, arguments are swapped: max first, min second.
        let (effective_min, effective_max) = if rev {
            (max_arg, min_arg)
        } else {
            (min_arg, max_arg)
        };
        let min_bound = parse_lex_bound(effective_min)?;
        let max_bound = parse_lex_bound(effective_max)?;
        let mut entries = lex_range_entries(&inner, &min_bound, &max_bound);
        if rev {
            entries.reverse();
        }
        if let Some((offset, count)) = limit {
            entries = apply_limit(entries, offset, count);
        }
        entries
    } else {
        // Rank-based: min/max are integer indices.
        let start_idx: i64 = std::str::from_utf8(min_arg)
            .ok()
            .and_then(|s| s.parse().ok())
            .ok_or(ValkeyError::Str(
                "ERR value is not an integer or out of range",
            ))?;
        let stop_idx: i64 = std::str::from_utf8(max_arg)
            .ok()
            .and_then(|s| s.parse().ok())
            .ok_or(ValkeyError::Str(
                "ERR value is not an integer or out of range",
            ))?;
        let mut all: Vec<(&Vec<u8>, f64)> = inner
            .scores
            .iter()
            .map(|((ScoreF64(s), m), ())| (m, *s))
            .collect();
        if rev {
            all.reverse();
        }
        let (start, end) = resolve_range(start_idx, stop_idx, all.len());
        all[start..end].to_vec()
    };

    Ok(ValkeyValue::Array(build_range_reply(&pairs, with_scores)))
}

// ── FLASH.ZRANGEBYSCORE ───────────────────────────────────────────────────────

/// `FLASH.ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]`
pub fn flash_zrangebyscore_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 4 {
        return Err(ValkeyError::WrongArity);
    }
    let key = &args[1];
    let min_bound = parse_score_bound(args[2].as_slice())?;
    let max_bound = parse_score_bound(args[3].as_slice())?;

    let (with_scores, limit) = parse_range_opts(&args[4..])?;

    let inner = match open_zset_read(ctx, key)? {
        None => return Ok(ValkeyValue::Array(vec![])),
        Some(z) => z,
    };

    let mut pairs = score_range_entries(&inner, min_bound, max_bound);
    if let Some((off, cnt)) = limit {
        pairs = apply_limit(pairs, off, cnt);
    }

    Ok(ValkeyValue::Array(build_range_reply(&pairs, with_scores)))
}

// ── FLASH.ZREVRANGEBYSCORE ────────────────────────────────────────────────────

/// `FLASH.ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]`
/// Note: max comes before min (reversed compared to ZRANGEBYSCORE).
pub fn flash_zrevrangebyscore_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 4 {
        return Err(ValkeyError::WrongArity);
    }
    let key = &args[1];
    // For ZREVRANGEBYSCORE, arg2 = max, arg3 = min
    let max_bound = parse_score_bound(args[2].as_slice())?;
    let min_bound = parse_score_bound(args[3].as_slice())?;

    let (with_scores, limit) = parse_range_opts(&args[4..])?;

    let inner = match open_zset_read(ctx, key)? {
        None => return Ok(ValkeyValue::Array(vec![])),
        Some(z) => z,
    };

    let mut pairs = score_range_entries(&inner, min_bound, max_bound);
    pairs.reverse(); // descending order
    if let Some((off, cnt)) = limit {
        pairs = apply_limit(pairs, off, cnt);
    }

    Ok(ValkeyValue::Array(build_range_reply(&pairs, with_scores)))
}

// ── FLASH.ZRANGEBYLEX ─────────────────────────────────────────────────────────

/// `FLASH.ZRANGEBYLEX key min max [LIMIT offset count]`
pub fn flash_zrangebylex_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 4 {
        return Err(ValkeyError::WrongArity);
    }
    let key = &args[1];
    let min_bound = parse_lex_bound(args[2].as_slice())?;
    let max_bound = parse_lex_bound(args[3].as_slice())?;

    let limit = parse_limit_only(&args[4..])?;

    let inner = match open_zset_read(ctx, key)? {
        None => return Ok(ValkeyValue::Array(vec![])),
        Some(z) => z,
    };

    let mut pairs = lex_range_entries(&inner, &min_bound, &max_bound);
    if let Some((off, cnt)) = limit {
        pairs = apply_limit(pairs, off, cnt);
    }

    Ok(ValkeyValue::Array(build_range_reply(&pairs, false)))
}

// ── FLASH.ZREVRANGEBYLEX ──────────────────────────────────────────────────────

/// `FLASH.ZREVRANGEBYLEX key max min [LIMIT offset count]`
/// Note: max comes before min.
pub fn flash_zrevrangebylex_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 4 {
        return Err(ValkeyError::WrongArity);
    }
    let key = &args[1];
    // max is arg2, min is arg3
    let max_bound = parse_lex_bound(args[2].as_slice())?;
    let min_bound = parse_lex_bound(args[3].as_slice())?;

    let limit = parse_limit_only(&args[4..])?;

    let inner = match open_zset_read(ctx, key)? {
        None => return Ok(ValkeyValue::Array(vec![])),
        Some(z) => z,
    };

    let mut pairs = lex_range_entries(&inner, &min_bound, &max_bound);
    pairs.reverse(); // descending lex order
    if let Some((off, cnt)) = limit {
        pairs = apply_limit(pairs, off, cnt);
    }

    Ok(ValkeyValue::Array(build_range_reply(&pairs, false)))
}

// ── Option parsers ────────────────────────────────────────────────────────────

/// Parse optional `[WITHSCORES] [LIMIT offset count]` for ZRANGEBYSCORE/ZREVRANGEBYSCORE.
fn parse_range_opts(args: &[ValkeyString]) -> Result<(bool, Option<(i64, i64)>), ValkeyError> {
    let mut with_scores = false;
    let mut limit: Option<(i64, i64)> = None;
    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_slice().to_ascii_uppercase().as_slice() {
            b"WITHSCORES" => {
                with_scores = true;
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
    Ok((with_scores, limit))
}

/// Parse optional `[LIMIT offset count]` for ZRANGEBYLEX/ZREVRANGEBYLEX.
fn parse_limit_only(args: &[ValkeyString]) -> Result<Option<(i64, i64)>, ValkeyError> {
    if args.is_empty() {
        return Ok(None);
    }
    if args.len() < 3 || !args[0].as_slice().eq_ignore_ascii_case(b"LIMIT") {
        return Err(ValkeyError::Str("ERR syntax error"));
    }
    let off: i64 = std::str::from_utf8(args[1].as_slice())
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or(ValkeyError::Str(
            "ERR value is not an integer or out of range",
        ))?;
    let cnt: i64 = std::str::from_utf8(args[2].as_slice())
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or(ValkeyError::Str(
            "ERR value is not an integer or out of range",
        ))?;
    Ok(Some((off, cnt)))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::zset_common::LexBound;
    use crate::types::zset::ZSetInner;
    use std::ops::Bound;

    fn zset_with(entries: &[(&[u8], f64)]) -> ZSetInner {
        let mut z = ZSetInner::new();
        for (m, s) in entries {
            z.insert(m.to_vec(), *s);
        }
        z
    }

    #[test]
    fn score_range_full() {
        let z = zset_with(&[(b"a", 1.0), (b"b", 2.0), (b"c", 3.0)]);
        let r = score_range_entries(&z, Bound::Unbounded, Bound::Unbounded);
        assert_eq!(r.len(), 3);
    }

    #[test]
    fn score_range_inclusive() {
        let z = zset_with(&[(b"a", 1.0), (b"b", 2.0), (b"c", 3.0)]);
        let r = score_range_entries(&z, Bound::Included(1.5), Bound::Included(2.5));
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].0.as_slice(), b"b");
    }

    #[test]
    fn score_range_exclusive() {
        let z = zset_with(&[(b"a", 1.0), (b"b", 2.0), (b"c", 3.0)]);
        let r = score_range_entries(&z, Bound::Excluded(1.0), Bound::Excluded(3.0));
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].0.as_slice(), b"b");
    }

    #[test]
    fn lex_range_basic() {
        let z = zset_with(&[(b"a", 0.0), (b"b", 0.0), (b"c", 0.0)]);
        let r = lex_range_entries(
            &z,
            &LexBound::Included(b"a".to_vec()),
            &LexBound::Included(b"b".to_vec()),
        );
        assert_eq!(r.len(), 2);
    }

    #[test]
    fn lex_range_exclusive() {
        let z = zset_with(&[(b"a", 0.0), (b"b", 0.0), (b"c", 0.0)]);
        let r = lex_range_entries(&z, &LexBound::Excluded(b"a".to_vec()), &LexBound::Max);
        assert_eq!(r.len(), 2); // b and c
    }

    #[test]
    fn rank_range_resolve() {
        let z = zset_with(&[(b"a", 1.0), (b"b", 2.0), (b"c", 3.0)]);
        let all: Vec<_> = z
            .scores
            .iter()
            .map(|((ScoreF64(s), m), ())| (m, *s))
            .collect();
        let (start, end) = resolve_range(0, -1, all.len());
        assert_eq!(&all[start..end].len(), &3);
    }

    #[test]
    fn apply_limit_pagination() {
        let items: Vec<i32> = (0..10).collect();
        let page = apply_limit(items, 3, 4);
        assert_eq!(page, vec![3, 4, 5, 6]);
    }
}
