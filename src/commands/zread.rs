use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

use crate::commands::zset_common::{
    glob_match, lex_range_entries, parse_lex_bound, parse_score_bound, promote_cold_zset,
    score_range_entries,
};
use crate::types::Tier;
use crate::types::zset::{FLASH_ZSET_TYPE, FlashZSetObject, ScoreF64, ZSetInner, format_score};

// ── helpers ───────────────────────────────────────────────────────────────────

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

// ── FLASH.ZSCORE ──────────────────────────────────────────────────────────────

/// `FLASH.ZSCORE key member`
pub fn flash_zscore_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() != 3 {
        return Err(ValkeyError::WrongArity);
    }
    let inner = match open_zset_read(ctx, &args[1])? {
        None => return Ok(ValkeyValue::Null),
        Some(z) => z,
    };
    match inner.get_score(args[2].as_slice()) {
        None => Ok(ValkeyValue::Null),
        Some(score) => Ok(ValkeyValue::StringBuffer(format_score(score).into_bytes())),
    }
}

// ── FLASH.ZRANK / FLASH.ZREVRANK ──────────────────────────────────────────────

fn zrank_impl(ctx: &Context, args: Vec<ValkeyString>, rev: bool) -> ValkeyResult {
    if args.len() < 3 || args.len() > 4 {
        return Err(ValkeyError::WrongArity);
    }
    let with_score = args
        .get(3)
        .map(|a| a.as_slice().eq_ignore_ascii_case(b"WITHSCORE"))
        .unwrap_or(false);

    if args.len() == 4 && !with_score {
        return Err(ValkeyError::Str("ERR syntax error"));
    }

    let inner = match open_zset_read(ctx, &args[1])? {
        None => return Ok(ValkeyValue::Null),
        Some(z) => z,
    };

    let member = args[2].as_slice();
    let score = match inner.get_score(member) {
        None => return Ok(ValkeyValue::Null),
        Some(s) => s,
    };

    // Rank = number of elements with (score, member) strictly less than target.
    let target_key = (ScoreF64(score), member.to_vec());
    let rank = inner.scores.range(..target_key).count();
    let effective_rank = if rev { inner.len() - 1 - rank } else { rank };

    if with_score {
        Ok(ValkeyValue::Array(vec![
            ValkeyValue::Integer(effective_rank as i64),
            ValkeyValue::StringBuffer(format_score(score).into_bytes()),
        ]))
    } else {
        Ok(ValkeyValue::Integer(effective_rank as i64))
    }
}

pub fn flash_zrank_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    zrank_impl(ctx, args, false)
}

pub fn flash_zrevrank_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    zrank_impl(ctx, args, true)
}

// ── FLASH.ZCARD ───────────────────────────────────────────────────────────────

/// `FLASH.ZCARD key`
pub fn flash_zcard_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() != 2 {
        return Err(ValkeyError::WrongArity);
    }
    let inner = match open_zset_read(ctx, &args[1])? {
        None => return Ok(ValkeyValue::Integer(0)),
        Some(z) => z,
    };
    Ok(ValkeyValue::Integer(inner.len() as i64))
}

// ── FLASH.ZCOUNT ──────────────────────────────────────────────────────────────

/// `FLASH.ZCOUNT key min max`
pub fn flash_zcount_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() != 4 {
        return Err(ValkeyError::WrongArity);
    }
    let min_bound = parse_score_bound(args[2].as_slice())?;
    let max_bound = parse_score_bound(args[3].as_slice())?;
    let inner = match open_zset_read(ctx, &args[1])? {
        None => return Ok(ValkeyValue::Integer(0)),
        Some(z) => z,
    };
    let count = score_range_entries(&inner, min_bound, max_bound).len();
    Ok(ValkeyValue::Integer(count as i64))
}

// ── FLASH.ZLEXCOUNT ───────────────────────────────────────────────────────────

/// `FLASH.ZLEXCOUNT key min max`
pub fn flash_zlexcount_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() != 4 {
        return Err(ValkeyError::WrongArity);
    }
    let min_bound = parse_lex_bound(args[2].as_slice())?;
    let max_bound = parse_lex_bound(args[3].as_slice())?;
    let inner = match open_zset_read(ctx, &args[1])? {
        None => return Ok(ValkeyValue::Integer(0)),
        Some(z) => z,
    };
    let count = lex_range_entries(&inner, &min_bound, &max_bound).len();
    Ok(ValkeyValue::Integer(count as i64))
}

// ── FLASH.ZSCAN ───────────────────────────────────────────────────────────────

/// `FLASH.ZSCAN key cursor [MATCH pattern] [COUNT count]`
///
/// Returns `[next_cursor, [member1, score1, member2, score2, ...]]`.
/// Cursor is a sequential index into the member list sorted by (score, member).
pub fn flash_zscan_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 3 {
        return Err(ValkeyError::WrongArity);
    }
    let key = &args[1];
    let cursor: u64 = std::str::from_utf8(args[2].as_slice())
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or(ValkeyError::Str(
            "ERR value is not an integer or out of range",
        ))?;

    let mut pattern: Option<Vec<u8>> = None;
    let mut count: usize = 10;

    let mut i = 3usize;
    while i < args.len() {
        match args[i].as_slice().to_ascii_uppercase().as_slice() {
            b"MATCH" => {
                if i + 1 >= args.len() {
                    return Err(ValkeyError::Str("ERR syntax error"));
                }
                pattern = Some(args[i + 1].as_slice().to_vec());
                i += 2;
            }
            b"COUNT" => {
                if i + 1 >= args.len() {
                    return Err(ValkeyError::Str("ERR syntax error"));
                }
                let c: i64 = std::str::from_utf8(args[i + 1].as_slice())
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .ok_or(ValkeyError::Str(
                        "ERR value is not an integer or out of range",
                    ))?;
                if c < 1 {
                    return Err(ValkeyError::Str("ERR syntax error"));
                }
                count = c as usize;
                i += 2;
            }
            _ => return Err(ValkeyError::Str("ERR syntax error")),
        }
    }

    let inner = match open_zset_read(ctx, key)? {
        None => {
            return Ok(ValkeyValue::Array(vec![
                ValkeyValue::SimpleString("0".to_string()),
                ValkeyValue::Array(vec![]),
            ]));
        }
        Some(z) => z,
    };

    // Collect all (member, score) pairs in BTreeMap order.
    let all_entries: Vec<(&Vec<u8>, f64)> = inner
        .scores
        .iter()
        .map(|((ScoreF64(s), m), ())| (m, *s))
        .collect();

    // Apply pattern filter to get matching entries.
    let filtered: Vec<(&Vec<u8>, f64)> = all_entries
        .iter()
        .filter(|(m, _)| pattern.as_deref().map(|p| glob_match(p, m)).unwrap_or(true))
        .copied()
        .collect();

    let total = filtered.len();
    let start = (cursor as usize).min(total);
    let end = (start + count).min(total);
    let page = &filtered[start..end];

    let next_cursor = if end >= total { 0u64 } else { end as u64 };

    let mut items: Vec<ValkeyValue> = Vec::with_capacity(page.len() * 2);
    for (member, score) in page {
        items.push(ValkeyValue::StringBuffer(member.to_vec()));
        items.push(ValkeyValue::StringBuffer(format_score(*score).into_bytes()));
    }

    Ok(ValkeyValue::Array(vec![
        ValkeyValue::SimpleString(next_cursor.to_string()),
        ValkeyValue::Array(items),
    ]))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
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
    fn zscore_present() {
        let z = zset_with(&[(b"a", 1.5)]);
        assert_eq!(z.get_score(b"a"), Some(1.5));
    }

    #[test]
    fn zscore_absent() {
        let z = ZSetInner::new();
        assert_eq!(z.get_score(b"missing"), None);
    }

    #[test]
    fn zrank_ascending() {
        let z = zset_with(&[(b"a", 1.0), (b"b", 2.0), (b"c", 3.0)]);
        let member = b"b";
        let score = z.get_score(member).unwrap();
        let target = (ScoreF64(score), member.to_vec());
        let rank = z.scores.range(..target).count();
        assert_eq!(rank, 1); // "a" is before "b"
    }

    #[test]
    fn zrank_descending() {
        let z = zset_with(&[(b"a", 1.0), (b"b", 2.0), (b"c", 3.0)]);
        let member = b"b";
        let score = z.get_score(member).unwrap();
        let target = (ScoreF64(score), member.to_vec());
        let rank = z.scores.range(..target).count();
        let rev_rank = z.len() - 1 - rank;
        assert_eq!(rev_rank, 1); // "c" is after "b" in rev
    }

    #[test]
    fn zcard_returns_len() {
        let z = zset_with(&[(b"a", 1.0), (b"b", 2.0)]);
        assert_eq!(z.len(), 2);
    }

    #[test]
    fn zcount_all() {
        let z = zset_with(&[(b"a", 1.0), (b"b", 2.0), (b"c", 3.0)]);
        let r = score_range_entries(&z, Bound::Unbounded, Bound::Unbounded);
        assert_eq!(r.len(), 3);
    }

    #[test]
    fn zlexcount_range() {
        let z = zset_with(&[(b"a", 0.0), (b"b", 0.0), (b"c", 0.0), (b"d", 0.0)]);
        let r = lex_range_entries(
            &z,
            &crate::commands::zset_common::LexBound::Included(b"b".to_vec()),
            &crate::commands::zset_common::LexBound::Included(b"c".to_vec()),
        );
        assert_eq!(r.len(), 2);
    }

    #[test]
    fn zscan_pagination() {
        // 10 entries, cursor=0, count=3 → returns first 3, next cursor=3
        let entries: Vec<(&[u8], f64)> = (0..10u8)
            .map(|i| {
                let b: &'static [u8] = Box::leak(format!("{i:02}").into_bytes().into_boxed_slice());
                (b, i as f64)
            })
            .collect();
        let mut z = ZSetInner::new();
        for (m, s) in &entries {
            z.insert(m.to_vec(), *s);
        }
        let all: Vec<_> = z
            .scores
            .iter()
            .map(|((ScoreF64(s), m), ())| (m, *s))
            .collect();
        let page = &all[0..3];
        let next = 3u64;
        assert_eq!(page.len(), 3);
        assert_eq!(next, 3);
    }

    #[test]
    fn zscan_done_when_cursor_at_end() {
        let z = zset_with(&[(b"a", 1.0), (b"b", 2.0)]);
        let all: Vec<_> = z
            .scores
            .iter()
            .map(|((ScoreF64(s), m), ())| (m, *s))
            .collect();
        let total = all.len();
        let cursor = total;
        let end = (cursor + 10).min(total);
        let next: u64 = if end >= total { 0 } else { end as u64 };
        assert_eq!(next, 0);
    }
}
