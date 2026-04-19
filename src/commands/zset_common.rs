use std::ops::Bound;

use valkey_module::{ValkeyError, ValkeyResult, ValkeyValue};

use crate::types::zset::{ZSetInner, zset_deserialize_or_warn};

// ── Score parsing ─────────────────────────────────────────────────────────────

/// Parse a score from bytes. Accepts "-inf", "+inf", "inf", and decimal floats.
/// Rejects NaN.
pub fn parse_score(bytes: &[u8]) -> Result<f64, ValkeyError> {
    let s = std::str::from_utf8(bytes).map_err(|_| ValkeyError::Str("ERR not a valid float"))?;
    match s.to_lowercase().as_str() {
        "+inf" | "inf" => return Ok(f64::INFINITY),
        "-inf" => return Ok(f64::NEG_INFINITY),
        _ => {}
    }
    let v = s
        .parse::<f64>()
        .map_err(|_| ValkeyError::Str("ERR not a valid float"))?;
    if v.is_nan() {
        return Err(ValkeyError::Str("ERR not a valid float"));
    }
    Ok(v)
}

// ── Score range bound parsing ─────────────────────────────────────────────────

/// Parse a ZRANGEBYSCORE-style boundary.
/// - `-inf` → Unbounded (no lower limit)
/// - `+inf` / `inf` → Unbounded (no upper limit)
/// - `(value` → Excluded(value)
/// - `value` → Included(value)
pub fn parse_score_bound(bytes: &[u8]) -> Result<Bound<f64>, ValkeyError> {
    if bytes.eq_ignore_ascii_case(b"-inf") {
        return Ok(Bound::Unbounded);
    }
    if bytes.eq_ignore_ascii_case(b"+inf") || bytes.eq_ignore_ascii_case(b"inf") {
        return Ok(Bound::Unbounded);
    }
    let (exclusive, rest) = if bytes.first() == Some(&b'(') {
        (true, &bytes[1..])
    } else {
        (false, bytes)
    };
    let score = parse_score(rest)?;
    if exclusive {
        Ok(Bound::Excluded(score))
    } else {
        Ok(Bound::Included(score))
    }
}

// ── Lex range bound parsing ───────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum LexBound {
    Min,               // `-`
    Max,               // `+`
    Included(Vec<u8>), // `[member`
    Excluded(Vec<u8>), // `(member`
}

pub fn parse_lex_bound(bytes: &[u8]) -> Result<LexBound, ValkeyError> {
    if bytes == b"-" {
        return Ok(LexBound::Min);
    }
    if bytes == b"+" {
        return Ok(LexBound::Max);
    }
    if bytes.is_empty() {
        return Err(ValkeyError::Str(
            "ERR min or max is not valid string range item",
        ));
    }
    match bytes[0] {
        b'[' => Ok(LexBound::Included(bytes[1..].to_vec())),
        b'(' => Ok(LexBound::Excluded(bytes[1..].to_vec())),
        _ => Err(ValkeyError::Str(
            "ERR min or max is not valid string range item",
        )),
    }
}

// ── Range helpers ─────────────────────────────────────────────────────────────

/// Filter ZSet entries by score range and optionally paginate.
/// Returns `(member_bytes, score)` pairs in ascending (score, member) order.
pub fn score_range_entries(
    inner: &ZSetInner,
    min: Bound<f64>,
    max: Bound<f64>,
) -> Vec<(&Vec<u8>, f64)> {
    inner
        .scores
        .iter()
        .filter(|((crate::types::zset::ScoreF64(s), _), ())| {
            let above_min = match min {
                Bound::Unbounded => true,
                Bound::Included(v) => *s >= v,
                Bound::Excluded(v) => *s > v,
            };
            let below_max = match max {
                Bound::Unbounded => true,
                Bound::Included(v) => *s <= v,
                Bound::Excluded(v) => *s < v,
            };
            above_min && below_max
        })
        .map(|((crate::types::zset::ScoreF64(s), m), ())| (m, *s))
        .collect()
}

/// Filter ZSet entries by lex range.
/// Returns `(member_bytes, score)` pairs in ascending (score, member) order.
pub fn lex_range_entries<'a>(
    inner: &'a ZSetInner,
    min: &'_ LexBound,
    max: &'_ LexBound,
) -> Vec<(&'a Vec<u8>, f64)> {
    inner
        .scores
        .iter()
        .filter(|((_, m), ())| {
            let above_min = match min {
                LexBound::Min => true,
                LexBound::Max => false,
                LexBound::Included(v) => m.as_slice() >= v.as_slice(),
                LexBound::Excluded(v) => m.as_slice() > v.as_slice(),
            };
            let below_max = match max {
                LexBound::Max => true,
                LexBound::Min => false,
                LexBound::Included(v) => m.as_slice() <= v.as_slice(),
                LexBound::Excluded(v) => m.as_slice() < v.as_slice(),
            };
            above_min && below_max
        })
        .map(|((crate::types::zset::ScoreF64(s), m), ())| (m, *s))
        .collect()
}

/// Apply LIMIT offset/count to a result slice.
/// A count of -1 means no limit.
pub fn apply_limit<T: Clone>(entries: Vec<T>, offset: i64, count: i64) -> Vec<T> {
    let start = if offset < 0 {
        0usize
    } else {
        (offset as usize).min(entries.len())
    };
    let slice = &entries[start..];
    if count < 0 {
        slice.to_vec()
    } else {
        slice[..(count as usize).min(slice.len())].to_vec()
    }
}

/// Build a reply array from (member, score) pairs.
/// If `with_scores` is true, interleaves [member, score_str, ...].
pub fn build_range_reply(pairs: &[(&Vec<u8>, f64)], with_scores: bool) -> Vec<ValkeyValue> {
    let mut out = Vec::with_capacity(if with_scores {
        pairs.len() * 2
    } else {
        pairs.len()
    });
    for (member, score) in pairs {
        out.push(ValkeyValue::StringBuffer(member.to_vec()));
        if with_scores {
            out.push(ValkeyValue::StringBuffer(
                crate::types::zset::format_score(*score).into_bytes(),
            ));
        }
    }
    out
}

// ── Glob matching ─────────────────────────────────────────────────────────────

/// Simple glob matching for ZSCAN MATCH (`*`, `?` supported).
pub fn glob_match(pattern: &[u8], text: &[u8]) -> bool {
    let mut pi = 0usize;
    let mut ti = 0usize;
    let mut star_pi: Option<usize> = None;
    let mut star_ti = 0usize;

    while ti < text.len() {
        if pi < pattern.len() && (pattern[pi] == b'?' || pattern[pi] == text[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < pattern.len() && pattern[pi] == b'*' {
            star_pi = Some(pi);
            star_ti = ti;
            pi += 1;
        } else if let Some(sp) = star_pi {
            star_ti += 1;
            ti = star_ti;
            pi = sp + 1;
        } else {
            return false;
        }
    }
    while pi < pattern.len() && pattern[pi] == b'*' {
        pi += 1;
    }
    pi == pattern.len()
}

// ── Cold tier promotion ───────────────────────────────────────────────────────

/// Promote a Cold ZSet from NVMe synchronously.
/// On replica or if storage is unavailable, returns an empty ZSet.
pub fn promote_cold_zset(backend_offset: u64, value_len: u32) -> Result<ZSetInner, ValkeyError> {
    if crate::replication::is_replica() {
        return Ok(ZSetInner::new());
    }
    let storage = crate::STORAGE
        .get()
        .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;
    let bytes = storage
        .read_at_offset(backend_offset, value_len)
        .map_err(|e| ValkeyError::String(e.to_string()))?;
    Ok(zset_deserialize_or_warn(&bytes))
}

// ── NVMe write completion ─────────────────────────────────────────────────────

/// Send-safe reply for ZSet write commands.
/// `ValkeyValue` is not `Send` because it may contain `ValkeyString` (raw ptr).
/// We store only primitive data here and construct `ValkeyValue` at reply time.
pub enum ZSetReply {
    Integer(i64),
    BulkString(Vec<u8>),
    Null,
    /// Flat alternating [bytes, bytes, ...] — used by ZPOPMIN/ZPOPMAX.
    FlatPairs(Vec<Vec<u8>>),
}

impl ZSetReply {
    pub fn into_valkey_value(self) -> ValkeyValue {
        match self {
            ZSetReply::Integer(n) => ValkeyValue::Integer(n),
            ZSetReply::BulkString(b) => ValkeyValue::StringBuffer(b),
            ZSetReply::Null => ValkeyValue::Null,
            ZSetReply::FlatPairs(pairs) => {
                ValkeyValue::Array(pairs.into_iter().map(ValkeyValue::StringBuffer).collect())
            }
        }
    }
}

#[cfg(not(test))]
pub struct ZSetWriteHandle {
    pub tsc: valkey_module::ThreadSafeContext<valkey_module::BlockedClient<()>>,
    pub reply: ZSetReply,
}

#[cfg(not(test))]
impl ZSetWriteHandle {
    pub fn new(bc: valkey_module::BlockedClient<()>, reply: ZSetReply) -> Self {
        ZSetWriteHandle {
            tsc: valkey_module::ThreadSafeContext::with_blocked_client(bc),
            reply,
        }
    }
}

#[cfg(not(test))]
impl crate::async_io::CompletionHandle for ZSetWriteHandle {
    fn complete(self: Box<Self>, result: crate::storage::backend::StorageResult<Vec<u8>>) {
        use valkey_module::logging;
        if let Err(ref e) = result {
            logging::log_warning(format!("flash: ZSet NVMe write failed: {e}").as_str());
        }
        let val = self.reply.into_valkey_value();
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.tsc.reply(Ok(val));
        }));
    }
}

/// Perform async NVMe write and reply when done.
///
/// If `serialized` is empty, the key was deleted from Valkey — NVMe delete is
/// performed instead of a put.  On replica or when storage is absent,
/// the reply is returned immediately without any NVMe I/O.
pub fn finish_zset_write(
    _ctx: &valkey_module::Context,
    _key_bytes: Vec<u8>,
    _serialized: Vec<u8>,
    reply: ZSetReply,
) -> ValkeyResult {
    #[cfg(not(test))]
    {
        let ctx = _ctx;
        let key_bytes = _key_bytes;
        let serialized = _serialized;
        if crate::replication::is_replica()
            || (crate::STORAGE.get().is_none() && crate::replication::must_obey_client(ctx))
        {
            return Ok(reply.into_valkey_value());
        }
        use crate::storage::backend::StorageBackend;
        let storage = crate::STORAGE
            .get()
            .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;
        let pool = crate::POOL
            .get()
            .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;
        let bc = ctx.block_client();
        let handle = Box::new(ZSetWriteHandle::new(bc, reply));
        pool.submit_or_complete(handle, move || {
            if !serialized.is_empty() {
                let offset = storage.put(&key_bytes, &serialized)?;
                if let Some(wal) = crate::WAL.get() {
                    let kh = crate::util::key_hash(&key_bytes);
                    let vh = crate::util::value_hash(&serialized);
                    let _ = wal.append(crate::storage::wal::WalOp::Put {
                        key_hash: kh,
                        offset,
                        value_hash: vh,
                    });
                }
            } else {
                storage.delete(&key_bytes).ok();
            }
            Ok(vec![])
        });
        return Ok(ValkeyValue::NoReply);
    }
    #[allow(unreachable_code)]
    Ok(reply.into_valkey_value())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_score_infinity() {
        assert_eq!(parse_score(b"+inf").unwrap(), f64::INFINITY);
        assert_eq!(parse_score(b"inf").unwrap(), f64::INFINITY);
        assert_eq!(parse_score(b"-inf").unwrap(), f64::NEG_INFINITY);
    }

    #[test]
    fn parse_score_float() {
        assert!((parse_score(b"3.5").unwrap() - 3.5).abs() < 1e-10);
        assert!((parse_score(b"-1.5").unwrap() - (-1.5)).abs() < 1e-10);
    }

    #[test]
    fn parse_score_invalid() {
        assert!(parse_score(b"notanumber").is_err());
        assert!(parse_score(b"").is_err());
    }

    #[test]
    fn parse_score_bound_inclusive() {
        assert_eq!(parse_score_bound(b"5").unwrap(), Bound::Included(5.0));
    }

    #[test]
    fn parse_score_bound_exclusive() {
        assert_eq!(parse_score_bound(b"(5").unwrap(), Bound::Excluded(5.0));
    }

    #[test]
    fn parse_score_bound_inf() {
        assert_eq!(parse_score_bound(b"-inf").unwrap(), Bound::Unbounded);
        assert_eq!(parse_score_bound(b"+inf").unwrap(), Bound::Unbounded);
        assert_eq!(parse_score_bound(b"inf").unwrap(), Bound::Unbounded);
    }

    #[test]
    fn parse_lex_bound_sentinels() {
        assert_eq!(parse_lex_bound(b"-").unwrap(), LexBound::Min);
        assert_eq!(parse_lex_bound(b"+").unwrap(), LexBound::Max);
    }

    #[test]
    fn parse_lex_bound_brackets() {
        assert_eq!(
            parse_lex_bound(b"[foo").unwrap(),
            LexBound::Included(b"foo".to_vec())
        );
        assert_eq!(
            parse_lex_bound(b"(bar").unwrap(),
            LexBound::Excluded(b"bar".to_vec())
        );
    }

    #[test]
    fn parse_lex_bound_invalid() {
        assert!(parse_lex_bound(b"foo").is_err());
        assert!(parse_lex_bound(b"").is_err());
    }

    #[test]
    fn apply_limit_basic() {
        let v: Vec<i32> = (0..10).collect();
        assert_eq!(apply_limit(v.clone(), 2, 3), vec![2, 3, 4]);
        assert_eq!(apply_limit(v.clone(), 0, -1), v);
        assert_eq!(apply_limit(v.clone(), 100, 5), Vec::<i32>::new());
    }

    #[test]
    fn glob_match_star() {
        assert!(glob_match(b"*", b"anything"));
        assert!(glob_match(b"h*llo", b"hello"));
        assert!(glob_match(b"h*llo", b"hllo"));
        assert!(!glob_match(b"h*llo", b"world"));
    }

    #[test]
    fn glob_match_question() {
        assert!(glob_match(b"h?llo", b"hello"));
        assert!(!glob_match(b"h?llo", b"hllo"));
    }

    #[test]
    fn glob_match_exact() {
        assert!(glob_match(b"abc", b"abc"));
        assert!(!glob_match(b"abc", b"abcd"));
    }
}
