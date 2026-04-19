use std::collections::VecDeque;
use std::time::{SystemTime, UNIX_EPOCH};

use valkey_module::{ValkeyError, ValkeyString};

use crate::STORAGE;
use crate::types::list::list_deserialize_or_warn;

pub fn current_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn parse_positive_bytes(bytes: &[u8], cmd: &str) -> Result<i64, ValkeyError> {
    let s = std::str::from_utf8(bytes)
        .map_err(|_| ValkeyError::Str("ERR value is not an integer or out of range"))?;
    let n: i64 = s
        .parse()
        .map_err(|_| ValkeyError::Str("ERR value is not an integer or out of range"))?;
    if n <= 0 {
        return Err(ValkeyError::String(format!(
            "ERR invalid expire time in {cmd}"
        )));
    }
    Ok(n)
}

pub const TTL_KEYWORDS: &[&[u8]] = &[b"EX", b"PX", b"EXAT", b"PXAT", b"KEEPTTL"];

pub fn find_ttl_start(args: &[ValkeyString]) -> Option<usize> {
    args.iter().position(|a| {
        let upper = a.as_slice().to_ascii_uppercase();
        TTL_KEYWORDS.contains(&upper.as_slice())
    })
}

pub fn parse_ttl_options_raw(
    args: &[&[u8]],
    cmd: &str,
) -> Result<(Option<i64>, bool), ValkeyError> {
    if args.is_empty() {
        return Ok((None, false));
    }
    let mut ttl_abs_ms: Option<i64> = None;
    let mut keepttl = false;
    let mut i = 0;

    while i < args.len() {
        let upper = args[i].to_ascii_uppercase();
        match upper.as_slice() {
            b"EX" => {
                i += 1;
                if i >= args.len() {
                    return Err(ValkeyError::Str("ERR syntax error"));
                }
                let secs = parse_positive_bytes(args[i], cmd)?;
                let rel_ms = secs.checked_mul(1000).ok_or_else(|| {
                    ValkeyError::String(format!("ERR invalid expire time in {cmd}"))
                })?;
                ttl_abs_ms = Some(current_time_ms() + rel_ms);
            }
            b"PX" => {
                i += 1;
                if i >= args.len() {
                    return Err(ValkeyError::Str("ERR syntax error"));
                }
                let ms = parse_positive_bytes(args[i], cmd)?;
                ttl_abs_ms = Some(current_time_ms() + ms);
            }
            b"EXAT" => {
                i += 1;
                if i >= args.len() {
                    return Err(ValkeyError::Str("ERR syntax error"));
                }
                let secs = parse_positive_bytes(args[i], cmd)?;
                let abs_ms = secs.checked_mul(1000).ok_or_else(|| {
                    ValkeyError::String(format!("ERR invalid expire time in {cmd}"))
                })?;
                ttl_abs_ms = Some(abs_ms);
            }
            b"PXAT" => {
                i += 1;
                if i >= args.len() {
                    return Err(ValkeyError::Str("ERR syntax error"));
                }
                let ms = parse_positive_bytes(args[i], cmd)?;
                ttl_abs_ms = Some(ms);
            }
            b"KEEPTTL" => {
                keepttl = true;
            }
            _ => {
                return Err(ValkeyError::Str("ERR syntax error"));
            }
        }
        i += 1;
    }

    if ttl_abs_ms.is_some() && keepttl {
        return Err(ValkeyError::Str(
            "ERR KEEPTTL option is not compatible with EX, PX, EXAT, or PXAT",
        ));
    }
    Ok((ttl_abs_ms, keepttl))
}

pub fn parse_ttl_options(
    args: &[ValkeyString],
    cmd: &str,
) -> Result<(Option<i64>, bool), ValkeyError> {
    let raw: Vec<&[u8]> = args.iter().map(|vs| vs.as_slice()).collect();
    parse_ttl_options_raw(&raw, cmd)
}

/// Promote a Cold list from NVMe synchronously (v1: event-loop blocking).
/// On replica or if storage is unavailable, returns an empty list.
pub fn promote_cold_list(
    backend_offset: u64,
    value_len: u32,
) -> Result<VecDeque<Vec<u8>>, ValkeyError> {
    if crate::replication::is_replica() {
        return Ok(VecDeque::new());
    }
    let storage = STORAGE
        .get()
        .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;
    let bytes = storage
        .read_at_offset(backend_offset, value_len)
        .map_err(|e| ValkeyError::String(e.to_string()))?;
    Ok(list_deserialize_or_warn(&bytes))
}

/// Resolve the absolute index from a possibly-negative Redis index.
/// Returns `None` if the index is out of range.
pub fn resolve_index(idx: i64, len: usize) -> Option<usize> {
    let i = if idx >= 0 {
        idx as usize
    } else {
        let abs = (-idx) as usize;
        if abs > len {
            return None;
        }
        len - abs
    };
    if i < len { Some(i) } else { None }
}

/// Resolve LRANGE-style start/stop (inclusive, negative allowed) to a
/// half-open [start, end) range clamped to `[0, len)`.
/// Returns `(0, 0)` when the range is empty.
pub fn resolve_range(start: i64, stop: i64, len: usize) -> (usize, usize) {
    if len == 0 {
        return (0, 0);
    }
    let len_i = len as i64;
    let s = if start >= 0 {
        start
    } else {
        (len_i + start).max(0)
    } as usize;
    let e = if stop >= 0 {
        stop.min(len_i - 1)
    } else {
        (len_i + stop).max(-1)
    };
    if e < 0 || s >= len || s as i64 > e {
        return (0, 0);
    }
    (s, e as usize + 1)
}

/// Apply a stored absolute-TTL back onto an open writable key handle.
pub fn apply_ttl_to_key(
    key_handle: &valkey_module::key::ValkeyKeyWritable,
    stored_ttl: Option<i64>,
) -> Result<(), ValkeyError> {
    if let Some(abs_ms) = stored_ttl {
        use std::time::Duration;
        let remaining_ms = (abs_ms - current_time_ms()).max(1) as u64;
        key_handle
            .set_expire(Duration::from_millis(remaining_ms))
            .map_err(|e| ValkeyError::String(format!("flash: set_expire: {e}")))?;
    }
    Ok(())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn s(b: &[u8]) -> &[u8] {
        b
    }

    #[test]
    fn parse_ttl_empty() {
        let (ttl, keepttl) = parse_ttl_options_raw(&[], "CMD").unwrap();
        assert!(ttl.is_none());
        assert!(!keepttl);
    }

    #[test]
    fn parse_ttl_keepttl() {
        let (_, keepttl) = parse_ttl_options_raw(&[s(b"KEEPTTL")], "CMD").unwrap();
        assert!(keepttl);
    }

    #[test]
    fn parse_ttl_keepttl_case_insensitive() {
        let (_, keepttl) = parse_ttl_options_raw(&[s(b"keepttl")], "CMD").unwrap();
        assert!(keepttl);
    }

    #[test]
    fn parse_ttl_ex_positive() {
        let (ttl, _) = parse_ttl_options_raw(&[s(b"EX"), s(b"10")], "CMD").unwrap();
        assert!(ttl.is_some());
        assert!(ttl.unwrap() > 0);
    }

    #[test]
    fn parse_ttl_exat_absolute() {
        let abs_secs = 9_999_999_999i64;
        let arg = abs_secs.to_string();
        let (ttl, _) = parse_ttl_options_raw(&[s(b"EXAT"), arg.as_bytes()], "CMD").unwrap();
        assert_eq!(ttl, Some(abs_secs * 1000));
    }

    #[test]
    fn parse_ttl_pxat_absolute_ms() {
        let abs_ms = 9_999_999_999_000i64;
        let arg = abs_ms.to_string();
        let (ttl, _) = parse_ttl_options_raw(&[s(b"PXAT"), arg.as_bytes()], "CMD").unwrap();
        assert_eq!(ttl, Some(abs_ms));
    }

    #[test]
    fn parse_ttl_ex_zero_is_error() {
        assert!(parse_ttl_options_raw(&[s(b"EX"), s(b"0")], "CMD").is_err());
    }

    #[test]
    fn parse_ttl_keepttl_and_ex_is_error() {
        assert!(parse_ttl_options_raw(&[s(b"KEEPTTL"), s(b"EX"), s(b"10")], "CMD").is_err());
    }

    #[test]
    fn resolve_range_full() {
        assert_eq!(resolve_range(0, -1, 5), (0, 5));
    }

    #[test]
    fn resolve_range_partial() {
        assert_eq!(resolve_range(1, 3, 5), (1, 4));
    }

    #[test]
    fn resolve_range_negative_start() {
        assert_eq!(resolve_range(-3, -1, 5), (2, 5));
    }

    #[test]
    fn resolve_range_empty_when_start_exceeds_stop() {
        assert_eq!(resolve_range(3, 1, 5), (0, 0));
    }

    #[test]
    fn resolve_range_empty_list() {
        assert_eq!(resolve_range(0, -1, 0), (0, 0));
    }

    #[test]
    fn resolve_index_positive() {
        assert_eq!(resolve_index(2, 5), Some(2));
    }

    #[test]
    fn resolve_index_negative() {
        assert_eq!(resolve_index(-1, 5), Some(4));
        assert_eq!(resolve_index(-5, 5), Some(0));
    }

    #[test]
    fn resolve_index_out_of_range() {
        assert_eq!(resolve_index(5, 5), None);
        assert_eq!(resolve_index(-6, 5), None);
    }
}
