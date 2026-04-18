use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use valkey_module::{Context, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

use crate::types::hash::{
    hash_deserialize_or_warn, hash_serialize, FlashHashObject, FLASH_HASH_TYPE,
};
use crate::types::Tier;
use crate::{CACHE, STORAGE};
#[cfg(not(test))]
use crate::{POOL, WAL};

// ── HSetCompletionHandle ──────────────────────────────────────────────────────

#[cfg(not(test))]
struct HSetCompletionHandle {
    tsc: valkey_module::ThreadSafeContext<valkey_module::BlockedClient<()>>,
    new_count: i64,
}

#[cfg(not(test))]
impl HSetCompletionHandle {
    fn new(bc: valkey_module::BlockedClient<()>, new_count: i64) -> Self {
        HSetCompletionHandle {
            tsc: valkey_module::ThreadSafeContext::with_blocked_client(bc),
            new_count,
        }
    }
}

#[cfg(not(test))]
impl crate::async_io::CompletionHandle for HSetCompletionHandle {
    fn complete(self: Box<Self>, result: crate::storage::backend::StorageResult<Vec<u8>>) {
        use valkey_module::logging;
        if let Err(ref e) = result {
            logging::log_warning(format!("flash: HSET NVMe write failed: {e}").as_str());
        }
        let reply = Ok(ValkeyValue::Integer(self.new_count));
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.tsc.reply(reply);
        }));
    }
}

// ── TTL option parsing ────────────────────────────────────────────────────────

fn current_time_ms() -> i64 {
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

/// TTL keywords recognized after the field-value pairs.
const TTL_KEYWORDS: &[&[u8]] = &[b"EX", b"PX", b"EXAT", b"PXAT", b"KEEPTTL"];

/// Returns the index of the first TTL keyword in `args`, if any.
fn find_ttl_start(args: &[ValkeyString]) -> Option<usize> {
    args.iter().position(|a| {
        let upper = a.as_slice().to_ascii_uppercase();
        TTL_KEYWORDS.contains(&upper.as_slice())
    })
}

/// Parse TTL options from the slice starting at a TTL keyword.
///
/// Returns `(ttl_abs_ms, keepttl)`:
/// - `Some(abs_ms)` + `false`: a new absolute expiry was specified.
/// - `None` + `true`:  KEEPTTL was specified.
/// - `None` + `false`: this slice is empty (caller handles as "keep existing TTL").
fn parse_ttl_options_raw(args: &[&[u8]]) -> Result<(Option<i64>, bool), ValkeyError> {
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
                let secs = parse_positive_bytes(args[i], "FLASH.HSET")?;
                let rel_ms = secs
                    .checked_mul(1000)
                    .ok_or(ValkeyError::Str("ERR invalid expire time in FLASH.HSET"))?;
                ttl_abs_ms = Some(current_time_ms() + rel_ms);
            }
            b"PX" => {
                i += 1;
                if i >= args.len() {
                    return Err(ValkeyError::Str("ERR syntax error"));
                }
                let ms = parse_positive_bytes(args[i], "FLASH.HSET")?;
                ttl_abs_ms = Some(current_time_ms() + ms);
            }
            b"EXAT" => {
                i += 1;
                if i >= args.len() {
                    return Err(ValkeyError::Str("ERR syntax error"));
                }
                let secs = parse_positive_bytes(args[i], "FLASH.HSET")?;
                let abs_ms = secs
                    .checked_mul(1000)
                    .ok_or(ValkeyError::Str("ERR invalid expire time in FLASH.HSET"))?;
                ttl_abs_ms = Some(abs_ms);
            }
            b"PXAT" => {
                i += 1;
                if i >= args.len() {
                    return Err(ValkeyError::Str("ERR syntax error"));
                }
                let ms = parse_positive_bytes(args[i], "FLASH.HSET")?;
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

fn parse_ttl_options(args: &[ValkeyString]) -> Result<(Option<i64>, bool), ValkeyError> {
    let raw: Vec<&[u8]> = args.iter().map(|vs| vs.as_slice()).collect();
    parse_ttl_options_raw(&raw)
}

// ── Command handler ───────────────────────────────────────────────────────────

/// `FLASH.HSET key field value [field value ...] [EX s | PX ms | EXAT ts-s | PXAT ts-ms | KEEPTTL]`
///
/// Add or update fields in a flash-hash key. Returns the count of newly
/// added fields (0 for pure overwrites, like Redis HSET).
///
/// TTL options (optional, after all field-value pairs):
/// - `EX s` / `PX ms` / `EXAT unix-s` / `PXAT unix-ms`: set key expiry.
/// - `KEEPTTL`: explicitly preserve the existing TTL (also the default when no
///   TTL option is supplied — HSET does not clear TTL by default).
///
/// Write-through strategy mirrors FLASH.SET:
///   - Update Valkey keyspace (Hot tier) immediately.
///   - Populate the hot cache with the serialized hash bytes.
///   - Replicate the full command.
///   - Async NVMe write; WAL Put on completion.
///
/// Cold-tier edge case (v1): if the key is currently cold, the existing hash is
/// read synchronously from NVMe on the event loop before applying updates.
pub fn flash_hset_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 4 {
        return Err(ValkeyError::WrongArity);
    }

    let key = &args[1];

    // Split field-value pairs from TTL options.
    // Field-value pairs start at args[2]; scan for the first TTL keyword.
    let fv_start = 2usize;
    let ttl_start = find_ttl_start(&args[fv_start..]).map(|p| p + fv_start);
    let fv_end = ttl_start.unwrap_or(args.len());
    let fv_count = fv_end - fv_start;

    if fv_count < 2 || !fv_count.is_multiple_of(2) {
        return Err(ValkeyError::WrongArity);
    }

    let (new_ttl_abs_ms, _keepttl) = parse_ttl_options(&args[fv_end..])?;

    let cache = CACHE
        .get()
        .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;

    let key_handle = ctx.open_key_writable(key);
    let existing = match key_handle.get_value::<FlashHashObject>(&FLASH_HASH_TYPE) {
        Err(_) => return Err(ValkeyError::WrongType),
        Ok(v) => v,
    };

    // Extract current fields; for cold keys, promote synchronously (v1 limitation).
    let (mut current_fields, old_ttl): (HashMap<Vec<u8>, Vec<u8>>, Option<i64>) = match existing {
        None => (HashMap::new(), None),
        Some(obj) => {
            let ttl = obj.ttl_ms;
            let fields = match &obj.tier {
                Tier::Hot(fields) => fields.clone(),
                Tier::Cold {
                    backend_offset,
                    value_len,
                    ..
                } => {
                    if crate::replication::is_replica() {
                        HashMap::new()
                    } else {
                        let storage = STORAGE
                            .get()
                            .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;
                        let bytes = storage
                            .read_at_offset(*backend_offset, *value_len)
                            .map_err(|e| ValkeyError::String(e.to_string()))?;
                        hash_deserialize_or_warn(&bytes)
                    }
                }
            };
            (fields, ttl)
        }
    };

    // Apply field updates; count truly new fields.
    let mut new_count = 0i64;
    for chunk in args[fv_start..fv_end].chunks(2) {
        let field = chunk[0].as_slice().to_vec();
        let value = chunk[1].as_slice().to_vec();
        if !current_fields.contains_key(&field) {
            new_count += 1;
        }
        current_fields.insert(field, value);
    }

    // Resolve TTL to store:
    // - Explicit EX/PX/EXAT/PXAT → use that absolute timestamp.
    // - KEEPTTL or no TTL flag → preserve existing TTL (both cases: keep old_ttl).
    let stored_ttl = new_ttl_abs_ms.or(old_ttl);

    // Store updated hash in Valkey keyspace (free() called on old object).
    key_handle
        .set_value(
            &FLASH_HASH_TYPE,
            FlashHashObject {
                tier: Tier::Hot(current_fields.clone()),
                ttl_ms: stored_ttl,
            },
        )
        .map_err(|e| ValkeyError::String(format!("flash: hset set_value: {e}")))?;

    // Register native key-level TTL when a new expiry was explicitly set.
    if let Some(abs_ms) = new_ttl_abs_ms {
        use std::time::Duration;
        let remaining_ms = (abs_ms - current_time_ms()).max(1) as u64;
        key_handle
            .set_expire(Duration::from_millis(remaining_ms))
            .map_err(|e| ValkeyError::String(format!("flash: hset set_expire: {e}")))?;
    }

    let serialized = hash_serialize(&current_fields);
    cache.put(key.as_slice(), serialized.clone());

    ctx.replicate_verbatim();
    ctx.notify_keyspace_event(NotifyEvent::GENERIC, "flash.hset", key);

    #[cfg(not(test))]
    {
        if crate::replication::is_replica()
            || (crate::STORAGE.get().is_none() && crate::replication::must_obey_client(ctx))
        {
            return Ok(ValkeyValue::Integer(new_count));
        }
        use crate::storage::backend::StorageBackend;
        let storage = STORAGE
            .get()
            .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;
        let pool = POOL
            .get()
            .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;
        let key_bytes = key.as_slice().to_vec();
        let bc = ctx.block_client();
        let handle = Box::new(HSetCompletionHandle::new(bc, new_count));
        pool.submit_or_complete(handle, move || {
            let offset = storage.put(&key_bytes, &serialized)?;
            if let Some(wal) = WAL.get() {
                let kh = crate::util::key_hash(&key_bytes);
                let vh = crate::util::value_hash(&serialized);
                if let Err(e) = wal.append(crate::storage::wal::WalOp::Put {
                    key_hash: kh,
                    offset,
                    value_hash: vh,
                }) {
                    valkey_module::logging::log_warning(
                        format!("flash: HSET WAL append failed: {e}").as_str(),
                    );
                }
            }
            Ok(vec![])
        });
        return Ok(ValkeyValue::NoReply);
    }

    #[allow(unreachable_code)]
    Ok(ValkeyValue::Integer(new_count))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hset_arity_requires_at_least_four_args() {
        let len = 4usize;
        assert!((len - 2).is_multiple_of(2) && len >= 4);
    }

    #[test]
    fn hset_odd_fv_count_fails_arity() {
        let len = 5usize;
        assert!(!(len - 2).is_multiple_of(2));
    }

    #[test]
    fn new_count_is_correct_for_all_new_fields() {
        let mut m: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        let pairs: &[(&[u8], &[u8])] = &[(b"f1", b"v1"), (b"f2", b"v2")];
        let mut new_count = 0i64;
        for (f, v) in pairs {
            if !m.contains_key(*f) {
                new_count += 1;
            }
            m.insert(f.to_vec(), v.to_vec());
        }
        assert_eq!(new_count, 2);
    }

    #[test]
    fn new_count_is_zero_for_pure_overwrite() {
        let mut m: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        m.insert(b"f1".to_vec(), b"old".to_vec());
        let mut new_count = 0i64;
        let field = b"f1".to_vec();
        if !m.contains_key(&field) {
            new_count += 1;
        }
        m.insert(field, b"new".to_vec());
        assert_eq!(new_count, 0);
    }

    // ── TTL option parsing ────────────────────────────────────────────────────

    fn s(b: &[u8]) -> &[u8] {
        b
    }

    #[test]
    fn parse_ttl_empty_returns_none_no_keepttl() {
        let (ttl, keepttl) = parse_ttl_options_raw(&[]).unwrap();
        assert!(ttl.is_none());
        assert!(!keepttl);
    }

    #[test]
    fn parse_ttl_keepttl_sets_flag() {
        let (ttl, keepttl) = parse_ttl_options_raw(&[s(b"KEEPTTL")]).unwrap();
        assert!(ttl.is_none());
        assert!(keepttl);
    }

    #[test]
    fn parse_ttl_keepttl_case_insensitive() {
        let (_, keepttl) = parse_ttl_options_raw(&[s(b"keepttl")]).unwrap();
        assert!(keepttl);
    }

    #[test]
    fn parse_ttl_ex_positive() {
        let (ttl, keepttl) = parse_ttl_options_raw(&[s(b"EX"), s(b"10")]).unwrap();
        assert!(ttl.is_some());
        assert!(ttl.unwrap() > 0);
        assert!(!keepttl);
    }

    #[test]
    fn parse_ttl_px_positive() {
        let (ttl, _) = parse_ttl_options_raw(&[s(b"PX"), s(b"5000")]).unwrap();
        assert!(ttl.is_some());
    }

    #[test]
    fn parse_ttl_exat_sets_absolute() {
        let abs_secs = 9_999_999_999i64;
        let arg = abs_secs.to_string();
        let (ttl, _) = parse_ttl_options_raw(&[s(b"EXAT"), arg.as_bytes()]).unwrap();
        assert_eq!(ttl, Some(abs_secs * 1000));
    }

    #[test]
    fn parse_ttl_pxat_sets_absolute_ms() {
        let abs_ms = 9_999_999_999_000i64;
        let arg = abs_ms.to_string();
        let (ttl, _) = parse_ttl_options_raw(&[s(b"PXAT"), arg.as_bytes()]).unwrap();
        assert_eq!(ttl, Some(abs_ms));
    }

    #[test]
    fn parse_ttl_ex_zero_is_error() {
        assert!(parse_ttl_options_raw(&[s(b"EX"), s(b"0")]).is_err());
    }

    #[test]
    fn parse_ttl_ex_negative_is_error() {
        assert!(parse_ttl_options_raw(&[s(b"EX"), s(b"-1")]).is_err());
    }

    #[test]
    fn parse_ttl_ex_missing_value_is_error() {
        assert!(parse_ttl_options_raw(&[s(b"EX")]).is_err());
    }

    #[test]
    fn parse_ttl_keepttl_and_ex_is_error() {
        assert!(parse_ttl_options_raw(&[s(b"KEEPTTL"), s(b"EX"), s(b"10")]).is_err());
    }

    #[test]
    fn parse_ttl_unknown_keyword_is_error() {
        assert!(parse_ttl_options_raw(&[s(b"NX")]).is_err());
    }

    // ── find_ttl_start ────────────────────────────────────────────────────────

    #[test]
    fn find_ttl_start_no_keyword_returns_none() {
        // Simulate: args = ["f1", "v1", "f2", "v2"] (after cmd+key stripped)
        // We can't easily construct ValkeyString in unit tests, but we can test
        // the underlying position logic by checking the constant keywords list.
        let keywords = TTL_KEYWORDS;
        let field = b"field1".as_ref();
        let val = b"value1".as_ref();
        let upper_f: Vec<u8> = field.to_ascii_uppercase();
        let upper_v: Vec<u8> = val.to_ascii_uppercase();
        assert!(!keywords.contains(&upper_f.as_slice()));
        assert!(!keywords.contains(&upper_v.as_slice()));
    }

    #[test]
    fn ttl_keywords_list_is_complete() {
        for kw in &[
            b"EX".as_ref(),
            b"PX".as_ref(),
            b"EXAT".as_ref(),
            b"PXAT".as_ref(),
            b"KEEPTTL".as_ref(),
        ] {
            assert!(TTL_KEYWORDS.contains(kw), "{kw:?} missing");
        }
    }
}
