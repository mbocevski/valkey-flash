use std::time::{Duration, SystemTime, UNIX_EPOCH};
use valkey_module::{Context, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

use crate::CACHE;
use crate::types::Tier;
use crate::types::string::{FLASH_STRING_TYPE, FlashStringObject};

// ── SetCompletionHandle ───────────────────────────────────────────────────────

/// Unblocks the waiting client with +OK on storage success or an error string on failure.
///
/// Only compiled outside test mode: `ThreadSafeContext` and `BlockedClient` require a running
/// Valkey server to be initialised.
#[cfg(not(test))]
pub struct SetCompletionHandle {
    tsc: valkey_module::ThreadSafeContext<valkey_module::BlockedClient<()>>,
}

#[cfg(not(test))]
impl SetCompletionHandle {
    pub fn new(bc: valkey_module::BlockedClient<()>) -> Self {
        SetCompletionHandle {
            tsc: valkey_module::ThreadSafeContext::with_blocked_client(bc),
        }
    }
}

#[cfg(not(test))]
impl crate::async_io::CompletionHandle for SetCompletionHandle {
    fn complete(self: Box<Self>, result: crate::storage::backend::StorageResult<Vec<u8>>) {
        let reply = match result {
            Ok(_) => Ok(ValkeyValue::SimpleStringStatic("OK")),
            Err(e) => Err(ValkeyError::String(e.to_string())),
        };
        // catch_unwind guards against a panic inside tsc.reply() killing the worker thread.
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.tsc.reply(reply);
        }));
    }
}

// ── Arg parsing ───────────────────────────────────────────────────────────────

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

/// Parse optional TTL and NX/XX args from raw byte slices (testable without server context).
///
/// Returns `(ttl_abs_ms, nx, xx)` where `ttl_abs_ms` is an absolute Unix-epoch millisecond
/// timestamp derived from the caller's local clock.
fn parse_options_raw(args: &[&[u8]]) -> Result<(Option<i64>, bool, bool), ValkeyError> {
    let mut ttl_abs_ms: Option<i64> = None;
    let mut nx = false;
    let mut xx = false;
    let mut i = 0;

    while i < args.len() {
        let upper = args[i].to_ascii_uppercase();
        match upper.as_slice() {
            b"EX" => {
                i += 1;
                if i >= args.len() {
                    return Err(ValkeyError::Str("ERR syntax error"));
                }
                let secs = parse_positive_bytes(args[i], "FLASH.SET")?;
                let rel_ms = secs
                    .checked_mul(1000)
                    .ok_or(ValkeyError::Str("ERR invalid expire time in FLASH.SET"))?;
                ttl_abs_ms = Some(current_time_ms() + rel_ms);
            }
            b"PX" => {
                i += 1;
                if i >= args.len() {
                    return Err(ValkeyError::Str("ERR syntax error"));
                }
                let ms = parse_positive_bytes(args[i], "FLASH.SET")?;
                ttl_abs_ms = Some(current_time_ms() + ms);
            }
            b"EXAT" => {
                i += 1;
                if i >= args.len() {
                    return Err(ValkeyError::Str("ERR syntax error"));
                }
                let secs = parse_positive_bytes(args[i], "FLASH.SET")?;
                let abs_ms = secs
                    .checked_mul(1000)
                    .ok_or(ValkeyError::Str("ERR invalid expire time in FLASH.SET"))?;
                ttl_abs_ms = Some(abs_ms);
            }
            b"PXAT" => {
                i += 1;
                if i >= args.len() {
                    return Err(ValkeyError::Str("ERR syntax error"));
                }
                let ms = parse_positive_bytes(args[i], "FLASH.SET")?;
                ttl_abs_ms = Some(ms);
            }
            b"NX" => {
                nx = true;
            }
            b"XX" => {
                xx = true;
            }
            _ => {
                return Err(ValkeyError::Str("ERR syntax error"));
            }
        }
        i += 1;
    }

    if nx && xx {
        return Err(ValkeyError::Str(
            "ERR XX and NX options at the same time are not compatible",
        ));
    }
    Ok((ttl_abs_ms, nx, xx))
}

fn parse_options(args: &[ValkeyString]) -> Result<(Option<i64>, bool, bool), ValkeyError> {
    let raw: Vec<&[u8]> = args.iter().map(|vs| vs.as_slice()).collect();
    parse_options_raw(&raw)
}

// ── Command handler ───────────────────────────────────────────────────────────

/// `FLASH.SET key value [EX s | PX ms | EXAT unix-s | PXAT unix-ms] [NX | XX]`
///
/// Write-through strategy:
/// 1. Parse and validate args.
/// 2. Type-check the key (WRONGTYPE if it holds a non-flash-string type).
/// 3. Honour NX / XX: return nil without writing if the condition is not met.
/// 4. Store `FlashStringObject` in Valkey's keyspace and set native TTL.
/// 5. Insert value bytes into the in-memory hot cache.
/// 6. Replicate the full command verbatim so replicas converge byte-identically.
/// 7. Block the client and submit an async NVMe write; reply +OK when it completes.
pub fn flash_set_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 3 {
        return Err(ValkeyError::WrongArity);
    }

    let key = &args[1];
    let value = args[2].as_slice().to_vec();
    let (ttl_abs_ms, nx, xx) = parse_options(&args[3..])?;

    // Type-check: Err means key exists with a different (non-flash-string) type.
    let key_handle = ctx.open_key_writable(key);
    let existing = match key_handle.get_value::<FlashStringObject>(&FLASH_STRING_TYPE) {
        Err(_) => return Err(ValkeyError::WrongType),
        Ok(v) => v,
    };
    let key_exists = existing.is_some();

    if nx && key_exists {
        return Ok(ValkeyValue::Null);
    }
    if xx && !key_exists {
        return Ok(ValkeyValue::Null);
    }

    // Store FlashStringObject in Valkey's keyspace.
    key_handle
        .set_value(
            &FLASH_STRING_TYPE,
            FlashStringObject {
                tier: Tier::Hot(value.clone()),
                ttl_ms: ttl_abs_ms,
            },
        )
        .map_err(|e| ValkeyError::String(format!("flash: set_value: {e}")))?;

    // Register native TTL so Valkey's expiry loop can clean up the key.
    if let Some(abs_ms) = ttl_abs_ms {
        let remaining_ms = (abs_ms - current_time_ms()).max(1) as u64;
        key_handle
            .set_expire(Duration::from_millis(remaining_ms))
            .map_err(|e| ValkeyError::String(format!("flash: set_expire: {e}")))?;
    }

    let cache = CACHE
        .get()
        .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;

    // Hot-cache put: the value is immediately readable from RAM.
    cache.put(key.as_slice(), value.clone());

    // Replicate before blocking — must execute on the event-loop thread.
    ctx.replicate_verbatim();
    ctx.notify_keyspace_event(NotifyEvent::GENERIC, "flash.set", key);

    // Block the client and dispatch the NVMe write asynchronously.
    // submit_or_complete guarantees complete() is called even when the pool is full.
    #[cfg(not(test))]
    {
        if crate::replication::must_run_sync(ctx)
            || (crate::STORAGE.get().is_none() && crate::replication::must_obey_client(ctx))
        {
            return Ok(ValkeyValue::SimpleStringStatic("OK"));
        }
        use crate::storage::backend::StorageBackend;
        let storage = crate::STORAGE
            .get()
            .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;
        let pool = crate::POOL
            .get()
            .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;
        let bc = ctx.block_client();
        let handle = Box::new(SetCompletionHandle::new(bc));
        let key_bytes = key.as_slice().to_vec();
        pool.submit_or_complete(handle, move || {
            let offset = storage.put(&key_bytes, &value)?;
            if let Some(wal) = crate::WAL.get() {
                let kh = crate::util::key_hash(&key_bytes);
                let vh = crate::util::value_hash(&value);
                if let Err(e) = wal.append(crate::storage::wal::WalOp::Put {
                    key_hash: kh,
                    offset,
                    value_hash: vh,
                }) {
                    valkey_module::logging::log_warning(
                        format!("flash: SET WAL append failed: {e}").as_str(),
                    );
                }
            }
            Ok(vec![])
        });
        return Ok(ValkeyValue::NoReply);
    }

    // In test builds the server runtime is absent; return OK directly.
    #[allow(unreachable_code)]
    Ok(ValkeyValue::SimpleStringStatic("OK"))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_options_empty_is_no_ttl_no_cond() {
        let (ttl, nx, xx) = parse_options_raw(&[]).unwrap();
        assert!(ttl.is_none());
        assert!(!nx);
        assert!(!xx);
    }

    #[test]
    fn parse_options_nx() {
        let (_, nx, xx) = parse_options_raw(&[b"NX"]).unwrap();
        assert!(nx);
        assert!(!xx);
    }

    #[test]
    fn parse_options_xx() {
        let (_, nx, xx) = parse_options_raw(&[b"XX"]).unwrap();
        assert!(!nx);
        assert!(xx);
    }

    #[test]
    fn parse_options_nx_and_xx_is_error() {
        assert!(parse_options_raw(&[b"NX", b"XX"]).is_err());
    }

    #[test]
    fn parse_options_ex_sets_ttl() {
        let before = current_time_ms();
        let (ttl, _, _) = parse_options_raw(&[b"EX", b"10"]).unwrap();
        let after = current_time_ms();
        let abs_ms = ttl.unwrap();
        // Should be current_time + 10_000ms; allow ±100ms for clock skew.
        assert!(abs_ms >= before + 9_900 && abs_ms <= after + 10_100);
    }

    #[test]
    fn parse_options_px_sets_ttl() {
        let before = current_time_ms();
        let (ttl, _, _) = parse_options_raw(&[b"PX", b"5000"]).unwrap();
        let after = current_time_ms();
        let abs_ms = ttl.unwrap();
        assert!(abs_ms >= before + 4_900 && abs_ms <= after + 5_100);
    }

    #[test]
    fn parse_options_ex_zero_is_error() {
        assert!(parse_options_raw(&[b"EX", b"0"]).is_err());
    }

    #[test]
    fn parse_options_px_negative_is_error() {
        assert!(parse_options_raw(&[b"PX", b"-1"]).is_err());
    }

    #[test]
    fn parse_options_unknown_keyword_is_error() {
        assert!(parse_options_raw(&[b"KEEPTTL"]).is_err());
    }

    #[test]
    fn parse_options_ex_missing_value_is_error() {
        assert!(parse_options_raw(&[b"EX"]).is_err());
    }

    #[test]
    fn parse_options_pxat_stores_absolute_ms() {
        let (ttl, _, _) = parse_options_raw(&[b"PXAT", b"9999999999000"]).unwrap();
        assert_eq!(ttl, Some(9_999_999_999_000));
    }

    #[test]
    fn parse_options_exat_converts_to_ms() {
        let (ttl, _, _) = parse_options_raw(&[b"EXAT", b"9999999999"]).unwrap();
        assert_eq!(ttl, Some(9_999_999_999_000));
    }

    #[test]
    fn parse_options_case_insensitive() {
        let (_, nx, _) = parse_options_raw(&[b"nx"]).unwrap();
        assert!(nx);
    }
}
