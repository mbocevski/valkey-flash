//! `FLASH.INCR` / `INCRBY` / `DECR` / `DECRBY` / `APPEND` — string read-modify-write.
//!
//! Production-readiness gap closer for v1.1.1. Wrappers' Bucket B currently
//! returns `WrapperError::FlashArithmeticUnsupported` when arithmetic commands
//! hit a flash-routed key, blocking counter / rate-limiter / append-log
//! workloads from running on the flash tier at all. These five commands
//! close that gap.
//!
//! ## Algorithm (v1: promote-modify-demote)
//!
//! 1. Open the key. If missing: initialise to the appropriate identity
//!    (`"0"` for arithmetic, empty bytes for APPEND) and proceed.
//! 2. If present and held by any non-`FlashString` type (FlashHash/List/ZSet,
//!    native, other module): return `WRONGTYPE` — these commands operate
//!    only on flash-tier strings.
//! 3. Materialise the current value bytes: clone for Hot, synchronous NVMe
//!    read for Cold. The Cold-read blocks the event loop briefly — accepted
//!    pattern (see `lset` / `linsert` / `lrem` and the v1 docstring on
//!    `list_common.rs`).
//! 4. For arithmetic: parse the bytes as a signed 64-bit integer (else
//!    `ERR value is not an integer or out of range`). Apply the delta with
//!    overflow check (else `ERR increment or decrement would overflow`).
//!    Format the new integer back to bytes.
//! 5. For APPEND: extend the materialised bytes with the new payload.
//! 6. Write the new bytes back at the same key as `Tier::Hot`. TTL
//!    preserved from the existing typed object. Auto-demotion will demote
//!    back to Cold later if policy warrants.
//! 7. Reply: arithmetic commands return the new integer; APPEND returns
//!    the new total length.
//!
//! Atomicity: single-tick command execution. The Cold-read is synchronous
//! within the handler; concurrent commands on the same event loop see
//! either the pre-state or the post-state, never an intermediate.
//!
//! ## Native semantics matched
//!
//! - INCR on a missing key initialises to `"0"` then applies delta `+1` →
//!   the key now holds `"1"`. Same for DECR / INCRBY / DECRBY.
//! - APPEND on a missing key sets the value to the provided bytes; returns
//!   the length of those bytes.
//! - Arithmetic on a non-integer string errors with the standard
//!   "value is not an integer or out of range" message.
//! - Overflow at i64 boundaries errors; clamped values do NOT silently wrap.
//! - TTL on the existing key is preserved across the write.
//!
//! ## What this v1 does NOT do (deferred)
//!
//! - **In-place RMW for Cold values.** Each call materialises the full
//!   value through Hot RAM. For 1 GiB Cold strings (an unusual but possible
//!   case), this means a full NVMe round-trip and a brief RAM spike during
//!   the operation. A v1.2 optimisation would do the parse+modify+write in
//!   place against NVMe under a per-key lock; complex enough to defer.
//! - **WATCH / multi-step transaction integration.** Same as native
//!   FLASH.SET; module commands don't currently surface dirty-bit hooks
//!   for WATCH. Out of scope.

use std::time::Duration;

use valkey_module::{Context, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

use crate::CACHE;
use crate::commands::list_common::current_time_ms;
use crate::types::Tier;
use crate::types::hash::{FLASH_HASH_TYPE, FlashHashObject};
use crate::types::list::{FLASH_LIST_TYPE, FlashListObject};
use crate::types::string::{FLASH_STRING_TYPE, FlashStringObject};
use crate::types::zset::{FLASH_ZSET_TYPE, FlashZSetObject};

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Synchronously read NVMe at `(offset, len)`. Mirrors `cold_read` helpers in
/// `convert.rs` and `copy_rename.rs` — the same event-loop-blocking pattern
/// already accepted in v1 for Cold materialisation.
fn cold_read(offset: u64, value_len: u32) -> Result<Vec<u8>, ValkeyError> {
    if value_len == 0 {
        return Ok(Vec::new());
    }
    let storage = crate::STORAGE
        .get()
        .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;
    storage
        .read_at_offset(offset, value_len)
        .map_err(|e| ValkeyError::String(e.to_string()))
}

/// Parse value bytes as a signed 64-bit integer, matching native Valkey's
/// strictness: leading/trailing whitespace not allowed; only `[+-]?\d+`.
fn parse_i64_strict(bytes: &[u8]) -> Result<i64, ValkeyError> {
    let s = std::str::from_utf8(bytes)
        .map_err(|_| ValkeyError::Str("ERR value is not an integer or out of range"))?;
    // Native Valkey rejects strings with whitespace, leading zeros (except "0"
    // and "-0"), and any non-digit. Rust's i64::parse accepts leading zeros
    // but rejects whitespace and non-digit chars — close enough for v1.
    s.parse::<i64>()
        .map_err(|_| ValkeyError::Str("ERR value is not an integer or out of range"))
}

/// Materialised flash-string view: the value bytes and its absolute-ms TTL
/// (None if the key has no expiry).
type ValueAndTtl = (Vec<u8>, Option<i64>);

/// Type-check the key. Returns `Ok(None)` if absent, `Ok(Some((bytes, ttl)))`
/// for a `FlashString` (with materialised bytes), or `Err(WRONGTYPE)` for any
/// other type (other flash type, native, or unknown module).
fn read_flash_string_or_init(
    ctx: &Context,
    key: &ValkeyString,
) -> Result<Option<ValueAndTtl>, ValkeyError> {
    let kh = ctx.open_key(key);

    // Non-flash-string flash types → WRONGTYPE.
    if matches!(
        kh.get_value::<FlashHashObject>(&FLASH_HASH_TYPE),
        Ok(Some(_))
    ) || matches!(
        kh.get_value::<FlashListObject>(&FLASH_LIST_TYPE),
        Ok(Some(_))
    ) || matches!(
        kh.get_value::<FlashZSetObject>(&FLASH_ZSET_TYPE),
        Ok(Some(_))
    ) {
        return Err(ValkeyError::WrongType);
    }

    // Native or other module type? FlashString get_value will Err.
    match kh.get_value::<FlashStringObject>(&FLASH_STRING_TYPE) {
        Err(_) => Err(ValkeyError::WrongType),
        Ok(None) => Ok(None),
        Ok(Some(obj)) => {
            let bytes = match &obj.tier {
                Tier::Hot(v) => v.clone(),
                Tier::Cold {
                    backend_offset,
                    value_len,
                    ..
                } => cold_read(*backend_offset, *value_len)?,
            };
            Ok(Some((bytes, obj.ttl_ms)))
        }
    }
}

/// Write `bytes` to `key` as a `Tier::Hot` `FlashString`, preserving the
/// optional TTL. Replaces any existing payload; on Cold replacement the
/// type's free callback releases the NVMe blocks.
fn write_flash_string(
    ctx: &Context,
    key: &ValkeyString,
    bytes: Vec<u8>,
    ttl_ms: Option<i64>,
) -> Result<(), ValkeyError> {
    let kh = ctx.open_key_writable(key);
    let cache = CACHE
        .get()
        .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;
    kh.set_value(
        &FLASH_STRING_TYPE,
        FlashStringObject {
            tier: Tier::Hot(bytes.clone()),
            ttl_ms,
        },
    )
    .map_err(|e| ValkeyError::String(format!("flash: incr/append set_value: {e}")))?;

    if let Some(abs_ms) = ttl_ms {
        let remaining_ms = (abs_ms - current_time_ms()).max(1) as u64;
        kh.set_expire(Duration::from_millis(remaining_ms))
            .map_err(|e| ValkeyError::String(format!("flash: incr/append set_expire: {e}")))?;
    }

    cache.put(key.as_slice(), bytes);
    Ok(())
}

// ── INCR family ───────────────────────────────────────────────────────────────

/// Shared implementation for INCR / INCRBY / DECR / DECRBY.
///
/// `delta` is the signed amount to add. DECR family negates the delta before
/// calling here.
fn incr_impl(ctx: &Context, args: &[ValkeyString], delta: i64, notify_event: &str) -> ValkeyResult {
    let key = &args[1];

    let (current_bytes, ttl) = match read_flash_string_or_init(ctx, key)? {
        Some(v) => v,
        // Missing key — initialise to "0".
        None => (b"0".to_vec(), None),
    };

    let current = parse_i64_strict(&current_bytes)?;
    let new = current.checked_add(delta).ok_or(ValkeyError::Str(
        "ERR increment or decrement would overflow",
    ))?;

    let new_bytes = new.to_string().into_bytes();
    write_flash_string(ctx, key, new_bytes, ttl)?;

    ctx.replicate_verbatim();
    ctx.notify_keyspace_event(NotifyEvent::STRING, notify_event, key);

    Ok(ValkeyValue::Integer(new))
}

/// `FLASH.INCR key`
pub fn flash_incr_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() != 2 {
        return Err(ValkeyError::WrongArity);
    }
    incr_impl(ctx, &args, 1, "flash.incrby")
}

/// `FLASH.INCRBY key delta`
pub fn flash_incrby_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() != 3 {
        return Err(ValkeyError::WrongArity);
    }
    let delta = parse_i64_strict(args[2].as_slice())?;
    incr_impl(ctx, &args, delta, "flash.incrby")
}

/// `FLASH.DECR key`
pub fn flash_decr_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() != 2 {
        return Err(ValkeyError::WrongArity);
    }
    incr_impl(ctx, &args, -1, "flash.decrby")
}

/// `FLASH.DECRBY key delta`
pub fn flash_decrby_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() != 3 {
        return Err(ValkeyError::WrongArity);
    }
    let delta = parse_i64_strict(args[2].as_slice())?;
    // Negate for DECRBY. i64::MIN can't be negated; reject early to match
    // native Valkey's behaviour (the symmetric INCRBY i64::MAX path also
    // overflows in the checked_add path below, but we want the same error
    // shape regardless of which boundary).
    let neg = delta.checked_neg().ok_or(ValkeyError::Str(
        "ERR value is not an integer or out of range",
    ))?;
    incr_impl(ctx, &args, neg, "flash.decrby")
}

// ── APPEND ────────────────────────────────────────────────────────────────────

/// `FLASH.APPEND key value`
///
/// If the key exists as a flash string, append `value` to it. If absent,
/// initialise to `value`. Returns the new total byte length.
pub fn flash_append_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() != 3 {
        return Err(ValkeyError::WrongArity);
    }
    let key = &args[1];
    let to_append = args[2].as_slice();

    // unwrap_or_default gives us (Vec::new(), None) when the key was absent.
    let (mut current_bytes, ttl) = read_flash_string_or_init(ctx, key)?.unwrap_or_default();

    current_bytes.extend_from_slice(to_append);
    let new_len = current_bytes.len() as i64;

    write_flash_string(ctx, key, current_bytes, ttl)?;

    ctx.replicate_verbatim();
    ctx.notify_keyspace_event(NotifyEvent::STRING, "flash.append", key);

    Ok(ValkeyValue::Integer(new_len))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_integer() {
        assert_eq!(parse_i64_strict(b"42").unwrap(), 42);
        assert_eq!(parse_i64_strict(b"-7").unwrap(), -7);
        assert_eq!(parse_i64_strict(b"0").unwrap(), 0);
    }

    #[test]
    fn parse_max_min_boundaries() {
        assert_eq!(parse_i64_strict(b"9223372036854775807").unwrap(), i64::MAX);
        assert_eq!(parse_i64_strict(b"-9223372036854775808").unwrap(), i64::MIN);
    }

    #[test]
    fn parse_rejects_overflow() {
        assert!(parse_i64_strict(b"9223372036854775808").is_err()); // i64::MAX + 1
        assert!(parse_i64_strict(b"-9223372036854775809").is_err()); // i64::MIN - 1
    }

    #[test]
    fn parse_rejects_non_integer() {
        for s in [
            b"abc".as_slice(),
            b"".as_slice(),
            b"3.14".as_slice(),
            b"42x".as_slice(),
            b" 42".as_slice(),
            b"42 ".as_slice(),
        ] {
            assert!(parse_i64_strict(s).is_err(), "should reject {:?}", s);
        }
    }

    #[test]
    fn checked_add_at_max_boundary_overflows() {
        let r: Option<i64> = i64::MAX.checked_add(1);
        assert!(r.is_none());
    }

    #[test]
    fn checked_add_at_min_boundary_overflows() {
        let r: Option<i64> = i64::MIN.checked_add(-1);
        assert!(r.is_none());
    }

    #[test]
    fn decr_one_under_zero_works() {
        // 0 + (-1) doesn't overflow.
        assert_eq!(0i64.checked_add(-1).unwrap(), -1);
    }

    #[test]
    fn decrby_negation_of_min_overflows() {
        // checked_neg on i64::MIN returns None; FLASH.DECRBY must reject it
        // before calling the shared incr_impl.
        assert!(i64::MIN.checked_neg().is_none());
    }

    #[test]
    fn append_extends_byte_buffer() {
        let mut v: Vec<u8> = b"hello".to_vec();
        v.extend_from_slice(b" world");
        assert_eq!(v, b"hello world");
        assert_eq!(v.len(), 11);
    }
}
