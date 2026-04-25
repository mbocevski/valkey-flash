//! `FLASH.EXPIRE` family — TTL mutators and readers.
//!
//! Production-readiness gap closer for the v1.1.1 release. Previously the only
//! way to set a TTL on a flash-tier key was at write-time via `FLASH.SET`'s
//! `EX`/`PX`/`EXAT`/`PXAT` options; there was no way to extend or inspect TTL
//! after the fact. That blocked the standard "session bump on activity"
//! pattern (extend TTL on every request) for any session-cache workload.
//!
//! This module ships nine commands mirroring the native Valkey `EXPIRE` /
//! `TTL` family:
//!
//! | Command | Args | Returns |
//! |---|---|---|
//! | `FLASH.EXPIRE` | `key seconds [NX\|XX\|GT\|LT]` | int |
//! | `FLASH.EXPIREAT` | `key unix-time-seconds [NX\|XX\|GT\|LT]` | int |
//! | `FLASH.PEXPIRE` | `key milliseconds [NX\|XX\|GT\|LT]` | int |
//! | `FLASH.PEXPIREAT` | `key unix-time-ms [NX\|XX\|GT\|LT]` | int |
//! | `FLASH.PERSIST` | `key` | int |
//! | `FLASH.TTL` | `key` | int (-2 missing, -1 no TTL, else seconds) |
//! | `FLASH.PTTL` | `key` | int (same units in ms) |
//! | `FLASH.EXPIRETIME` | `key` | int (absolute Unix seconds) |
//! | `FLASH.PEXPIRETIME` | `key` | int (absolute Unix ms) |
//!
//! Implementation notes:
//!
//! - **Pure metadata change.** No NVMe IO, no demotion-pipeline interaction.
//!   The TTL field lives on the in-RAM typed object (`obj.ttl_ms` on each of
//!   the four `FlashType` objects); we update it and call Valkey's native
//!   `set_expire` / `remove_expire` so active expiration kicks in.
//! - **Replication is canonicalised.** All four mutators replicate as a
//!   `FLASH.PEXPIREAT key <abs_ms>` regardless of the input form. Replicas
//!   apply the same absolute timestamp without clock-skew drift.
//! - **Non-flash keys contribute 0**, not a `WRONGTYPE` error. A key that
//!   was on flash but got drained back to native should be observable as
//!   "missing" from the flash module's viewpoint, not as an error.
//! - **NX/XX/GT/LT semantics match native Valkey 7.0+** exactly. `GT`/`LT`
//!   treat "no current TTL" as `+∞`: `GT` fails (nothing > +∞), `LT` always
//!   sets (everything < +∞).

use std::time::Duration;

use valkey_module::key::{ValkeyKey, ValkeyKeyWritable};
use valkey_module::{Context, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

use crate::commands::list_common::current_time_ms;
use crate::types::hash::{FLASH_HASH_TYPE, FlashHashObject};
use crate::types::list::{FLASH_LIST_TYPE, FlashListObject};
use crate::types::string::{FLASH_STRING_TYPE, FlashStringObject};
use crate::types::zset::{FLASH_ZSET_TYPE, FlashZSetObject};

// ── Types ─────────────────────────────────────────────────────────────────────

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum FlashType {
    String,
    Hash,
    List,
    ZSet,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum ExpireFlag {
    Nx,
    Xx,
    Gt,
    Lt,
}

// ── Parsing helpers ───────────────────────────────────────────────────────────

fn parse_expire_flag(arg: &[u8]) -> Result<ExpireFlag, ValkeyError> {
    let upper: Vec<u8> = arg.iter().map(|c| c.to_ascii_uppercase()).collect();
    match upper.as_slice() {
        b"NX" => Ok(ExpireFlag::Nx),
        b"XX" => Ok(ExpireFlag::Xx),
        b"GT" => Ok(ExpireFlag::Gt),
        b"LT" => Ok(ExpireFlag::Lt),
        _ => Err(ValkeyError::Str(
            "ERR Unsupported option (must be one of NX, XX, GT, LT)",
        )),
    }
}

fn parse_i64(arg: &[u8]) -> Result<i64, ValkeyError> {
    std::str::from_utf8(arg)
        .map_err(|_| ValkeyError::Str("ERR value is not an integer or out of range"))?
        .parse()
        .map_err(|_| ValkeyError::Str("ERR value is not an integer or out of range"))
}

// ── Type detection on writable handle ─────────────────────────────────────────

fn detect_flash_type_rw(kh: &ValkeyKeyWritable) -> Option<FlashType> {
    if matches!(
        kh.get_value::<FlashStringObject>(&FLASH_STRING_TYPE),
        Ok(Some(_))
    ) {
        return Some(FlashType::String);
    }
    if matches!(
        kh.get_value::<FlashHashObject>(&FLASH_HASH_TYPE),
        Ok(Some(_))
    ) {
        return Some(FlashType::Hash);
    }
    if matches!(
        kh.get_value::<FlashListObject>(&FLASH_LIST_TYPE),
        Ok(Some(_))
    ) {
        return Some(FlashType::List);
    }
    if matches!(
        kh.get_value::<FlashZSetObject>(&FLASH_ZSET_TYPE),
        Ok(Some(_))
    ) {
        return Some(FlashType::ZSet);
    }
    None
}

fn read_ttl_for_type_rw(kh: &ValkeyKeyWritable, t: FlashType) -> Option<i64> {
    match t {
        FlashType::String => kh
            .get_value::<FlashStringObject>(&FLASH_STRING_TYPE)
            .ok()
            .flatten()
            .and_then(|o| o.ttl_ms),
        FlashType::Hash => kh
            .get_value::<FlashHashObject>(&FLASH_HASH_TYPE)
            .ok()
            .flatten()
            .and_then(|o| o.ttl_ms),
        FlashType::List => kh
            .get_value::<FlashListObject>(&FLASH_LIST_TYPE)
            .ok()
            .flatten()
            .and_then(|o| o.ttl_ms),
        FlashType::ZSet => kh
            .get_value::<FlashZSetObject>(&FLASH_ZSET_TYPE)
            .ok()
            .flatten()
            .and_then(|o| o.ttl_ms),
    }
}

fn write_ttl_for_type(kh: &ValkeyKeyWritable, t: FlashType, abs_ms: Option<i64>) {
    match t {
        FlashType::String => {
            if let Ok(Some(obj)) = kh.get_value::<FlashStringObject>(&FLASH_STRING_TYPE) {
                obj.ttl_ms = abs_ms;
            }
        }
        FlashType::Hash => {
            if let Ok(Some(obj)) = kh.get_value::<FlashHashObject>(&FLASH_HASH_TYPE) {
                obj.ttl_ms = abs_ms;
            }
        }
        FlashType::List => {
            if let Ok(Some(obj)) = kh.get_value::<FlashListObject>(&FLASH_LIST_TYPE) {
                obj.ttl_ms = abs_ms;
            }
        }
        FlashType::ZSet => {
            if let Ok(Some(obj)) = kh.get_value::<FlashZSetObject>(&FLASH_ZSET_TYPE) {
                obj.ttl_ms = abs_ms;
            }
        }
    }
}

// ── Type detection on read-only handle ────────────────────────────────────────

fn detect_flash_type_ro(kh: &ValkeyKey) -> Option<FlashType> {
    if matches!(
        kh.get_value::<FlashStringObject>(&FLASH_STRING_TYPE),
        Ok(Some(_))
    ) {
        return Some(FlashType::String);
    }
    if matches!(
        kh.get_value::<FlashHashObject>(&FLASH_HASH_TYPE),
        Ok(Some(_))
    ) {
        return Some(FlashType::Hash);
    }
    if matches!(
        kh.get_value::<FlashListObject>(&FLASH_LIST_TYPE),
        Ok(Some(_))
    ) {
        return Some(FlashType::List);
    }
    if matches!(
        kh.get_value::<FlashZSetObject>(&FLASH_ZSET_TYPE),
        Ok(Some(_))
    ) {
        return Some(FlashType::ZSet);
    }
    None
}

fn read_ttl_for_type_ro(kh: &ValkeyKey, t: FlashType) -> Option<i64> {
    match t {
        FlashType::String => kh
            .get_value::<FlashStringObject>(&FLASH_STRING_TYPE)
            .ok()
            .flatten()
            .and_then(|o| o.ttl_ms),
        FlashType::Hash => kh
            .get_value::<FlashHashObject>(&FLASH_HASH_TYPE)
            .ok()
            .flatten()
            .and_then(|o| o.ttl_ms),
        FlashType::List => kh
            .get_value::<FlashListObject>(&FLASH_LIST_TYPE)
            .ok()
            .flatten()
            .and_then(|o| o.ttl_ms),
        FlashType::ZSet => kh
            .get_value::<FlashZSetObject>(&FLASH_ZSET_TYPE)
            .ok()
            .flatten()
            .and_then(|o| o.ttl_ms),
    }
}

// ── NX/XX/GT/LT predicate ─────────────────────────────────────────────────────

/// `current` = `None` is treated as `+∞` (key has no TTL set).
fn flag_allows_set(flag: Option<ExpireFlag>, current: Option<i64>, new_abs_ms: i64) -> bool {
    match flag {
        None => true,
        Some(ExpireFlag::Nx) => current.is_none(),
        Some(ExpireFlag::Xx) => current.is_some(),
        Some(ExpireFlag::Gt) => match current {
            None => false, // current = +∞; nothing > +∞
            Some(c) => new_abs_ms > c,
        },
        Some(ExpireFlag::Lt) => match current {
            None => true, // current = +∞; anything < +∞
            Some(c) => new_abs_ms < c,
        },
    }
}

// ── Mutator shared implementation ─────────────────────────────────────────────

fn expire_impl(
    ctx: &Context,
    args: Vec<ValkeyString>,
    cmd_label: &'static str,
    parse_to_abs_ms: impl Fn(i64) -> Result<i64, ValkeyError>,
) -> ValkeyResult {
    if args.len() < 3 || args.len() > 4 {
        return Err(ValkeyError::WrongArity);
    }

    let key = &args[1];
    let raw_value = parse_i64(args[2].as_slice())?;
    let new_abs_ms = parse_to_abs_ms(raw_value)?;
    let flag = match args.get(3) {
        None => None,
        Some(a) => Some(parse_expire_flag(a.as_slice())?),
    };

    let kh = ctx.open_key_writable(key);

    let kind = match detect_flash_type_rw(&kh) {
        Some(k) => k,
        None => return Ok(ValkeyValue::Integer(0)),
    };

    let current_ttl = read_ttl_for_type_rw(&kh, kind);
    if !flag_allows_set(flag, current_ttl, new_abs_ms) {
        return Ok(ValkeyValue::Integer(0));
    }

    write_ttl_for_type(&kh, kind, Some(new_abs_ms));
    let remaining_ms = (new_abs_ms - current_time_ms()).max(1) as u64;
    kh.set_expire(Duration::from_millis(remaining_ms))
        .map_err(|e| ValkeyError::String(format!("flash: {cmd_label} set_expire: {e}")))?;

    // Canonicalise replication as FLASH.PEXPIREAT so replicas + AOF replay
    // get the absolute timestamp regardless of the input command form.
    let abs_str = new_abs_ms.to_string();
    let args_for_replicate: [&[u8]; 2] = [key.as_slice(), abs_str.as_bytes()];
    ctx.replicate("FLASH.PEXPIREAT", &args_for_replicate[..]);

    ctx.notify_keyspace_event(NotifyEvent::GENERIC, "flash.expire", key);

    Ok(ValkeyValue::Integer(1))
}

// ── Mutator handlers ──────────────────────────────────────────────────────────

/// `FLASH.EXPIRE key seconds [NX|XX|GT|LT]`
pub fn flash_expire_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    expire_impl(ctx, args, "FLASH.EXPIRE", |secs| {
        let ms = secs
            .checked_mul(1000)
            .ok_or(ValkeyError::Str("ERR invalid expire time in FLASH.EXPIRE"))?;
        Ok(current_time_ms() + ms)
    })
}

/// `FLASH.EXPIREAT key unix-time-seconds [NX|XX|GT|LT]`
pub fn flash_expireat_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    expire_impl(ctx, args, "FLASH.EXPIREAT", |secs| {
        secs.checked_mul(1000).ok_or(ValkeyError::Str(
            "ERR invalid expire time in FLASH.EXPIREAT",
        ))
    })
}

/// `FLASH.PEXPIRE key milliseconds [NX|XX|GT|LT]`
pub fn flash_pexpire_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    expire_impl(ctx, args, "FLASH.PEXPIRE", |ms| Ok(current_time_ms() + ms))
}

/// `FLASH.PEXPIREAT key unix-time-ms [NX|XX|GT|LT]`
pub fn flash_pexpireat_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    expire_impl(ctx, args, "FLASH.PEXPIREAT", Ok)
}

// ── PERSIST handler ───────────────────────────────────────────────────────────

/// `FLASH.PERSIST key`
///
/// Returns 1 if the key existed as a flash type AND had a TTL that was removed.
/// Returns 0 if the key is missing, non-flash, or has no TTL.
pub fn flash_persist_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() != 2 {
        return Err(ValkeyError::WrongArity);
    }
    let key = &args[1];
    let kh = ctx.open_key_writable(key);

    let kind = match detect_flash_type_rw(&kh) {
        Some(k) => k,
        None => return Ok(ValkeyValue::Integer(0)),
    };

    if read_ttl_for_type_rw(&kh, kind).is_none() {
        return Ok(ValkeyValue::Integer(0));
    }

    write_ttl_for_type(&kh, kind, None);
    kh.remove_expire()
        .map_err(|e| ValkeyError::String(format!("flash: PERSIST remove_expire: {e}")))?;

    ctx.replicate_verbatim();
    ctx.notify_keyspace_event(NotifyEvent::GENERIC, "flash.persist", key);

    Ok(ValkeyValue::Integer(1))
}

// ── TTL/EXPIRETIME readers ────────────────────────────────────────────────────

/// Encode the four reader variants in two flags.
struct TtlOutput {
    /// True ⇒ output in milliseconds; false ⇒ seconds.
    ms_units: bool,
    /// True ⇒ absolute Unix timestamp; false ⇒ remaining duration.
    absolute: bool,
}

fn ttl_handler(ctx: &Context, args: Vec<ValkeyString>, out: TtlOutput) -> ValkeyResult {
    if args.len() != 2 {
        return Err(ValkeyError::WrongArity);
    }
    let key = &args[1];
    let kh = ctx.open_key(key);

    let kind = match detect_flash_type_ro(&kh) {
        Some(k) => k,
        // Native semantics: -2 if key doesn't exist (or non-flash from this
        // module's viewpoint).
        None => return Ok(ValkeyValue::Integer(-2)),
    };

    let abs_ms = match read_ttl_for_type_ro(&kh, kind) {
        // Native semantics: -1 if key has no TTL.
        None => return Ok(ValkeyValue::Integer(-1)),
        Some(v) => v,
    };

    let value = if out.absolute {
        if out.ms_units { abs_ms } else { abs_ms / 1000 }
    } else {
        let remaining = (abs_ms - current_time_ms()).max(0);
        if out.ms_units {
            remaining
        } else {
            remaining / 1000
        }
    };

    Ok(ValkeyValue::Integer(value))
}

/// `FLASH.TTL key` → seconds remaining, -1 if no TTL, -2 if missing.
pub fn flash_ttl_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    ttl_handler(
        ctx,
        args,
        TtlOutput {
            ms_units: false,
            absolute: false,
        },
    )
}

/// `FLASH.PTTL key` → ms remaining, -1 if no TTL, -2 if missing.
pub fn flash_pttl_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    ttl_handler(
        ctx,
        args,
        TtlOutput {
            ms_units: true,
            absolute: false,
        },
    )
}

/// `FLASH.EXPIRETIME key` → absolute Unix seconds, -1 if no TTL, -2 if missing.
pub fn flash_expiretime_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    ttl_handler(
        ctx,
        args,
        TtlOutput {
            ms_units: false,
            absolute: true,
        },
    )
}

/// `FLASH.PEXPIRETIME key` → absolute Unix ms, -1 if no TTL, -2 if missing.
pub fn flash_pexpiretime_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    ttl_handler(
        ctx,
        args,
        TtlOutput {
            ms_units: true,
            absolute: true,
        },
    )
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_flag_uppercase() {
        assert_eq!(parse_expire_flag(b"NX").unwrap(), ExpireFlag::Nx);
        assert_eq!(parse_expire_flag(b"XX").unwrap(), ExpireFlag::Xx);
        assert_eq!(parse_expire_flag(b"GT").unwrap(), ExpireFlag::Gt);
        assert_eq!(parse_expire_flag(b"LT").unwrap(), ExpireFlag::Lt);
    }

    #[test]
    fn parse_flag_lowercase_accepted() {
        assert_eq!(parse_expire_flag(b"nx").unwrap(), ExpireFlag::Nx);
        assert_eq!(parse_expire_flag(b"xx").unwrap(), ExpireFlag::Xx);
    }

    #[test]
    fn parse_flag_invalid_rejected() {
        assert!(parse_expire_flag(b"XY").is_err());
        assert!(parse_expire_flag(b"").is_err());
    }

    #[test]
    fn flag_none_always_allows() {
        assert!(flag_allows_set(None, None, 1000));
        assert!(flag_allows_set(None, Some(500), 1000));
    }

    #[test]
    fn nx_only_when_no_current_ttl() {
        assert!(flag_allows_set(Some(ExpireFlag::Nx), None, 1000));
        assert!(!flag_allows_set(Some(ExpireFlag::Nx), Some(500), 1000));
    }

    #[test]
    fn xx_only_when_current_ttl_exists() {
        assert!(!flag_allows_set(Some(ExpireFlag::Xx), None, 1000));
        assert!(flag_allows_set(Some(ExpireFlag::Xx), Some(500), 1000));
    }

    #[test]
    fn gt_treats_no_ttl_as_infinity() {
        // current = +∞, nothing > +∞ ⇒ never sets.
        assert!(!flag_allows_set(Some(ExpireFlag::Gt), None, i64::MAX));
        // current = 500, new = 1000, new > current ⇒ sets.
        assert!(flag_allows_set(Some(ExpireFlag::Gt), Some(500), 1000));
        // current = 1000, new = 500 ⇒ doesn't set.
        assert!(!flag_allows_set(Some(ExpireFlag::Gt), Some(1000), 500));
        // current = new ⇒ doesn't set (strict greater-than).
        assert!(!flag_allows_set(Some(ExpireFlag::Gt), Some(1000), 1000));
    }

    #[test]
    fn lt_treats_no_ttl_as_infinity() {
        // current = +∞, anything < +∞ ⇒ always sets.
        assert!(flag_allows_set(Some(ExpireFlag::Lt), None, 1000));
        assert!(flag_allows_set(Some(ExpireFlag::Lt), None, i64::MIN));
        // current = 1000, new = 500 ⇒ sets.
        assert!(flag_allows_set(Some(ExpireFlag::Lt), Some(1000), 500));
        // current = 500, new = 1000 ⇒ doesn't set.
        assert!(!flag_allows_set(Some(ExpireFlag::Lt), Some(500), 1000));
        // current = new ⇒ doesn't set (strict less-than).
        assert!(!flag_allows_set(Some(ExpireFlag::Lt), Some(1000), 1000));
    }

    #[test]
    fn parse_i64_valid_negative() {
        assert_eq!(parse_i64(b"-5").unwrap(), -5);
        assert_eq!(parse_i64(b"0").unwrap(), 0);
        assert_eq!(parse_i64(b"123").unwrap(), 123);
    }

    #[test]
    fn parse_i64_invalid() {
        assert!(parse_i64(b"abc").is_err());
        assert!(parse_i64(b"").is_err());
        assert!(parse_i64(b"1.5").is_err());
    }
}
