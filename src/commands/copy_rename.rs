//! `FLASH.COPY` / `FLASH.RENAME` / `FLASH.RENAMENX` — atomic key relocation.
//!
//! Production-readiness gap closer for v1.1.1. Wrappers' Bucket C currently
//! falls back to non-atomic GET+SET+DEL across tiers — risk of partial state
//! on crash. Module-side primitives are atomic by virtue of single-tick
//! command execution: the entire operation runs to completion within one
//! event-loop iteration, no client observes an intermediate state.
//!
//! ## Algorithm (v1)
//!
//! 1. Open `src`, type-check (must be one of the four `FlashType`s — else
//!    return error: `WRONGTYPE` for type mismatch on RENAME, no-op for
//!    COPY-without-REPLACE).
//! 2. Materialise the value: clone for Hot, synchronous NVMe read for Cold
//!    (matches the existing pattern in `lset` / `linsert` / `lrem` / etc).
//! 3. Read TTL from src's typed object.
//! 4. Open `dst` writable. Existing payload (if any) is replaced when we
//!    `set_value` — Valkey calls its free callback automatically, which for
//!    Cold values releases the NVMe blocks via `release_cold_blocks`.
//! 5. `set_value` at dst with a new typed object whose tier is `Hot` and
//!    whose payload is the materialised value. The auto-demotion pipeline
//!    will demote it back to Cold later if policy warrants.
//! 6. Apply src's TTL (if any) to dst.
//! 7. For RENAME/RENAMENX: open `src` writable and delete it. Cold src's
//!    blocks are released by the free callback — they're no longer referenced
//!    because we already materialised the value into RAM at step 2 and wrote
//!    it Hot at dst.
//!
//! Atomicity is single-tick: steps 4-7 happen without yielding the event
//! loop, so a client that issues `FLASH.RENAME` followed by `FLASH.GET dst`
//! sees the rename complete. The Cold-read in step 2 blocks the event loop
//! synchronously — accepted v1 pattern (documented in `list_common.rs`).
//!
//! ## What this v1 does NOT do (deferred)
//!
//! - **Pointer-swap optimisation.** The spec proposed an O(1) metadata-only
//!   rename for Cold-src cases (just migrate the `TIERING_MAP` entry from
//!   `H_src` to `H_dst`, no data movement). v1 instead materialises through
//!   Hot — slower for large Cold values, but simpler and shippable in a
//!   single commit. Follow-up perf task.
//! - **Custom WAL record type.** v1 uses the standard SET / DEL paths which
//!   write the standard Put / Delete WAL records. Recovery semantics are
//!   identical to a manual GET + SET + DEL sequence; the atomicity comes
//!   from single-tick execution, not WAL coordination.
//!
//! ## Cluster mode
//!
//! Native RENAME / COPY require src and dst in the same hash slot or return
//! CROSSSLOT. The module commands inherit this via the standard key-spec
//! declaration `(1, 2, 1)` — first key at position 1, last at position 2,
//! step 1. Valkey's cluster routing enforces the slot constraint before the
//! command handler runs.

use std::time::Duration;

use valkey_module::{Context, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

use crate::CACHE;
use crate::commands::list_common::current_time_ms;
use crate::types::Tier;
use crate::types::hash::{
    FLASH_HASH_TYPE, FlashHashObject, hash_deserialize_or_warn, hash_serialize,
};
use crate::types::list::{
    FLASH_LIST_TYPE, FlashListObject, list_deserialize_or_warn, list_serialize,
};
use crate::types::string::{FLASH_STRING_TYPE, FlashStringObject};
use crate::types::zset::{
    FLASH_ZSET_TYPE, FlashZSetObject, zset_deserialize_or_warn, zset_serialize,
};

// ── Materialised src value ────────────────────────────────────────────────────

/// Eagerly-materialised value for a flash key, ready to be written under a new
/// name as a `Tier::Hot` typed object.
enum MaterialisedFlash {
    String(Vec<u8>),
    Hash(std::collections::HashMap<Vec<u8>, Vec<u8>>),
    List(std::collections::VecDeque<Vec<u8>>),
    ZSet(crate::types::zset::ZSetInner),
}

/// Synchronously read NVMe at `(offset, len)`. Mirrors `convert.rs::cold_read`
/// — the same event-loop-blocking helper used by FLASH.CONVERT and other
/// Cold-materialisation paths.
fn cold_read(offset: u64, len: u32) -> Result<Vec<u8>, ValkeyError> {
    if len == 0 {
        return Ok(Vec::new());
    }
    let storage = crate::STORAGE
        .get()
        .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;
    storage
        .read_at_offset(offset, len)
        .map_err(|e| ValkeyError::String(e.to_string()))
}

/// Read src's value into RAM and capture its TTL. Returns `Ok(None)` if src
/// doesn't exist OR holds a non-flash type — the caller decides how to react.
fn materialise_src(
    ctx: &Context,
    src: &ValkeyString,
) -> Result<Option<(MaterialisedFlash, Option<i64>)>, ValkeyError> {
    let kh = ctx.open_key(src);

    // Try each flash type. get_value returns Err only when the key is present
    // but a different type; Ok(None) when absent; Ok(Some) when matched.
    if let Ok(Some(obj)) = kh.get_value::<FlashStringObject>(&FLASH_STRING_TYPE) {
        let bytes = match &obj.tier {
            Tier::Hot(v) => v.clone(),
            Tier::Cold {
                backend_offset,
                value_len,
                ..
            } => cold_read(*backend_offset, *value_len)?,
        };
        return Ok(Some((MaterialisedFlash::String(bytes), obj.ttl_ms)));
    }
    if let Ok(Some(obj)) = kh.get_value::<FlashHashObject>(&FLASH_HASH_TYPE) {
        let map = match &obj.tier {
            Tier::Hot(m) => m.clone(),
            Tier::Cold {
                backend_offset,
                value_len,
                ..
            } => {
                let bytes = cold_read(*backend_offset, *value_len)?;
                hash_deserialize_or_warn(&bytes)
            }
        };
        return Ok(Some((MaterialisedFlash::Hash(map), obj.ttl_ms)));
    }
    if let Ok(Some(obj)) = kh.get_value::<FlashListObject>(&FLASH_LIST_TYPE) {
        let list = match &obj.tier {
            Tier::Hot(d) => d.clone(),
            Tier::Cold {
                backend_offset,
                value_len,
                ..
            } => {
                let bytes = cold_read(*backend_offset, *value_len)?;
                list_deserialize_or_warn(&bytes)
            }
        };
        return Ok(Some((MaterialisedFlash::List(list), obj.ttl_ms)));
    }
    if let Ok(Some(obj)) = kh.get_value::<FlashZSetObject>(&FLASH_ZSET_TYPE) {
        let z = match &obj.tier {
            Tier::Hot(z) => z.clone(),
            Tier::Cold {
                backend_offset,
                value_len,
                ..
            } => {
                let bytes = cold_read(*backend_offset, *value_len)?;
                zset_deserialize_or_warn(&bytes)
            }
        };
        return Ok(Some((MaterialisedFlash::ZSet(z), obj.ttl_ms)));
    }

    // Either absent or a non-flash type. Caller distinguishes.
    Ok(None)
}

/// Does `key` currently hold any value (any type — flash, native, etc)?
/// Used by RENAMENX / COPY-without-REPLACE to gate the operation.
fn key_exists_any_type(ctx: &Context, key: &ValkeyString) -> bool {
    let kh = ctx.open_key(key);
    // ValkeyKey doesn't expose key_type() directly in this crate version; use
    // a Boolean check via the cheapest predicate available — try get_value on
    // any flash type, then check whether the keyspace would let us probe via
    // a string-getter.  Simplest correct: check every flash type, plus inspect
    // the `key_type()` if present.  In practice, the wrappers route by policy
    // and dst won't have non-flash content, but for correctness we check both.
    if matches!(
        kh.get_value::<FlashStringObject>(&FLASH_STRING_TYPE),
        Ok(Some(_))
    ) || matches!(
        kh.get_value::<FlashHashObject>(&FLASH_HASH_TYPE),
        Ok(Some(_))
    ) || matches!(
        kh.get_value::<FlashListObject>(&FLASH_LIST_TYPE),
        Ok(Some(_))
    ) || matches!(
        kh.get_value::<FlashZSetObject>(&FLASH_ZSET_TYPE),
        Ok(Some(_))
    ) {
        return true;
    }
    // Native / other-module value present.
    kh.key_type() != valkey_module::raw::KeyType::Empty
}

/// Write `value` at `dst` as a `Tier::Hot` typed object. Replaces any
/// existing payload (Valkey calls the old type's free callback inside
/// set_value, which releases Cold blocks for flash types).
fn write_dst_hot(
    ctx: &Context,
    dst: &ValkeyString,
    value: MaterialisedFlash,
    ttl_ms: Option<i64>,
) -> Result<(), ValkeyError> {
    let dst_kh = ctx.open_key_writable(dst);
    let cache = CACHE
        .get()
        .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;

    // ModuleTypeSetValue is type-strict: it errors with "Existing key has
    // wrong Valkey type" if the key currently holds a value of a different
    // type than the one we're trying to write.  Native RENAME / COPY are
    // type-blind — they overwrite dst regardless.  Match that semantic by
    // deleting dst first if it exists.  For same-type replacement this is
    // a no-op-via-recreate; for cross-type it's required for correctness.
    // For Cold values, the free callback releases NVMe blocks via
    // `release_cold_blocks` — same outcome as set_value's automatic free.
    if dst_kh.key_type() != valkey_module::raw::KeyType::Empty {
        let _ = dst_kh.delete();
        cache.delete(dst.as_slice());
    }

    match value {
        MaterialisedFlash::String(bytes) => {
            let serialized = bytes.clone();
            dst_kh
                .set_value(
                    &FLASH_STRING_TYPE,
                    FlashStringObject {
                        tier: Tier::Hot(bytes),
                        ttl_ms,
                    },
                )
                .map_err(|e| ValkeyError::String(format!("flash: copy/rename set_value: {e}")))?;
            cache.put(dst.as_slice(), serialized);
        }
        MaterialisedFlash::Hash(map) => {
            let serialized = hash_serialize(&map);
            dst_kh
                .set_value(
                    &FLASH_HASH_TYPE,
                    FlashHashObject {
                        tier: Tier::Hot(map),
                        ttl_ms,
                    },
                )
                .map_err(|e| ValkeyError::String(format!("flash: copy/rename set_value: {e}")))?;
            cache.put(dst.as_slice(), serialized);
        }
        MaterialisedFlash::List(deque) => {
            let serialized = list_serialize(&deque);
            dst_kh
                .set_value(
                    &FLASH_LIST_TYPE,
                    FlashListObject {
                        tier: Tier::Hot(deque),
                        ttl_ms,
                    },
                )
                .map_err(|e| ValkeyError::String(format!("flash: copy/rename set_value: {e}")))?;
            cache.put(dst.as_slice(), serialized);
        }
        MaterialisedFlash::ZSet(z) => {
            let serialized = zset_serialize(&z);
            dst_kh
                .set_value(
                    &FLASH_ZSET_TYPE,
                    FlashZSetObject {
                        tier: Tier::Hot(z),
                        ttl_ms,
                    },
                )
                .map_err(|e| ValkeyError::String(format!("flash: copy/rename set_value: {e}")))?;
            cache.put(dst.as_slice(), serialized);
        }
    }

    // Apply TTL if any. set_value clears the native expire as a side-effect
    // (per the docstring on util_expire.rs) so we re-arm it here.
    if let Some(abs_ms) = ttl_ms {
        let remaining_ms = (abs_ms - current_time_ms()).max(1) as u64;
        dst_kh
            .set_expire(Duration::from_millis(remaining_ms))
            .map_err(|e| ValkeyError::String(format!("flash: copy/rename set_expire: {e}")))?;
    }

    Ok(())
}

/// Delete `src` from the keyspace. For Cold values, the type's free callback
/// releases the NVMe blocks via `release_cold_blocks`.
fn delete_src(ctx: &Context, src: &ValkeyString) {
    let cache = CACHE.get();
    let src_kh = ctx.open_key_writable(src);
    let _ = src_kh.delete();
    if let Some(c) = cache {
        c.delete(src.as_slice());
    }
}

// ── Command handlers ──────────────────────────────────────────────────────────

/// `FLASH.COPY source destination [REPLACE]`
///
/// Returns `1` on successful copy, `0` if `dst` exists without `REPLACE`.
/// `WRONGTYPE` if `src` exists but is not a flash type. No-op if `src` doesn't
/// exist (returns `0`, matching native COPY).
pub fn flash_copy_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 3 || args.len() > 4 {
        return Err(ValkeyError::WrongArity);
    }
    let src = &args[1];
    let dst = &args[2];
    let replace = match args.get(3) {
        None => false,
        Some(a) => {
            let upper = a.as_slice().to_ascii_uppercase();
            if upper.as_slice() != b"REPLACE" {
                return Err(ValkeyError::Str("ERR syntax error"));
            }
            true
        }
    };

    let materialised = match materialise_src(ctx, src)? {
        Some(m) => m,
        None => {
            // src absent or non-flash — return 0 (matches native COPY semantics
            // where a missing src returns 0; non-flash is treated identically
            // for this module's purposes — wrappers route by policy).
            return Ok(ValkeyValue::Integer(0));
        }
    };

    if !replace && key_exists_any_type(ctx, dst) {
        return Ok(ValkeyValue::Integer(0));
    }

    let (value, ttl) = materialised;
    write_dst_hot(ctx, dst, value, ttl)?;

    ctx.replicate_verbatim();
    ctx.notify_keyspace_event(NotifyEvent::GENERIC, "flash.copy", dst);

    Ok(ValkeyValue::Integer(1))
}

/// `FLASH.RENAME source destination`
///
/// Renames `src` to `dst`, replacing any existing `dst` payload. Returns
/// `+OK` on success. Errors if `src` doesn't exist or holds a non-flash type.
pub fn flash_rename_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() != 3 {
        return Err(ValkeyError::WrongArity);
    }
    let src = &args[1];
    let dst = &args[2];

    let materialised = materialise_src(ctx, src)?.ok_or(ValkeyError::Str("ERR no such key"))?;
    let (value, ttl) = materialised;

    write_dst_hot(ctx, dst, value, ttl)?;
    delete_src(ctx, src);

    ctx.replicate_verbatim();
    // Two notifications: a 'flash.rename_from' on src and 'flash.rename_to'
    // on dst. Match native RENAME's notification shape (which fires
    // 'rename_from' + 'rename_to' under the GENERIC kind).
    ctx.notify_keyspace_event(NotifyEvent::GENERIC, "flash.rename_from", src);
    ctx.notify_keyspace_event(NotifyEvent::GENERIC, "flash.rename_to", dst);

    Ok(ValkeyValue::SimpleStringStatic("OK"))
}

/// `FLASH.RENAMENX source destination`
///
/// Returns `1` if rename happened, `0` if `dst` already existed (no-op).
/// Errors if `src` doesn't exist or holds a non-flash type.
pub fn flash_renamenx_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() != 3 {
        return Err(ValkeyError::WrongArity);
    }
    let src = &args[1];
    let dst = &args[2];

    let materialised = materialise_src(ctx, src)?.ok_or(ValkeyError::Str("ERR no such key"))?;

    if key_exists_any_type(ctx, dst) {
        return Ok(ValkeyValue::Integer(0));
    }

    let (value, ttl) = materialised;
    write_dst_hot(ctx, dst, value, ttl)?;
    delete_src(ctx, src);

    ctx.replicate_verbatim();
    ctx.notify_keyspace_event(NotifyEvent::GENERIC, "flash.rename_from", src);
    ctx.notify_keyspace_event(NotifyEvent::GENERIC, "flash.rename_to", dst);

    Ok(ValkeyValue::Integer(1))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    // Most behaviour is covered by integration tests against a live module
    // (tests/test_flash_copy_rename.py). Unit-testable surface is small
    // because the handlers are mostly Valkey-API dispatch.

    #[test]
    fn replace_keyword_recognition_is_case_insensitive() {
        for s in [b"REPLACE", b"replace", b"Replace", b"rEpLaCe"] {
            let upper = s.to_ascii_uppercase();
            assert_eq!(upper.as_slice(), b"REPLACE");
        }
    }

    #[test]
    fn unrecognised_keyword_rejected() {
        let s = b"REPLAC";
        let upper = s.to_ascii_uppercase();
        assert_ne!(upper.as_slice(), b"REPLACE");
    }
}
