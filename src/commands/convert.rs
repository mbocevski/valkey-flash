//! `FLASH.CONVERT key` — per-key primitive that atomically converts a FLASH.*
//! key to its native Valkey counterpart (STRING / HASH / LIST / ZSET),
//! preserving name + TTL. The prerequisite for `MODULE UNLOAD flash`, which
//! Valkey refuses while custom-type keys exist.
//!
//! Replication strategy: all state-mutating sub-calls (`DEL`, the native
//! create, optional `PEXPIREAT`) are issued via `ctx.call_ext` with the
//! replicate flag set, so the AOF and replication stream see plain native
//! commands — never `FLASH.CONVERT` itself. That keeps the persisted log
//! module-independent after conversion: AOF replay and replica state survive
//! a subsequent `MODULE UNLOAD flash`.

use std::sync::atomic::{AtomicU64, Ordering};

use valkey_module::{
    CallOptions, CallOptionsBuilder, CallResult, Context, NotifyEvent, ValkeyError, ValkeyResult,
    ValkeyString, ValkeyValue, logging, raw::KeyType,
};

use crate::STORAGE;
use crate::types::Tier;
use crate::types::hash::{FLASH_HASH_TYPE, FlashHashObject, hash_deserialize_or_warn};
use crate::types::list::{FLASH_LIST_TYPE, FlashListObject, list_deserialize_or_warn};
use crate::types::string::{FLASH_STRING_TYPE, FlashStringObject};
use crate::types::zset::{FLASH_ZSET_TYPE, FlashZSetObject, zset_deserialize_or_warn};

/// Count of successful `FLASH.CONVERT` operations on this node.
/// Surfaced via `INFO flash` as `flash_convert_total`.
pub static CONVERT_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Payload materialised from a FLASH.* key, ready to be rewritten as a native
/// Valkey value via `ctx.call_ext`. Owned by the caller so it survives the
/// drop of the source key's open-key handle.
pub(crate) enum ConvertPayload {
    String {
        bytes: Vec<u8>,
        ttl_ms: Option<i64>,
    },
    Hash {
        // Flat `[f1, v1, f2, v2, ...]` — matches the HSET argument shape.
        flat: Vec<Vec<u8>>,
        ttl_ms: Option<i64>,
    },
    List {
        // Head-to-tail order; RPUSH-ing in this sequence reproduces the list.
        elems: Vec<Vec<u8>>,
        ttl_ms: Option<i64>,
    },
    ZSet {
        // Flat `[score_str, member, score_str, member, ...]` — ZADD argument shape.
        flat: Vec<Vec<u8>>,
        ttl_ms: Option<i64>,
    },
}

impl ConvertPayload {
    pub(crate) fn ttl_ms(&self) -> Option<i64> {
        match self {
            ConvertPayload::String { ttl_ms, .. }
            | ConvertPayload::Hash { ttl_ms, .. }
            | ConvertPayload::List { ttl_ms, .. }
            | ConvertPayload::ZSet { ttl_ms, .. } => *ttl_ms,
        }
    }
}

// ── Command handler ───────────────────────────────────────────────────────────

/// `FLASH.CONVERT key` → `:1` on success, `:0` if the key is missing or is
/// already a non-flash type (idempotent).
pub fn flash_convert_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() != 2 {
        return Err(ValkeyError::WrongArity);
    }

    match extract_payload(ctx, &args[1])? {
        None => Ok(ValkeyValue::Integer(0)),
        Some(payload) => {
            apply_conversion(ctx, &args[1], payload)?;
            CONVERT_TOTAL.fetch_add(1, Ordering::Relaxed);
            Ok(ValkeyValue::Integer(1))
        }
    }
}

// ── Internals ─────────────────────────────────────────────────────────────────

/// Read a FLASH.* key and materialise its payload into plain Rust values.
///
/// Returns `Ok(None)` when the key is absent, is a native (non-module) type,
/// or is a module type other than ours — callers treat all three as idempotent
/// "nothing to convert" and reply `:0`.
pub(crate) fn extract_payload(
    ctx: &Context,
    key: &ValkeyString,
) -> Result<Option<ConvertPayload>, ValkeyError> {
    let key_handle = ctx.open_key_writable(key);

    match key_handle.key_type() {
        KeyType::Empty => return Ok(None),
        KeyType::Module => {}
        _ => return Ok(None),
    }

    if let Ok(Some(obj)) = key_handle.get_value::<FlashStringObject>(&FLASH_STRING_TYPE) {
        let bytes = materialize_string_bytes(&obj.tier)?;
        return Ok(Some(ConvertPayload::String {
            bytes,
            ttl_ms: obj.ttl_ms,
        }));
    }
    if let Ok(Some(obj)) = key_handle.get_value::<FlashHashObject>(&FLASH_HASH_TYPE) {
        let flat = materialize_hash_flat(&obj.tier)?;
        return Ok(Some(ConvertPayload::Hash {
            flat,
            ttl_ms: obj.ttl_ms,
        }));
    }
    if let Ok(Some(obj)) = key_handle.get_value::<FlashListObject>(&FLASH_LIST_TYPE) {
        let elems = materialize_list_elems(&obj.tier)?;
        return Ok(Some(ConvertPayload::List {
            elems,
            ttl_ms: obj.ttl_ms,
        }));
    }
    if let Ok(Some(obj)) = key_handle.get_value::<FlashZSetObject>(&FLASH_ZSET_TYPE) {
        let flat = materialize_zset_flat(&obj.tier)?;
        return Ok(Some(ConvertPayload::ZSet {
            flat,
            ttl_ms: obj.ttl_ms,
        }));
    }

    // KeyType::Module but not one of our four — some other module's key.
    Ok(None)
}

fn cold_read(backend_offset: u64, value_len: u32) -> Result<Vec<u8>, ValkeyError> {
    // On a replica, the Cold tier is present only when
    // `flash.replica-tier-enabled=yes`. Without it, Cold entries have no
    // backing bytes, so CONVERT is a replica no-op — but this path should not
    // be reachable because CONVERT is a write command and Valkey rejects writes
    // on replicas. Guard anyway for belt-and-suspenders.
    if crate::replication::is_replica() {
        return Ok(Vec::new());
    }
    let storage = STORAGE
        .get()
        .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;
    storage
        .read_at_offset(backend_offset, value_len)
        .map_err(|e| ValkeyError::String(e.to_string()))
}

fn materialize_string_bytes(tier: &Tier<Vec<u8>>) -> Result<Vec<u8>, ValkeyError> {
    match tier {
        Tier::Hot(v) => Ok(v.clone()),
        Tier::Cold {
            backend_offset,
            value_len,
            ..
        } => cold_read(*backend_offset, *value_len),
    }
}

fn materialize_hash_flat(
    tier: &Tier<std::collections::HashMap<Vec<u8>, Vec<u8>>>,
) -> Result<Vec<Vec<u8>>, ValkeyError> {
    let map = match tier {
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
    let mut flat = Vec::with_capacity(map.len() * 2);
    for (k, v) in map {
        flat.push(k);
        flat.push(v);
    }
    Ok(flat)
}

fn materialize_list_elems(
    tier: &Tier<std::collections::VecDeque<Vec<u8>>>,
) -> Result<Vec<Vec<u8>>, ValkeyError> {
    match tier {
        Tier::Hot(deq) => Ok(deq.iter().cloned().collect()),
        Tier::Cold {
            backend_offset,
            value_len,
            ..
        } => {
            let bytes = cold_read(*backend_offset, *value_len)?;
            Ok(list_deserialize_or_warn(&bytes).into_iter().collect())
        }
    }
}

fn materialize_zset_flat(
    tier: &Tier<crate::types::zset::ZSetInner>,
) -> Result<Vec<Vec<u8>>, ValkeyError> {
    let inner = match tier {
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
    // Iterate in score-then-member order for determinism.
    let mut flat = Vec::with_capacity(inner.scores.len() * 2);
    for (score, member) in inner.scores.keys() {
        flat.push(format_score(score.0).into_bytes());
        flat.push(member.clone());
    }
    Ok(flat)
}

/// Render an `f64` score the same way Valkey does on the wire: avoid trailing
/// `.0` for integer-valued scores and preserve `inf` / `-inf`. This keeps
/// round-tripped ZADD payloads byte-identical to what native ZADD would accept.
fn format_score(score: f64) -> String {
    if score.is_infinite() {
        return if score.is_sign_negative() {
            "-inf".to_string()
        } else {
            "inf".to_string()
        };
    }
    // %.17g is the Redis/Valkey convention (d2string in util.c). Rust's
    // default f64 Display rounds to the shortest round-trippable form, which
    // matches %.17g output for the vast majority of scores; emitting `17` for
    // non-integer cases guarantees a lossless round-trip.
    if score.fract() == 0.0 && score.abs() < 1e17 {
        format!("{}", score as i64)
    } else {
        format!("{score:.17}")
    }
}

/// Apply a materialised `ConvertPayload` by replacing the key with its native
/// equivalent. All sub-calls are issued with the replicate flag so the AOF
/// and replicas observe plain native commands.
pub(crate) fn apply_conversion(
    ctx: &Context,
    key: &ValkeyString,
    payload: ConvertPayload,
) -> Result<(), ValkeyError> {
    let options = CallOptionsBuilder::new()
        .replicate()
        .errors_as_replies()
        .build();
    let key_bytes = key.as_slice().to_vec();

    // Step 1: DEL the flash key. This triggers our type's free callback,
    // which emits the WAL Delete tombstone and releases NVMe blocks for the
    // Cold case. Replicated as `DEL key`.
    let del_args: [&[u8]; 1] = [&key_bytes];
    call_checked(ctx, "DEL", &options, &del_args[..], "DEL")?;

    // Step 2: create the native key.
    match &payload {
        ConvertPayload::String { bytes, .. } => {
            let set_args: [&[u8]; 2] = [&key_bytes, bytes];
            call_checked(ctx, "SET", &options, &set_args[..], "SET")?;
        }
        ConvertPayload::Hash { flat, .. } => {
            // HSET key f1 v1 f2 v2 ...
            let mut refs: Vec<&[u8]> = Vec::with_capacity(1 + flat.len());
            refs.push(&key_bytes);
            refs.extend(flat.iter().map(|v| v.as_slice()));
            call_checked(ctx, "HSET", &options, refs.as_slice(), "HSET")?;
        }
        ConvertPayload::List { elems, .. } => {
            // Skip RPUSH entirely on an empty list — RPUSH rejects zero elements
            // with an arity error. A FlashList reaches empty only transiently
            // (it is deleted at the type layer when empty), but belt-and-suspenders.
            if !elems.is_empty() {
                let mut refs: Vec<&[u8]> = Vec::with_capacity(1 + elems.len());
                refs.push(&key_bytes);
                refs.extend(elems.iter().map(|v| v.as_slice()));
                call_checked(ctx, "RPUSH", &options, refs.as_slice(), "RPUSH")?;
            }
        }
        ConvertPayload::ZSet { flat, .. } => {
            if !flat.is_empty() {
                let mut refs: Vec<&[u8]> = Vec::with_capacity(1 + flat.len());
                refs.push(&key_bytes);
                refs.extend(flat.iter().map(|v| v.as_slice()));
                call_checked(ctx, "ZADD", &options, refs.as_slice(), "ZADD")?;
            }
        }
    }

    // Step 3: restore TTL via PEXPIREAT with absolute epoch ms.
    // Valkey treats a past timestamp as an immediate delete — matches core
    // native SET-with-expired-TTL semantics.
    if let Some(ttl_ms) = payload.ttl_ms() {
        let ttl_str = ttl_ms.to_string();
        let expire_args: [&[u8]; 2] = [&key_bytes, ttl_str.as_bytes()];
        call_checked(ctx, "PEXPIREAT", &options, &expire_args[..], "PEXPIREAT")?;
    }

    // Step 4: emit a correlating module-level event. The native sub-calls
    // above also emit their own standard events (`del`, `set` / `hset` /
    // `rpush` / `zadd`, `expire`). Subscribers can latch onto `flash.convert`
    // to know which native events came from a drain.
    ctx.notify_keyspace_event(NotifyEvent::GENERIC, "flash.convert", key);

    Ok(())
}

fn call_checked(
    ctx: &Context,
    cmd: &str,
    options: &CallOptions,
    args: &[&[u8]],
    label: &str,
) -> Result<(), ValkeyError> {
    let res: CallResult<'static> = ctx.call_ext(cmd, options, args);
    match res {
        Ok(_) => Ok(()),
        Err(e) => {
            let msg = e.to_utf8_string().unwrap_or_else(|| label.to_string());
            logging::log_warning(
                format!("flash: FLASH.CONVERT {label} sub-call failed: {msg}").as_str(),
            );
            Err(ValkeyError::String(format!(
                "ERR FLASH.CONVERT {label} sub-call failed: {msg}"
            )))
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_score_integers_omit_fraction() {
        assert_eq!(format_score(0.0), "0");
        assert_eq!(format_score(1.0), "1");
        assert_eq!(format_score(-5.0), "-5");
        assert_eq!(format_score(42.0), "42");
    }

    #[test]
    fn format_score_fractional_roundtrips() {
        let s = format_score(1.5);
        let back: f64 = s.parse().unwrap();
        assert_eq!(back, 1.5);
    }

    #[test]
    fn format_score_infinity() {
        assert_eq!(format_score(f64::INFINITY), "inf");
        assert_eq!(format_score(f64::NEG_INFINITY), "-inf");
    }

    #[test]
    fn convert_payload_ttl_accessor() {
        let p = ConvertPayload::String {
            bytes: vec![1, 2, 3],
            ttl_ms: Some(12345),
        };
        assert_eq!(p.ttl_ms(), Some(12345));

        let p = ConvertPayload::Hash {
            flat: vec![],
            ttl_ms: None,
        };
        assert_eq!(p.ttl_ms(), None);
    }

    #[test]
    fn convert_total_counter_is_readable() {
        let _ = CONVERT_TOTAL.load(Ordering::Relaxed);
    }
}
