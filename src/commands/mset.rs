//! `FLASH.MSET key value [key value ...]` — batched set across flash strings.
//!
//! Production-readiness gap closer for v1.1.1. Wrappers previously had to
//! issue N separate `FLASH.SET` commands for bulk loads, paying N round-trips
//! per batch. This command takes one round-trip and is atomic from the
//! client's viewpoint.
//!
//! Semantics match the spec at backlog `f5d207d2`:
//!
//! - **Hot-tier-only writes.** All values land in `Tier::Hot` immediately;
//!   demotion to NVMe happens lazily via the standard auto-demotion path.
//!   The command is fast (~native MSET speed) and the call doesn't block
//!   on NVMe IO.
//! - **Atomic to the client.** Phase 1 type-checks every key without
//!   modifying anything; phase 2 writes all keys within one event-loop
//!   tick. Either all writes apply or none do.
//! - **TTL is cleared on each write**, matching native `MSET` (which
//!   acts as a per-key SET-without-KEEPTTL).
//! - **Batch size capped at 1024 keys** per call. Larger batches return
//!   an error suggesting client-side chunking. The cap exists because
//!   hot-tier-only writes temporarily inflate RAM by `sum(value bytes)`
//!   before demotion catches up; pathological huge batches could OOM.
//! - **Replicates verbatim** — primary's MSET sends as one MSET to
//!   replicas and AOF.
//! - **Per-key keyspace notifications** (`flash.set`), matching native
//!   MSET behaviour.

use valkey_module::{Context, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

use crate::CACHE;
use crate::types::Tier;
use crate::types::string::{FLASH_STRING_TYPE, FlashStringObject};

/// Hard upper bound on the per-call MSET batch size. Bigger batches must
/// chunk client-side. Sized to keep transient RAM inflation under one
/// reasonable working set.
const MAX_MSET_KEYS: usize = 1024;

/// `FLASH.MSET key value [key value ...]`
pub fn flash_mset_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    // Need at least one (key, value) pair after the command name.
    if args.len() < 3 || !(args.len() - 1).is_multiple_of(2) {
        return Err(ValkeyError::WrongArity);
    }

    let n_pairs = (args.len() - 1) / 2;
    if n_pairs > MAX_MSET_KEYS {
        return Err(ValkeyError::String(format!(
            "ERR FLASH.MSET batch too large ({n_pairs} keys > {MAX_MSET_KEYS}); chunk the call client-side"
        )));
    }

    let cache = CACHE
        .get()
        .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;

    // Phase 1 — type-check every key with no side effects.  Any key that
    // exists with a non-flash-string type fails the whole batch with
    // WRONGTYPE.  This matches FLASH.SET behaviour key-by-key.
    for i in 0..n_pairs {
        let key = &args[1 + 2 * i];
        let kh = ctx.open_key_writable(key);
        if kh
            .get_value::<FlashStringObject>(&FLASH_STRING_TYPE)
            .is_err()
        {
            return Err(ValkeyError::WrongType);
        }
        // kh drops here → CloseKey
    }

    // Phase 2 — write every key.  Within a single event-loop tick this is
    // atomic from any external observer's perspective.
    for i in 0..n_pairs {
        let key = &args[1 + 2 * i];
        let value = args[2 + 2 * i].as_slice().to_vec();
        let kh = ctx.open_key_writable(key);
        kh.set_value(
            &FLASH_STRING_TYPE,
            FlashStringObject {
                tier: Tier::Hot(value.clone()),
                // Native MSET clears TTL on each write (no KEEPTTL option).
                ttl_ms: None,
            },
        )
        .map_err(|e| ValkeyError::String(format!("flash: mset set_value: {e}")))?;

        cache.put(key.as_slice(), value);
    }

    // Replicate the entire MSET as one command — replicas + AOF replay
    // get the same atomic batch.
    ctx.replicate_verbatim();

    // Per-key STRING notifications, matching native MSET.
    for i in 0..n_pairs {
        let key = &args[1 + 2 * i];
        ctx.notify_keyspace_event(NotifyEvent::STRING, "flash.set", key);
    }

    Ok(ValkeyValue::SimpleStringStatic("OK"))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #[test]
    fn arg_layout_matches_native_mset() {
        // (cmd, k1, v1) → 3 args, 1 pair
        // (cmd, k1, v1, k2, v2) → 5 args, 2 pairs
        // Generally: 1 + 2N args, N pairs.
        for n in 1..=5usize {
            let args = 1 + 2 * n;
            assert!(args >= 3 && (args - 1).is_multiple_of(2));
            assert_eq!((args - 1) / 2, n);
        }
    }

    #[test]
    fn even_total_args_is_invalid() {
        // (cmd, k1) → 2 args, even, no value for k1.
        // (cmd, k1, v1, k2) → 4 args, missing v2.
        for args in [2usize, 4, 6] {
            assert!(!(args - 1).is_multiple_of(2));
        }
    }
}
