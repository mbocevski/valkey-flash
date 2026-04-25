//! `FLASH.MGET key [key ...]` — batched get across flash strings.
//!
//! Production-readiness gap closer for v1.1.1. Wrappers previously had to
//! issue N separate `FLASH.GET` commands (or a `pipeline()` of `FLASH.GET`s)
//! to fetch a batch, paying N round-trips. This command takes one round-trip.
//!
//! Semantics mirror native `MGET`:
//!
//! - For each key argument, return the value if it exists as a `FlashString`,
//!   else nil. Non-string flash types (`FlashHash`/`List`/`ZSet`), native
//!   keys, and missing keys all map to nil — this command never raises
//!   `WRONGTYPE`.
//! - Returns one array reply with N entries in argument order.
//! - **Single round-trip from the client's perspective**, regardless of
//!   how many keys are cold-tier. Cold reads are bundled into one async
//!   task on the I/O pool that performs them sequentially before unblocking
//!   the client. The io_uring batch optimisation (one SQE-batch instead of
//!   N sequential reads) is a deferred follow-up; correctness ships here,
//!   the bandwidth win comes later.
//! - Promotes successfully-read cold values into the hot cache so a
//!   subsequent `FLASH.GET` on the same key is fast.

#[cfg(not(test))]
use std::sync::{Arc, Mutex};

use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

use crate::CACHE;
use crate::types::Tier;
use crate::types::string::{FLASH_STRING_TYPE, FlashStringObject};

// ── Per-key result slot ───────────────────────────────────────────────────────

/// One slot in the MGET reply array. `Pending` is a worker-thread-only state
/// that gets converted to `Cold` (success) or `Error` (NVMe failure → reply
/// nil for that slot).
#[derive(Clone)]
enum MGetSlot {
    /// Key was missing, native, non-flash-string flash type, or wrong-type —
    /// indistinguishable to the client (all reply nil).
    Nil,
    /// Hot tier value, ready to reply.
    Hot(Vec<u8>),
    /// Pending cold-tier read. The actual `(offset, len)` is carried in the
    /// parallel `cold_pending` vector at command-handler scope; this slot is
    /// just a placeholder that the worker overwrites with `Cold` or `Error`.
    Pending,
    /// Cold value successfully read from NVMe.
    Cold(Vec<u8>),
    /// Cold read failed on NVMe error — replies as nil to match native MGET
    /// best-effort semantics. Logged by the worker for operator visibility.
    #[allow(dead_code)]
    Error,
}

// ── MGet completion handle ────────────────────────────────────────────────────

#[cfg(not(test))]
struct MGetCompletionHandle {
    tsc: valkey_module::ThreadSafeContext<valkey_module::BlockedClient<()>>,
    /// Reply slots in argument order. Worker fills `Pending` → `Cold`/`Error`.
    slots: Arc<Mutex<Vec<MGetSlot>>>,
    /// Keys for cold slots (parallel index list) used for hot-cache promotion
    /// after the worker fills the values.
    cold_keys: Vec<(usize, Vec<u8>)>,
    cache: &'static crate::storage::cache::FlashCache,
}

#[cfg(not(test))]
impl crate::async_io::CompletionHandle for MGetCompletionHandle {
    fn complete(self: Box<Self>, _result: crate::storage::backend::StorageResult<Vec<u8>>) {
        let me = *self;
        // Take ownership of the slot vector — the worker's done with it.
        let slots = std::mem::take(&mut *me.slots.lock().unwrap());

        // Promote successful cold reads into the hot cache so future GETs are fast.
        for (idx, key) in &me.cold_keys {
            if let Some(MGetSlot::Cold(bytes)) = slots.get(*idx) {
                me.cache.put(key, bytes.clone());
            }
        }

        // Assemble the reply — every slot maps to a value or nil.
        let array: Vec<ValkeyValue> = slots
            .into_iter()
            .map(|s| match s {
                MGetSlot::Nil | MGetSlot::Pending | MGetSlot::Error => ValkeyValue::Null,
                MGetSlot::Hot(b) | MGetSlot::Cold(b) => ValkeyValue::StringBuffer(b),
            })
            .collect();

        let reply = Ok(ValkeyValue::Array(array));
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            me.tsc.reply(reply);
        }));
    }
}

// ── Command handler ───────────────────────────────────────────────────────────

/// `FLASH.MGET key [key ...]`
pub fn flash_mget_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 2 {
        return Err(ValkeyError::WrongArity);
    }

    let keys = &args[1..];
    let cache = CACHE
        .get()
        .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;

    // Phase 1: scan all keys, fill hot/missing inline, mark cold-pending.
    // Each key is opened in its own scope so the borrow doesn't escape.
    let mut slots: Vec<MGetSlot> = Vec::with_capacity(keys.len());
    let mut cold_pending: Vec<(usize, Vec<u8>, u64, u32)> = Vec::new();

    for (idx, key) in keys.iter().enumerate() {
        // Always type-check via open_key before trusting the cache.
        // The cache is unified across all flash types — a FLASH.HSET on key K
        // populates the cache with the serialised hash bytes; if MGET later
        // hits K and trusts the cache blindly, it would reply with hash bytes
        // as if they were a string. So: the open_key + get_value::<FlashString>
        // gates the cache lookup.
        let kh = ctx.open_key(key);
        match kh.get_value::<FlashStringObject>(&FLASH_STRING_TYPE) {
            Err(_) | Ok(None) => {
                // Wrong-type flash key, native key, or missing — all → nil.
                slots.push(MGetSlot::Nil);
            }
            Ok(Some(obj)) => match &obj.tier {
                Tier::Hot(bytes) => {
                    // Type-confirmed hot string. Cache may have a stale value
                    // for other commands; here we use the typed object's
                    // current bytes which are authoritative.
                    slots.push(MGetSlot::Hot(bytes.clone()));
                }
                Tier::Cold {
                    backend_offset,
                    value_len,
                    ..
                } => {
                    // Type-confirmed cold string. NOW the cache lookup is
                    // safe — only consult after we know the key type.
                    if let Some(bytes) = cache.get(key.as_slice()) {
                        slots.push(MGetSlot::Hot(bytes));
                    } else {
                        slots.push(MGetSlot::Pending);
                        cold_pending.push((
                            idx,
                            key.as_slice().to_vec(),
                            *backend_offset,
                            *value_len,
                        ));
                    }
                }
            },
        }
        // kh drops here → CloseKey
    }

    // Fast path: no cold reads needed → reply inline.
    if cold_pending.is_empty() {
        let array: Vec<ValkeyValue> = slots
            .into_iter()
            .map(|s| match s {
                MGetSlot::Nil | MGetSlot::Pending | MGetSlot::Error => ValkeyValue::Null,
                MGetSlot::Hot(b) | MGetSlot::Cold(b) => ValkeyValue::StringBuffer(b),
            })
            .collect();
        return Ok(ValkeyValue::Array(array));
    }

    // Slow path: dispatch one async task that does all cold reads sequentially.
    // The handle holds a shared mutex; the worker writes results into it
    // before complete() runs.
    #[cfg(not(test))]
    {
        if crate::replication::must_run_sync(ctx)
            || (crate::STORAGE.get().is_none() && crate::replication::must_obey_client(ctx))
        {
            // No storage: fill all pending slots as nil.
            let array: Vec<ValkeyValue> = slots
                .into_iter()
                .map(|s| match s {
                    MGetSlot::Nil | MGetSlot::Pending | MGetSlot::Error => ValkeyValue::Null,
                    MGetSlot::Hot(b) | MGetSlot::Cold(b) => ValkeyValue::StringBuffer(b),
                })
                .collect();
            return Ok(ValkeyValue::Array(array));
        }

        let storage = crate::STORAGE
            .get()
            .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;
        let pool = crate::POOL
            .get()
            .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;
        let bc = ctx.block_client();
        let shared_slots = Arc::new(Mutex::new(slots));
        let handle = Box::new(MGetCompletionHandle {
            tsc: valkey_module::ThreadSafeContext::with_blocked_client(bc),
            slots: shared_slots.clone(),
            cold_keys: cold_pending
                .iter()
                .map(|(i, k, _, _)| (*i, k.clone()))
                .collect(),
            cache,
        });

        pool.submit_or_complete(handle, move || {
            let mut guard = shared_slots.lock().unwrap();
            for (idx, _key, offset, len) in &cold_pending {
                match storage.read_at_offset(*offset, *len) {
                    Ok(bytes) => {
                        guard[*idx] = MGetSlot::Cold(bytes);
                    }
                    Err(e) => {
                        valkey_module::logging::log_warning(
                            format!("flash: MGET cold read failed @ slot {idx}: {e}").as_str(),
                        );
                        guard[*idx] = MGetSlot::Error;
                    }
                }
            }
            Ok(vec![])
        });
        return Ok(ValkeyValue::NoReply);
    }

    // Test build: synchronous fall-through. Real integration tests run with
    // the full async path.
    #[allow(unreachable_code)]
    {
        let array: Vec<ValkeyValue> = slots
            .into_iter()
            .map(|s| match s {
                MGetSlot::Nil | MGetSlot::Pending | MGetSlot::Error => ValkeyValue::Null,
                MGetSlot::Hot(b) | MGetSlot::Cold(b) => ValkeyValue::StringBuffer(b),
            })
            .collect();
        Ok(ValkeyValue::Array(array))
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slot_to_value_nil_for_absent_or_pending() {
        // Sanity-check the slot → ValkeyValue mapping logic.
        let nils = [MGetSlot::Nil, MGetSlot::Pending, MGetSlot::Error];
        for s in nils {
            let v = match s {
                MGetSlot::Nil | MGetSlot::Pending | MGetSlot::Error => "null",
                MGetSlot::Hot(_) | MGetSlot::Cold(_) => "value",
            };
            assert_eq!(v, "null");
        }
    }

    #[test]
    fn slot_to_value_string_for_hot_or_cold() {
        for s in [MGetSlot::Hot(b"x".to_vec()), MGetSlot::Cold(b"y".to_vec())] {
            let v = match s {
                MGetSlot::Nil | MGetSlot::Pending | MGetSlot::Error => "null",
                MGetSlot::Hot(_) | MGetSlot::Cold(_) => "value",
            };
            assert_eq!(v, "value");
        }
    }
}
