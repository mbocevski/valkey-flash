use std::ffi::CString;
use std::os::raw::{c_char, c_void};
use std::ptr::null_mut;
use valkey_module::native_types::ValkeyType;
use valkey_module::{RedisModuleDefragCtx, RedisModuleString, logging, raw};

use super::Tier;
use crate::persistence::aux;

// ── FlashStringObject ─────────────────────────────────────────────────────────

pub struct FlashStringObject {
    pub tier: Tier<Vec<u8>>,
    pub ttl_ms: Option<i64>,
}

// ── Type registration ─────────────────────────────────────────────────────────

const ENCODING_VERSION: i32 = 1;

// RDB format constants (spec #13, v1 inline format):
//   [u64 encoding_version][u64 shape_tag][i64 ttl_ms][string_buffer value]
const SHAPE_TAG_STRING: u64 = 0x01;
// Sentinel stored in the RDB ttl_ms field when the key has no expiry.
const TTL_NONE_SENTINEL: i64 = -1;

// "flashstr1" is exactly 9 chars — satisfies the module-type-id constraint.
pub static FLASH_STRING_TYPE: ValkeyType = ValkeyType::new(
    "flashstr1",
    ENCODING_VERSION,
    raw::RedisModuleTypeMethods {
        version: raw::REDISMODULE_TYPE_METHOD_VERSION as u64,
        rdb_load: Some(rdb_load),
        rdb_save: Some(rdb_save),
        aof_rewrite: Some(aof_rewrite),
        digest: Some(digest),
        // mem_usage (v1) intentionally None; real accounting is in mem_usage2 (v2).
        mem_usage: None,
        free: Some(free),
        aux_load: Some(aux::aux_load),
        aux_save: Some(aux::aux_save),
        aux_save2: None,
        // BEFORE_RDB (1) | AFTER_RDB (2)
        aux_save_triggers: raw::Aux::Before as i32 | raw::Aux::After as i32,
        free_effort: None,
        unlink: None,
        copy: Some(copy),
        defrag: Some(defrag),
        mem_usage2: Some(mem_usage2),
        free_effort2: None,
        unlink2: None,
        copy2: None,
    },
);

// ── Callbacks ─────────────────────────────────────────────────────────────────

/// # Safety
pub unsafe extern "C" fn free(value: *mut c_void) {
    unsafe {
        // SAFETY: value was allocated by Box::into_raw(Box::new(FlashStringObject {...}))
        // in a command handler. Valkey calls this callback exactly once per key
        // deletion / eviction — never while the key is still accessible.
        let obj = Box::from_raw(value.cast::<FlashStringObject>());
        if let Tier::Cold {
            key_hash,
            backend_offset,
            num_blocks,
            ..
        } = obj.tier
        {
            // Reclaim NVMe blocks so the space can be reused.
            if let Some(storage) = crate::STORAGE.get() {
                storage.release_cold_blocks(backend_offset, num_blocks);
            }
            // WAL tombstone: prevents recovery from re-promoting this key after a crash.
            if let Some(wal) = crate::WAL.get() {
                let _ = wal.append(crate::storage::wal::WalOp::Delete { key_hash });
            }
            // Remove from TIERING_MAP so recovery is not confused by a stale entry.
            if let Ok(mut map) = crate::TIERING_MAP.lock() {
                map.remove(&key_hash);
            }
        }
        // obj (and any Hot payload) drops here
    }
}

/// # Safety
pub unsafe extern "C" fn mem_usage2(
    _ctx: *mut raw::RedisModuleKeyOptCtx,
    value: *const c_void,
    _sample_size: usize,
) -> usize {
    unsafe {
        // SAFETY: value was allocated by Box::into_raw(Box::new(FlashStringObject {...}))
        // and remains valid for the duration of this call (Valkey holds a read lock on
        // the key). Cast to shared reference is safe; no mutation occurs.
        let obj = &*value.cast::<FlashStringObject>();
        match &obj.tier {
            Tier::Hot(v) => std::mem::size_of::<FlashStringObject>() + v.len(),
            Tier::Cold { .. } => std::mem::size_of::<FlashStringObject>(),
        }
    }
}

/// # Safety
///
/// Serialise a `FlashStringObject` into the RDB stream.
///
/// Wire format (spec #13 v1):
///   [u64 encoding_version = 1][u64 shape_tag = 0x01][i64 ttl_ms|-1][string_buffer value]
///
/// Cold-tier objects: NVMe fetch is not possible here because `rdb_save` does not
/// receive the key. No code currently transitions keys to `Tier::Cold`, so this
/// branch is unreachable today. If reached, an empty value is written with a
/// warning — future work (demotion) must store the key in `Tier::Cold`.
pub unsafe extern "C" fn rdb_save(io: *mut raw::RedisModuleIO, value: *mut c_void) {
    unsafe {
        // SAFETY: value was allocated by Box::into_raw(Box::new(FlashStringObject {...}))
        // and remains valid for the duration of this call (Valkey holds a read lock).
        let obj = &*value.cast::<FlashStringObject>();

        raw::save_unsigned(io, ENCODING_VERSION as u64);
        raw::save_unsigned(io, SHAPE_TAG_STRING);

        let ttl = obj.ttl_ms.unwrap_or(TTL_NONE_SENTINEL);
        raw::save_signed(io, ttl);

        match &obj.tier {
            Tier::Hot(v) => {
                raw::save_slice(io, v);
            }
            Tier::Cold {
                backend_offset,
                value_len,
                ..
            } => {
                // Fetch the value from NVMe using the offset stored in the Cold variant.
                match crate::STORAGE
                    .get()
                    .and_then(|s| s.read_at_offset(*backend_offset, *value_len).ok())
                {
                    Some(bytes) => raw::save_slice(io, &bytes),
                    None => {
                        logging::log_warning(
                            "flash: rdb_save on Tier::Cold: NVMe read failed; writing empty bytes",
                        );
                        raw::save_slice(io, &[]);
                    }
                }
            }
        }
    }
}

// ── Pure-Rust RDB payload parser ──────────────────────────────────────────────
//
// Flat wire format for the pure parser (mirrors rdb_save field order):
//   [u64-LE encoding_version][u64-LE shape_tag][i64-LE ttl_ms][u64-LE value_len][bytes]
//
// `rdb_load` reads these same fields from Valkey's ModuleIO (using FFI helpers)
// and delegates the validation + construction work to `build_rdb_string`, so the
// fuzz target exercises the exact same validation paths as production loads.

// 512 MiB — matches Redis/Valkey's max string value size.
const MAX_VALUE_BYTES: usize = 512 * 1024 * 1024;

/// Errors returned by [`parse_rdb_payload`] and [`build_rdb_string`].
#[derive(Debug, PartialEq)]
pub enum RdbParseError {
    Truncated,
    UnsupportedVersion(u64),
    UnexpectedTag(u64),
    ValueTooLarge(usize),
}

impl std::fmt::Display for RdbParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RdbParseError::Truncated => write!(f, "truncated RDB payload"),
            RdbParseError::UnsupportedVersion(v) => {
                write!(
                    f,
                    "unsupported encoding version {v} (max {ENCODING_VERSION})"
                )
            }
            RdbParseError::UnexpectedTag(t) => write!(
                f,
                "unexpected shape_tag {t:#04x} (expected {SHAPE_TAG_STRING:#04x})"
            ),
            RdbParseError::ValueTooLarge(n) => {
                write!(f, "value too large: {n} bytes (max {MAX_VALUE_BYTES})")
            }
        }
    }
}

/// Core validation and construction for FlashString RDB data.
///
/// Accepts already-decoded primitives (as returned by Valkey's IO helpers or by
/// `parse_rdb_payload`'s flat-LE decoder) and applies all invariant checks.
/// Both `rdb_load` and the fuzz harness call this function, ensuring fuzz
/// coverage is over the canonical validation code rather than a parallel copy.
pub fn build_rdb_string(
    version: u64,
    tag: u64,
    ttl_raw: i64,
    value: Vec<u8>,
) -> Result<FlashStringObject, RdbParseError> {
    if version > ENCODING_VERSION as u64 {
        return Err(RdbParseError::UnsupportedVersion(version));
    }
    if tag != SHAPE_TAG_STRING {
        return Err(RdbParseError::UnexpectedTag(tag));
    }
    if value.len() > MAX_VALUE_BYTES {
        return Err(RdbParseError::ValueTooLarge(value.len()));
    }
    let ttl_ms = if ttl_raw == TTL_NONE_SENTINEL {
        None
    } else {
        Some(ttl_raw)
    };
    Ok(FlashStringObject {
        tier: Tier::Hot(value),
        ttl_ms,
    })
}

/// Parse a FlashString RDB payload from a flat LE byte buffer.
///
/// Wire format: `[u64-LE version][u64-LE shape_tag][i64-LE ttl_ms][u64-LE value_len][bytes]`
///
/// This is the entry point for the fuzz harness. It exercises the same
/// [`build_rdb_string`] validation logic that [`rdb_load`] calls in production.
pub fn parse_rdb_payload(data: &[u8]) -> Result<FlashStringObject, RdbParseError> {
    use std::io::{Cursor, Read};
    let mut cur = Cursor::new(data);

    macro_rules! ru64 {
        () => {{
            let mut b = [0u8; 8];
            cur.read_exact(&mut b)
                .map_err(|_| RdbParseError::Truncated)?;
            u64::from_le_bytes(b)
        }};
    }
    macro_rules! ri64 {
        () => {{
            let mut b = [0u8; 8];
            cur.read_exact(&mut b)
                .map_err(|_| RdbParseError::Truncated)?;
            i64::from_le_bytes(b)
        }};
    }

    let version = ru64!();
    let tag = ru64!();
    let ttl_raw = ri64!();
    let val_len = ru64!() as usize;
    if val_len > MAX_VALUE_BYTES {
        return Err(RdbParseError::ValueTooLarge(val_len));
    }
    let remaining = (data.len() as u64).saturating_sub(cur.position()) as usize;
    if val_len > remaining {
        return Err(RdbParseError::Truncated);
    }
    let mut value = vec![0u8; val_len];
    cur.read_exact(&mut value)
        .map_err(|_| RdbParseError::Truncated)?;

    build_rdb_string(version, tag, ttl_raw, value)
}

/// # Safety
///
/// Deserialise a `FlashStringObject` from the RDB stream. Returns a raw pointer
/// to a heap-allocated object owned by Valkey, or `null_mut()` on any error.
///
/// All loaded values come back as `Tier::Hot` — cold tiering happens later via
/// the eviction path.
pub unsafe extern "C" fn rdb_load(io: *mut raw::RedisModuleIO, encver: i32) -> *mut c_void {
    // Reject data saved by a future, incompatible module version.
    if encver > ENCODING_VERSION {
        logging::log_warning(
            format!(
                "flash: rdb_load: unsupported module encoding version {encver} \
                 (max supported: {ENCODING_VERSION})"
            )
            .as_str(),
        );
        return null_mut();
    }

    // Read all fields from Valkey's ModuleIO, then delegate to build_rdb_string
    // so the same validation runs in both production loads and the fuzz harness.
    let version = match raw::load_unsigned(io) {
        Ok(v) => v,
        Err(_) => {
            logging::log_warning("flash: rdb_load: short read on encoding_version");
            return null_mut();
        }
    };
    let tag = match raw::load_unsigned(io) {
        Ok(t) => t,
        Err(_) => {
            logging::log_warning("flash: rdb_load: short read on shape_tag");
            return null_mut();
        }
    };
    let ttl_raw = match raw::load_signed(io) {
        Ok(t) => t,
        Err(_) => {
            logging::log_warning("flash: rdb_load: short read on ttl_ms");
            return null_mut();
        }
    };
    let value = match raw::load_string_buffer(io) {
        Ok(buf) => buf.as_ref().to_vec(),
        Err(_) => {
            logging::log_warning("flash: rdb_load: short read on value bytes");
            return null_mut();
        }
    };

    match build_rdb_string(version, tag, ttl_raw, value) {
        Ok(obj) => {
            // SAFETY: Box::into_raw transfers ownership to Valkey's keyspace.
            // Valkey calls our free() callback exactly once on key deletion.
            Box::into_raw(Box::new(obj)).cast::<c_void>()
        }
        Err(e) => {
            logging::log_warning(format!("flash: rdb_load: {e}").as_str());
            null_mut()
        }
    }
}

/// # Safety
///
/// Emit a `FLASH.SET key value [PXAT <ms>]` command into the AOF rewrite buffer.
///
/// Cold-tier limitation (v1): `aof_rewrite` receives no key bytes and no NVMe
/// offset, so cold objects cannot be reconstructed. Unlike `rdb_save` (which
/// has the byte offset in `Tier::Cold` and can call `read_at_offset`), AOF
/// rewrite has no equivalent hook. Keys demoted via `FLASH.DEBUG.DEMOTE` will
/// reach this branch — a warning is logged and the key is skipped.
pub unsafe extern "C" fn aof_rewrite(
    aof: *mut raw::RedisModuleIO,
    key: *mut raw::RedisModuleString,
    value: *mut c_void,
) {
    unsafe {
        let obj = &*value.cast::<FlashStringObject>();

        let bytes = match &obj.tier {
            Tier::Hot(v) => v.as_slice(),
            Tier::Cold { .. } => {
                logging::log_warning(
                    "flash: aof_rewrite on Tier::Cold object — cannot fetch from NVMe without key; \
                 skipping key (task #57)",
                );
                return;
            }
        };

        let cmd = match CString::new("FLASH.SET") {
            Ok(s) => s,
            Err(_) => return,
        };

        let emit = match raw::RedisModule_EmitAOF {
            Some(f) => f,
            None => {
                logging::log_warning("flash: aof_rewrite: RedisModule_EmitAOF is null");
                return;
            }
        };

        match obj.ttl_ms {
            None => {
                // FLASH.SET key value
                let fmt = match CString::new("sb") {
                    Ok(s) => s,
                    Err(_) => return,
                };
                emit(
                    aof,
                    cmd.as_ptr(),
                    fmt.as_ptr(),
                    key,
                    bytes.as_ptr().cast::<c_char>(),
                    bytes.len(),
                );
            }
            Some(ttl) => {
                // FLASH.SET key value PXAT <absolute-ms>
                let fmt = match CString::new("sbcl") {
                    Ok(s) => s,
                    Err(_) => return,
                };
                let pxat = match CString::new("PXAT") {
                    Ok(s) => s,
                    Err(_) => return,
                };
                emit(
                    aof,
                    cmd.as_ptr(),
                    fmt.as_ptr(),
                    key,
                    bytes.as_ptr().cast::<c_char>(),
                    bytes.len(),
                    pxat.as_ptr(),
                    ttl as std::os::raw::c_longlong,
                );
            }
        }
    }
}

/// # Safety
pub unsafe extern "C" fn digest(_md: *mut raw::RedisModuleDigest, _value: *mut c_void) {
    // stub — digest not yet implemented
}

/// # Safety
///
/// Called by Valkey's COPY/OBJECT COPY to deep-copy a FlashStringObject.
///
/// ## Pointer contract
/// - `value` — const pointer to the source `FlashStringObject`; must not be mutated.
/// - Returns an owned `*mut FlashStringObject` allocated via `Box::into_raw` on success,
///   or `null_mut()` on failure. A non-null return is consumed (and eventually freed) by
///   Valkey via the type's `free` callback when the destination key is deleted.
///
/// ## Cold-tier strategy (v1)
/// Cold objects are materialised via a synchronous NVMe read and returned as a Hot copy.
/// This matches the RDB-load convention (rdb_load always returns Hot). A v2 optimisation
/// could allocate new NVMe blocks and perform an NVMe→NVMe copy without promoting to Hot.
///
/// ## TTL handling
/// `ttl_ms` is copied verbatim from source to destination so that AOF rewrite for the
/// new key emits the correct PEXPIREAT command. Valkey core handles the runtime key-level
/// expiry separately (objectGetExpire/setExpire in copyCommand).
pub unsafe extern "C" fn copy(
    _from_key: *mut RedisModuleString,
    to_key: *mut RedisModuleString,
    value: *const c_void,
) -> *mut c_void {
    unsafe {
        // COPY REPLACE: Valkey deletes the destination after this callback returns
        // a new object. The module's in-memory cache is keyed by raw bytes and is
        // not otherwise invalidated, so a subsequent FLASH.GET would serve the
        // pre-REPLACE destination value. Drop the cache entry for `to_key` here
        // so the new object's value is authoritative.
        if let Some(cache) = crate::CACHE.get() {
            let to_key_bytes = crate::util::module_string_bytes(to_key);
            cache.delete(&to_key_bytes);
        }
        let src: &FlashStringObject = &*value.cast::<FlashStringObject>();
        match &src.tier {
            Tier::Hot(v) => {
                let new_obj = Box::new(FlashStringObject {
                    tier: Tier::Hot(v.clone()),
                    ttl_ms: src.ttl_ms,
                });
                Box::into_raw(new_obj).cast::<c_void>()
            }
            Tier::Cold {
                backend_offset,
                value_len,
                ..
            } => {
                // Materialise cold value via synchronous NVMe read → return Hot copy.
                // Null return signals COPY failure to Valkey core; source is untouched.
                #[cfg(not(test))]
                {
                    let storage = match crate::STORAGE.get() {
                        Some(s) => s,
                        None => return null_mut(),
                    };
                    match storage.read_at_offset(*backend_offset, *value_len) {
                        Ok(bytes) => {
                            let new_obj = Box::new(FlashStringObject {
                                tier: Tier::Hot(bytes),
                                ttl_ms: src.ttl_ms,
                            });
                            Box::into_raw(new_obj).cast::<c_void>()
                        }
                        Err(_) => null_mut(),
                    }
                }
                #[cfg(test)]
                {
                    let _ = (backend_offset, value_len);
                    null_mut()
                }
            }
        }
    }
}

/// # Safety
///
/// Called by Valkey's active-defrag mechanism to relocate a FlashStringObject
/// to a less-fragmented heap arena.
///
/// ## Pointer contract
/// - `ctx`   — valid `RedisModuleDefragCtx`; remains valid for the call's duration.
/// - `value` — double-pointer (`*mut *mut FlashStringObject`). Writing `*value`
///   updates Valkey's stored reference when the struct itself is relocated.
/// - Returns 0 (complete). Cursor-based resumption is not needed because
///   FlashStringObject has at most one inner allocation (the Hot `Vec<u8>`).
///
/// ## `alloc()` semantics
/// `defrag.alloc(ptr)` calls `je_defrag_alloc`:
/// - Returns **null** → allocation is already well-placed; `ptr` remains valid.
/// - Returns **non-null** → data was copied to the new address and the old
///   allocation was freed by jemalloc. The caller **must not** dereference `ptr`
///   again; the returned pointer is now the sole valid reference.
///
/// ## No-concurrent-access guarantee
/// Valkey's active-defrag runs on the main event-loop thread under the GIL.
/// No command handler executes concurrently with this callback.
/// Async I/O completion handlers (hot-promotion via background NVMe reads) only
/// write to `FlashCache`; they hold no pointer to the keyspace object. It is
/// therefore safe to relocate or mutate the object without extra synchronisation.
pub unsafe extern "C" fn defrag(
    ctx: *mut RedisModuleDefragCtx,
    _key: *mut RedisModuleString,
    value: *mut *mut c_void,
) -> i32 {
    unsafe {
        use std::mem;
        use valkey_module::defrag::Defrag;

        let dfg = Defrag::new(ctx);

        // ── Step 1: relocate the FlashStringObject struct itself ──────────────────
        // If alloc() returns non-null, the struct was moved; old *value is freed.
        // Writing *value here redirects Valkey's internal key→object pointer.
        let new_struct = dfg.alloc(*value);
        if !new_struct.is_null() {
            *value = new_struct;
        }

        // ── Step 2: for Hot tier, relocate the Vec<u8> backing buffer ─────────────
        // Cold tier holds only primitive scalars (key_hash, offset, counts) — no
        // heap pointer to defrag. A Vec with capacity 0 uses a dangling sentinel
        // pointer that is not a real heap allocation; skip it.
        let obj: &mut FlashStringObject = &mut *(*value).cast::<FlashStringObject>();
        if let Tier::Hot(ref mut vec) = obj.tier
            && vec.capacity() > 0
        {
            let new_buf = dfg.alloc(vec.as_mut_ptr().cast::<c_void>());
            if !new_buf.is_null() {
                // The buffer was relocated. Rebuild the Vec at the new address.
                // `mem::forget` on the old Vec prevents its Drop impl from
                // running (which would call `dealloc` on the already-freed ptr).
                let old_len = vec.len();
                let old_cap = vec.capacity();
                let old = mem::replace(
                    vec,
                    Vec::from_raw_parts(new_buf.cast::<u8>(), old_len, old_cap),
                );
                mem::forget(old);
            }
        }

        0 // defrag complete — single-allocation types never need cursor resumption
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoding_version_is_one() {
        assert_eq!(ENCODING_VERSION, 1);
    }

    #[test]
    fn shape_tag_string_is_0x01() {
        assert_eq!(SHAPE_TAG_STRING, 0x01);
    }

    #[test]
    fn ttl_none_sentinel_is_negative_one() {
        assert_eq!(TTL_NONE_SENTINEL, -1);
    }

    #[test]
    fn ttl_sentinel_is_negative_and_below_any_valid_timestamp() {
        // Valid epoch-ms timestamps are large positive integers; the sentinel is -1.
        const { assert!(TTL_NONE_SENTINEL < 0) };
        const { assert!(1_700_000_000_000_i64 > TTL_NONE_SENTINEL) };
    }

    #[test]
    fn ttl_sentinel_roundtrip() {
        // Verify the sentinel → None → sentinel identity.
        let sentinel_to_opt = |v: i64| {
            if v == TTL_NONE_SENTINEL {
                None
            } else {
                Some(v)
            }
        };
        assert_eq!(sentinel_to_opt(TTL_NONE_SENTINEL), None::<i64>);
        assert_eq!(
            sentinel_to_opt(1_700_000_000_000),
            Some(1_700_000_000_000_i64)
        );
    }

    #[test]
    fn flash_string_object_hot() {
        let obj = FlashStringObject {
            tier: Tier::Hot(b"hello".to_vec()),
            ttl_ms: None,
        };
        assert_eq!(obj.tier, Tier::Hot(b"hello".to_vec()));
        assert!(obj.ttl_ms.is_none());
    }

    #[test]
    fn flash_string_object_cold() {
        let obj = FlashStringObject {
            tier: Tier::Cold {
                key_hash: 0xdeadbeef,
                backend_offset: 4096,
                num_blocks: 1,
                value_len: 5,
            },
            ttl_ms: Some(5000),
        };
        assert!(matches!(obj.tier, Tier::Cold { .. }));
        assert_eq!(obj.ttl_ms, Some(5000));
    }

    #[test]
    fn mem_usage2_hot_reports_struct_plus_value_bytes() {
        let value = b"hello world".to_vec();
        let value_len = value.len();
        let obj = Box::new(FlashStringObject {
            tier: Tier::Hot(value),
            ttl_ms: None,
        });
        let ptr = Box::into_raw(obj) as *const c_void;
        // SAFETY: ptr is freshly allocated, valid for the duration of this test.
        let size = unsafe { mem_usage2(std::ptr::null_mut(), ptr, 0) };
        // SAFETY: ptr was allocated by Box::into_raw above and has not been freed.
        unsafe { drop(Box::from_raw(ptr as *mut FlashStringObject)) };
        assert_eq!(size, std::mem::size_of::<FlashStringObject>() + value_len);
    }

    #[test]
    fn mem_usage2_cold_reports_struct_only() {
        let obj = Box::new(FlashStringObject {
            tier: Tier::Cold {
                key_hash: 0,
                backend_offset: 0,
                num_blocks: 1,
                value_len: 0,
            },
            ttl_ms: None,
        });
        let ptr = Box::into_raw(obj) as *const c_void;
        // SAFETY: ptr is freshly allocated, valid for the duration of this test.
        let size = unsafe { mem_usage2(std::ptr::null_mut(), ptr, 0) };
        // SAFETY: ptr was allocated by Box::into_raw above and has not been freed.
        unsafe { drop(Box::from_raw(ptr as *mut FlashStringObject)) };
        assert_eq!(size, std::mem::size_of::<FlashStringObject>());
    }

    #[test]
    fn mem_usage2_empty_hot_value() {
        let obj = Box::new(FlashStringObject {
            tier: Tier::Hot(vec![]),
            ttl_ms: None,
        });
        let ptr = Box::into_raw(obj) as *const c_void;
        // SAFETY: ptr is freshly allocated via Box::into_raw, valid for the duration of this test.
        let size = unsafe { mem_usage2(std::ptr::null_mut(), ptr, 0) };
        // SAFETY: ptr was allocated by Box::into_raw above and has not been freed.
        unsafe { drop(Box::from_raw(ptr as *mut FlashStringObject)) };
        assert_eq!(size, std::mem::size_of::<FlashStringObject>());
    }

    #[test]
    fn struct_size_regression() {
        // Catches accidental layout growth. Update the bound if a deliberate field is added.
        const { assert!(std::mem::size_of::<FlashStringObject>() <= 64) };
    }
}
