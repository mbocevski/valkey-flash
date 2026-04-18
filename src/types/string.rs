use std::ffi::CString;
use std::os::raw::{c_char, c_void};
use std::ptr::null_mut;
use valkey_module::native_types::ValkeyType;
use valkey_module::{logging, raw, RedisModuleDefragCtx, RedisModuleString};

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
    // SAFETY: value was allocated by Box::into_raw(Box::new(FlashStringObject {...}))
    // in a command handler. Valkey calls this callback exactly once per key
    // deletion / eviction — never while the key is still accessible.
    drop(Box::from_raw(value.cast::<FlashStringObject>()));
}

/// # Safety
pub unsafe extern "C" fn mem_usage2(
    _ctx: *mut raw::RedisModuleKeyOptCtx,
    value: *const c_void,
    _sample_size: usize,
) -> usize {
    // SAFETY: value was allocated by Box::into_raw(Box::new(FlashStringObject {...}))
    // and remains valid for the duration of this call (Valkey holds a read lock on
    // the key). Cast to shared reference is safe; no mutation occurs.
    let obj = &*value.cast::<FlashStringObject>();
    match &obj.tier {
        Tier::Hot(v) => std::mem::size_of::<FlashStringObject>() + v.len(),
        Tier::Cold { .. } => std::mem::size_of::<FlashStringObject>(),
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
        Tier::Cold { .. } => {
            // No key available here; cannot fetch from NVMe. Log and write empty
            // bytes — the loaded key will have an empty value.
            logging::log_warning(
                "flash: rdb_save on Tier::Cold object — value cannot be materialised; \
                 writing empty bytes (demotion support required)",
            );
            raw::save_slice(io, &[]);
        }
    }
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

    // ── Read encoding_version ──────────────────────────────────────────────
    let version = match raw::load_unsigned(io) {
        Ok(v) => v,
        Err(_) => {
            logging::log_warning("flash: rdb_load: short read on encoding_version");
            return null_mut();
        }
    };
    if version > ENCODING_VERSION as u64 {
        logging::log_warning(
            format!(
                "flash: rdb_load: unsupported in-stream encoding_version {version} \
                 (max supported: {ENCODING_VERSION})"
            )
            .as_str(),
        );
        return null_mut();
    }

    // ── Read shape_tag ─────────────────────────────────────────────────────
    let tag = match raw::load_unsigned(io) {
        Ok(t) => t,
        Err(_) => {
            logging::log_warning("flash: rdb_load: short read on shape_tag");
            return null_mut();
        }
    };
    if tag != SHAPE_TAG_STRING {
        logging::log_warning(
            format!(
                "flash: rdb_load: unexpected shape_tag {tag:#04x} \
                 (expected {SHAPE_TAG_STRING:#04x})"
            )
            .as_str(),
        );
        return null_mut();
    }

    // ── Read ttl_ms ────────────────────────────────────────────────────────
    let ttl_raw = match raw::load_signed(io) {
        Ok(t) => t,
        Err(_) => {
            logging::log_warning("flash: rdb_load: short read on ttl_ms");
            return null_mut();
        }
    };
    let ttl_ms = if ttl_raw == TTL_NONE_SENTINEL {
        None
    } else {
        Some(ttl_raw)
    };

    // ── Read value bytes ───────────────────────────────────────────────────
    let value = match raw::load_string_buffer(io) {
        Ok(buf) => buf.as_ref().to_vec(),
        Err(_) => {
            logging::log_warning("flash: rdb_load: short read on value bytes");
            return null_mut();
        }
    };

    let obj = Box::new(FlashStringObject {
        tier: Tier::Hot(value),
        ttl_ms,
    });

    // SAFETY: Box::into_raw transfers ownership to Valkey's keyspace. Valkey will
    // call the free() callback exactly once when the key is deleted or evicted,
    // which drops the box via `drop(Box::from_raw(...))`.
    Box::into_raw(obj).cast::<c_void>()
}

/// # Safety
///
/// Emit a `FLASH.SET key value [PXAT <ms>]` command into the AOF rewrite buffer.
///
/// Cold-tier limitation (v1): `aof_rewrite` receives no key bytes, so cold
/// objects cannot be fetched from NVMe. Since no code currently creates
/// `Tier::Cold` objects, this branch is unreachable. If reached, a warning is
/// logged and the key is skipped (tracked in task #57).
pub unsafe extern "C" fn aof_rewrite(
    aof: *mut raw::RedisModuleIO,
    key: *mut raw::RedisModuleString,
    value: *mut c_void,
) {
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

/// # Safety
pub unsafe extern "C" fn digest(_md: *mut raw::RedisModuleDigest, _value: *mut c_void) {
    // stub — digest not yet implemented
}

/// # Safety
pub unsafe extern "C" fn copy(
    _from_key: *mut RedisModuleString,
    _to_key: *mut RedisModuleString,
    _value: *const c_void,
) -> *mut c_void {
    // stub — COPY not yet implemented
    logging::log_warning("flash: copy for FlashString is a stub; returning null");
    null_mut()
}

/// # Safety
pub unsafe extern "C" fn defrag(
    _ctx: *mut RedisModuleDefragCtx,
    _key: *mut RedisModuleString,
    _value: *mut *mut c_void,
) -> i32 {
    // stub — defrag not yet implemented
    0
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
            tier: Tier::Cold { size_hint: 1024 },
            ttl_ms: Some(5000),
        };
        assert!(matches!(obj.tier, Tier::Cold { size_hint: 1024 }));
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
            tier: Tier::Cold { size_hint: 4096 },
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
