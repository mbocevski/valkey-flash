use std::os::raw::c_void;
use std::ptr::null_mut;
use valkey_module::native_types::ValkeyType;
use valkey_module::{logging, raw, RedisModuleDefragCtx, RedisModuleString};

use super::Tier;

// ── FlashStringObject ─────────────────────────────────────────────────────────

pub struct FlashStringObject {
    pub tier: Tier<Vec<u8>>,
    pub ttl_ms: Option<i64>,
}

// ── Type registration ─────────────────────────────────────────────────────────

const ENCODING_VERSION: i32 = 1;

// "flashstr1" is exactly 9 chars — satisfies the module-type-id constraint.
pub static FLASH_STRING_TYPE: ValkeyType = ValkeyType::new(
    "flashstr1",
    ENCODING_VERSION,
    raw::RedisModuleTypeMethods {
        version: raw::REDISMODULE_TYPE_METHOD_VERSION as u64,
        // rdb_save/rdb_load are None until task #25: a registered rdb_load returning
        // null_mut() triggers rdbReportCorruptRDB → server exit (rdb.c:2942).
        rdb_load: None,
        rdb_save: None,
        aof_rewrite: Some(aof_rewrite),
        digest: Some(digest),
        // mem_usage (v1) intentionally None; real accounting is in mem_usage2 (v2).
        mem_usage: None,
        free: Some(free),
        aux_load: None,
        aux_save: None,
        aux_save2: None,
        aux_save_triggers: 0,
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
pub unsafe extern "C" fn aof_rewrite(
    _aof: *mut raw::RedisModuleIO,
    _key: *mut raw::RedisModuleString,
    _value: *mut c_void,
) {
    // stub — AOF rewrite not yet implemented (task #25)
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
    // stub — COPY not yet implemented (task #25)
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
        let size = unsafe { mem_usage2(std::ptr::null_mut(), ptr, 0) };
        unsafe { drop(Box::from_raw(ptr as *mut FlashStringObject)) };
        assert_eq!(size, std::mem::size_of::<FlashStringObject>());
    }

    #[test]
    fn struct_size_regression() {
        // Catches accidental layout growth. Update the bound if a deliberate field is added.
        const { assert!(std::mem::size_of::<FlashStringObject>() <= 64) };
    }
}
