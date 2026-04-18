use std::os::raw::{c_int, c_void};
use std::ptr::null_mut;
use valkey_module::native_types::ValkeyType;
use valkey_module::{logging, raw, RedisModuleDefragCtx, RedisModuleString};

// ── FlashStringObject ─────────────────────────────────────────────────────────

pub struct FlashStringObject {
    pub value: Vec<u8>,
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
        rdb_load: Some(rdb_load),
        rdb_save: Some(rdb_save),
        aof_rewrite: Some(aof_rewrite),
        digest: Some(digest),
        mem_usage: Some(mem_usage),
        free: Some(free),
        aux_load: None,
        aux_save: None,
        aux_save2: None,
        aux_save_triggers: 0,
        free_effort: None,
        unlink: None,
        copy: Some(copy),
        defrag: Some(defrag),
        mem_usage2: None,
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
pub unsafe extern "C" fn rdb_save(rdb: *mut raw::RedisModuleIO, _value: *mut c_void) {
    // stub — RDB persistence not yet implemented (task #25)
    logging::log_warning("flash: rdb_save for FlashString is a stub; key will not persist");
    // Write a version sentinel so a future rdb_load can detect stub-saved data.
    raw::save_unsigned(rdb, 0);
}

/// # Safety
pub unsafe extern "C" fn rdb_load(rdb: *mut raw::RedisModuleIO, _encver: c_int) -> *mut c_void {
    // stub — RDB persistence not yet implemented (task #25)
    logging::log_warning("flash: rdb_load for FlashString is a stub; returning null");
    // Consume the sentinel written by rdb_save to keep the stream consistent.
    let _ = raw::load_unsigned(rdb);
    null_mut()
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
pub unsafe extern "C" fn mem_usage(value: *const c_void) -> usize {
    let obj = &*value.cast::<FlashStringObject>();
    std::mem::size_of::<FlashStringObject>() + obj.value.len()
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
    fn flash_string_object_construction() {
        let obj = FlashStringObject {
            value: b"hello".to_vec(),
            ttl_ms: None,
        };
        assert_eq!(obj.value, b"hello");
        assert!(obj.ttl_ms.is_none());
    }

    #[test]
    fn flash_string_object_with_ttl() {
        let obj = FlashStringObject {
            value: b"world".to_vec(),
            ttl_ms: Some(5000),
        };
        assert_eq!(obj.ttl_ms, Some(5000));
    }
}
