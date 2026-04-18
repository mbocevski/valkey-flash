use std::collections::HashMap;
use std::os::raw::{c_int, c_void};
use std::ptr::null_mut;
use valkey_module::native_types::ValkeyType;
use valkey_module::{logging, raw, RedisModuleDefragCtx, RedisModuleString};

// ── FlashHashObject ───────────────────────────────────────────────────────────

pub struct FlashHashObject {
    pub fields: HashMap<Vec<u8>, Vec<u8>>,
    pub ttl_ms: Option<i64>,
}

// ── Type registration ─────────────────────────────────────────────────────────

const ENCODING_VERSION: i32 = 1;

// "flashhsh1" is exactly 9 chars — satisfies the module-type-id constraint.
pub static FLASH_HASH_TYPE: ValkeyType = ValkeyType::new(
    "flashhsh1",
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
    // SAFETY: value was allocated by Box::into_raw(Box::new(FlashHashObject {...}))
    // in a command handler. Valkey calls this callback exactly once per key
    // deletion / eviction — never while the key is still accessible.
    drop(Box::from_raw(value.cast::<FlashHashObject>()));
}

/// # Safety
pub unsafe extern "C" fn rdb_save(rdb: *mut raw::RedisModuleIO, _value: *mut c_void) {
    // stub — RDB persistence not yet implemented (task #25)
    logging::log_warning("flash: rdb_save for FlashHash is a stub; key will not persist");
    raw::save_unsigned(rdb, 0);
}

/// # Safety
pub unsafe extern "C" fn rdb_load(rdb: *mut raw::RedisModuleIO, _encver: c_int) -> *mut c_void {
    // stub — RDB persistence not yet implemented (task #25)
    logging::log_warning("flash: rdb_load for FlashHash is a stub; returning null");
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
    let obj = &*value.cast::<FlashHashObject>();
    let fields_bytes: usize = obj.fields.iter().map(|(k, v)| k.len() + v.len()).sum();
    std::mem::size_of::<FlashHashObject>() + fields_bytes
}

/// # Safety
pub unsafe extern "C" fn copy(
    _from_key: *mut RedisModuleString,
    _to_key: *mut RedisModuleString,
    _value: *const c_void,
) -> *mut c_void {
    // stub — COPY not yet implemented (task #25)
    logging::log_warning("flash: copy for FlashHash is a stub; returning null");
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
    fn flash_hash_object_construction_empty() {
        let obj = FlashHashObject {
            fields: HashMap::new(),
            ttl_ms: None,
        };
        assert!(obj.fields.is_empty());
        assert!(obj.ttl_ms.is_none());
    }

    #[test]
    fn flash_hash_object_construction_with_fields() {
        let mut fields = HashMap::new();
        fields.insert(b"name".to_vec(), b"alice".to_vec());
        let obj = FlashHashObject {
            fields,
            ttl_ms: Some(10_000),
        };
        assert_eq!(obj.fields.get(b"name".as_ref()), Some(&b"alice".to_vec()));
        assert_eq!(obj.ttl_ms, Some(10_000));
    }
}
