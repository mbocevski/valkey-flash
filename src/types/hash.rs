use std::collections::HashMap;
use std::os::raw::c_void;
use std::ptr::null_mut;
use valkey_module::native_types::ValkeyType;
use valkey_module::{logging, raw, RedisModuleDefragCtx, RedisModuleString};

use super::Tier;

// ── Serialization ─────────────────────────────────────────────────────────────

/// Encode a hash map to bytes for NVMe storage and cache.
/// Wire format: [u32 count] then per-field [u32 klen][key][u32 vlen][val].
/// Keys are sorted for a deterministic byte sequence (stable value_hash).
pub fn hash_serialize(fields: &HashMap<Vec<u8>, Vec<u8>>) -> Vec<u8> {
    let mut pairs: Vec<(&Vec<u8>, &Vec<u8>)> = fields.iter().collect();
    pairs.sort_unstable_by_key(|(a, _)| *a);
    let capacity = 4 + pairs
        .iter()
        .map(|(k, v)| 8 + k.len() + v.len())
        .sum::<usize>();
    let mut buf = Vec::with_capacity(capacity);
    buf.extend_from_slice(&(pairs.len() as u32).to_le_bytes());
    for (k, v) in pairs {
        buf.extend_from_slice(&(k.len() as u32).to_le_bytes());
        buf.extend_from_slice(k);
        buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
        buf.extend_from_slice(v);
    }
    buf
}

/// Decode bytes (from NVMe or cache) back to a hash map.
/// Returns `None` on any structural corruption.
pub fn hash_deserialize(bytes: &[u8]) -> Option<HashMap<Vec<u8>, Vec<u8>>> {
    if bytes.len() < 4 {
        return None;
    }
    let count = u32::from_le_bytes(bytes[..4].try_into().ok()?) as usize;
    let mut map = HashMap::with_capacity(count);
    let mut pos = 4usize;
    for _ in 0..count {
        if pos + 4 > bytes.len() {
            return None;
        }
        let klen = u32::from_le_bytes(bytes[pos..pos + 4].try_into().ok()?) as usize;
        pos += 4;
        if pos + klen > bytes.len() {
            return None;
        }
        let k = bytes[pos..pos + klen].to_vec();
        pos += klen;
        if pos + 4 > bytes.len() {
            return None;
        }
        let vlen = u32::from_le_bytes(bytes[pos..pos + 4].try_into().ok()?) as usize;
        pos += 4;
        if pos + vlen > bytes.len() {
            return None;
        }
        let v = bytes[pos..pos + vlen].to_vec();
        pos += vlen;
        map.insert(k, v);
    }
    if pos != bytes.len() {
        return None;
    }
    Some(map)
}

// ── FlashHashObject ───────────────────────────────────────────────────────────

pub struct FlashHashObject {
    pub tier: Tier<HashMap<Vec<u8>, Vec<u8>>>,
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
    // SAFETY: value was allocated by Box::into_raw(Box::new(FlashHashObject {...}))
    // in a command handler. Valkey calls this callback exactly once per key
    // deletion / eviction — never while the key is still accessible.
    let obj = Box::from_raw(value.cast::<FlashHashObject>());
    if let Tier::Cold {
        key_hash,
        backend_offset,
        num_blocks,
        ..
    } = obj.tier
    {
        if let Some(storage) = crate::STORAGE.get() {
            storage.release_cold_blocks(backend_offset, num_blocks);
        }
        if let Some(wal) = crate::WAL.get() {
            let _ = wal.append(crate::storage::wal::WalOp::Delete { key_hash });
        }
        if let Ok(mut map) = crate::TIERING_MAP.lock() {
            map.remove(&key_hash);
        }
    }
}

/// # Safety
pub unsafe extern "C" fn mem_usage2(
    _ctx: *mut raw::RedisModuleKeyOptCtx,
    value: *const c_void,
    _sample_size: usize,
) -> usize {
    // SAFETY: value was allocated by Box::into_raw(Box::new(FlashHashObject {...}))
    // and remains valid for the duration of this call (Valkey holds a read lock on
    // the key). Cast to shared reference is safe; no mutation occurs.
    let obj = &*value.cast::<FlashHashObject>();
    match &obj.tier {
        Tier::Hot(fields) => {
            let fields_bytes: usize = fields.iter().map(|(k, v)| k.len() + v.len()).sum();
            std::mem::size_of::<FlashHashObject>() + fields_bytes
        }
        Tier::Cold { .. } => std::mem::size_of::<FlashHashObject>(),
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
    fn flash_hash_object_hot_empty() {
        let obj = FlashHashObject {
            tier: Tier::Hot(HashMap::new()),
            ttl_ms: None,
        };
        assert_eq!(obj.tier, Tier::Hot(HashMap::new()));
        assert!(obj.ttl_ms.is_none());
    }

    #[test]
    fn flash_hash_object_hot_with_fields() {
        let mut fields = HashMap::new();
        fields.insert(b"name".to_vec(), b"alice".to_vec());
        let obj = FlashHashObject {
            tier: Tier::Hot(fields),
            ttl_ms: Some(10_000),
        };
        if let Tier::Hot(ref f) = obj.tier {
            assert_eq!(f.get(b"name".as_ref()), Some(&b"alice".to_vec()));
        } else {
            panic!("expected Hot tier");
        }
        assert_eq!(obj.ttl_ms, Some(10_000));
    }

    #[test]
    fn flash_hash_object_cold() {
        let obj = FlashHashObject {
            tier: Tier::Cold {
                key_hash: 0xdeadbeef,
                backend_offset: 4096,
                num_blocks: 1,
                value_len: 64,
            },
            ttl_ms: Some(5000),
        };
        assert!(matches!(obj.tier, Tier::Cold { .. }));
        assert_eq!(obj.ttl_ms, Some(5000));
    }

    #[test]
    fn mem_usage2_hot_reports_struct_plus_field_bytes() {
        let mut fields = HashMap::new();
        fields.insert(b"key".to_vec(), b"value".to_vec()); // 3 + 5 = 8 bytes
        let obj = Box::new(FlashHashObject {
            tier: Tier::Hot(fields),
            ttl_ms: None,
        });
        let ptr = Box::into_raw(obj) as *const c_void;
        // SAFETY: ptr is freshly allocated, valid for the duration of this test.
        let size = unsafe { mem_usage2(std::ptr::null_mut(), ptr, 0) };
        // SAFETY: ptr was allocated by Box::into_raw above and has not been freed.
        unsafe { drop(Box::from_raw(ptr as *mut FlashHashObject)) };
        assert_eq!(size, std::mem::size_of::<FlashHashObject>() + 8);
    }

    #[test]
    fn mem_usage2_hot_empty_reports_struct_only() {
        let obj = Box::new(FlashHashObject {
            tier: Tier::Hot(HashMap::new()),
            ttl_ms: None,
        });
        let ptr = Box::into_raw(obj) as *const c_void;
        // SAFETY: ptr is freshly allocated via Box::into_raw, valid for the duration of this test.
        let size = unsafe { mem_usage2(std::ptr::null_mut(), ptr, 0) };
        // SAFETY: ptr was allocated by Box::into_raw above and has not been freed.
        unsafe { drop(Box::from_raw(ptr as *mut FlashHashObject)) };
        assert_eq!(size, std::mem::size_of::<FlashHashObject>());
    }

    #[test]
    fn mem_usage2_cold_reports_struct_only() {
        let obj = Box::new(FlashHashObject {
            tier: Tier::Cold {
                key_hash: 0,
                backend_offset: 0,
                num_blocks: 1,
                value_len: 0,
            },
            ttl_ms: None,
        });
        let ptr = Box::into_raw(obj) as *const c_void;
        // SAFETY: ptr is freshly allocated via Box::into_raw, valid for the duration of this test.
        let size = unsafe { mem_usage2(std::ptr::null_mut(), ptr, 0) };
        // SAFETY: ptr was allocated by Box::into_raw above and has not been freed.
        unsafe { drop(Box::from_raw(ptr as *mut FlashHashObject)) };
        assert_eq!(size, std::mem::size_of::<FlashHashObject>());
    }

    #[test]
    fn mem_usage2_hot_multi_field_sums_all_bytes() {
        let mut fields = HashMap::new();
        fields.insert(b"a".to_vec(), b"bb".to_vec()); // 1 + 2 = 3
        fields.insert(b"ccc".to_vec(), b"dddd".to_vec()); // 3 + 4 = 7
        let expected_field_bytes = 10usize;
        let obj = Box::new(FlashHashObject {
            tier: Tier::Hot(fields),
            ttl_ms: None,
        });
        let ptr = Box::into_raw(obj) as *const c_void;
        // SAFETY: ptr is freshly allocated via Box::into_raw, valid for the duration of this test.
        let size = unsafe { mem_usage2(std::ptr::null_mut(), ptr, 0) };
        // SAFETY: ptr was allocated by Box::into_raw above and has not been freed.
        unsafe { drop(Box::from_raw(ptr as *mut FlashHashObject)) };
        assert_eq!(
            size,
            std::mem::size_of::<FlashHashObject>() + expected_field_bytes
        );
    }

    #[test]
    fn struct_size_regression() {
        // Catches accidental layout growth. Update the bound if a deliberate field is added.
        const { assert!(std::mem::size_of::<FlashHashObject>() <= 80) };
    }

    // ── Codec tests ───────────────────────────────────────────────────────────

    #[test]
    fn empty_hash_roundtrip() {
        let m: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        let bytes = hash_serialize(&m);
        assert_eq!(hash_deserialize(&bytes), Some(HashMap::new()));
    }

    #[test]
    fn single_field_roundtrip() {
        let mut m = HashMap::new();
        m.insert(b"field".to_vec(), b"value".to_vec());
        let bytes = hash_serialize(&m);
        let m2 = hash_deserialize(&bytes).unwrap();
        assert_eq!(m2.get(b"field".as_ref()), Some(&b"value".to_vec()));
    }

    #[test]
    fn multi_field_roundtrip() {
        let mut m = HashMap::new();
        m.insert(b"a".to_vec(), b"1".to_vec());
        m.insert(b"b".to_vec(), b"2".to_vec());
        m.insert(b"c".to_vec(), b"3".to_vec());
        let bytes = hash_serialize(&m);
        let m2 = hash_deserialize(&bytes).unwrap();
        assert_eq!(m2, m);
    }

    #[test]
    fn serialize_is_deterministic() {
        let mut m = HashMap::new();
        m.insert(b"z".to_vec(), b"0".to_vec());
        m.insert(b"a".to_vec(), b"1".to_vec());
        let b1 = hash_serialize(&m);
        let b2 = hash_serialize(&m);
        assert_eq!(b1, b2, "serialize must be deterministic");
    }

    #[test]
    fn deserialize_truncated_returns_none() {
        let mut m = HashMap::new();
        m.insert(b"key".to_vec(), b"val".to_vec());
        let bytes = hash_serialize(&m);
        assert!(hash_deserialize(&bytes[..bytes.len() / 2]).is_none());
    }

    #[test]
    fn deserialize_empty_slice_returns_none() {
        assert!(hash_deserialize(&[]).is_none());
    }

    #[test]
    fn binary_key_value_roundtrip() {
        let mut m = HashMap::new();
        m.insert(vec![0u8, 255, 128], vec![1u8, 2, 3, 4]);
        let bytes = hash_serialize(&m);
        let m2 = hash_deserialize(&bytes).unwrap();
        assert_eq!(m2, m);
    }
}
