use std::collections::VecDeque;
use std::ffi::CString;
use std::os::raw::{c_char, c_void};
use std::ptr::null_mut;
use valkey_module::native_types::ValkeyType;
use valkey_module::{logging, raw, RedisModuleDefragCtx, RedisModuleString};

use super::Tier;

// ── Serialization ─────────────────────────────────────────────────────────────

/// Encode a list to bytes for NVMe storage and cache.
/// Wire format: [u32 count] then per-element [u32 elem_len][elem_bytes].
pub fn list_serialize(items: &VecDeque<Vec<u8>>) -> Vec<u8> {
    let capacity = 4 + items.iter().map(|e| 4 + e.len()).sum::<usize>();
    let mut buf = Vec::with_capacity(capacity);
    buf.extend_from_slice(&(items.len() as u32).to_le_bytes());
    for elem in items {
        buf.extend_from_slice(&(elem.len() as u32).to_le_bytes());
        buf.extend_from_slice(elem);
    }
    buf
}

/// Decode bytes back to a list. Returns `None` on any structural corruption.
///
/// Element count is capped at 1 << 20 (≈ 1 M) to prevent OOM from a corrupt
/// 4-byte count field on a degraded NVMe device.
pub fn list_deserialize(bytes: &[u8]) -> Option<VecDeque<Vec<u8>>> {
    const MAX_ELEMENTS: usize = 1 << 20;
    if bytes.len() < 4 {
        return None;
    }
    let count = u32::from_le_bytes(bytes[..4].try_into().ok()?) as usize;
    if count > MAX_ELEMENTS {
        return None;
    }
    let mut list = VecDeque::with_capacity(count);
    let mut pos = 4usize;
    for _ in 0..count {
        if pos + 4 > bytes.len() {
            return None;
        }
        let elen = u32::from_le_bytes(bytes[pos..pos + 4].try_into().ok()?) as usize;
        pos += 4;
        if pos + elen > bytes.len() {
            return None;
        }
        list.push_back(bytes[pos..pos + elen].to_vec());
        pos += elen;
    }
    if pos != bytes.len() {
        return None;
    }
    Some(list)
}

/// Like [`list_deserialize`] but logs a warning and returns an empty list when
/// bytes are non-empty and parsing fails — preventing silent data loss.
pub fn list_deserialize_or_warn(bytes: &[u8]) -> VecDeque<Vec<u8>> {
    match list_deserialize(bytes) {
        Some(l) => l,
        None => {
            if !bytes.is_empty() {
                logging::log_warning(
                    "flash: list_deserialize: corrupt or truncated bytes — returning empty list",
                );
            }
            VecDeque::new()
        }
    }
}

// ── FlashListObject ────────────────────────────────────────────────────────────

pub struct FlashListObject {
    pub tier: Tier<VecDeque<Vec<u8>>>,
    pub ttl_ms: Option<i64>,
}

// ── Type registration ─────────────────────────────────────────────────────────

const ENCODING_VERSION: i32 = 1;

// RDB format: [u64 encoding_version][u64 shape_tag][i64 ttl_ms][serialized_list_bytes]
const SHAPE_TAG_LIST: u64 = 0x03;
const TTL_NONE_SENTINEL: i64 = -1;

// "flashlst1" is exactly 9 chars — satisfies the module-type-id constraint.
pub static FLASH_LIST_TYPE: ValkeyType = ValkeyType::new(
    "flashlst1",
    ENCODING_VERSION,
    raw::RedisModuleTypeMethods {
        version: raw::REDISMODULE_TYPE_METHOD_VERSION as u64,
        rdb_load: Some(rdb_load),
        rdb_save: Some(rdb_save),
        aof_rewrite: Some(aof_rewrite),
        digest: Some(digest),
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
    // SAFETY: value was allocated by Box::into_raw(Box::new(FlashListObject {...})).
    let obj = Box::from_raw(value.cast::<FlashListObject>());
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
    let obj = &*value.cast::<FlashListObject>();
    match &obj.tier {
        Tier::Hot(items) => {
            let items_bytes: usize = items.iter().map(|e| e.len()).sum();
            std::mem::size_of::<FlashListObject>() + items_bytes
        }
        Tier::Cold { .. } => std::mem::size_of::<FlashListObject>(),
    }
}

/// # Safety
///
/// Wire format: [u64 encoding_version][u64 shape_tag=0x03][i64 ttl_ms][list_bytes]
pub unsafe extern "C" fn rdb_save(io: *mut raw::RedisModuleIO, value: *mut c_void) {
    let obj = &*value.cast::<FlashListObject>();

    raw::save_unsigned(io, ENCODING_VERSION as u64);
    raw::save_unsigned(io, SHAPE_TAG_LIST);
    raw::save_signed(io, obj.ttl_ms.unwrap_or(TTL_NONE_SENTINEL));

    match &obj.tier {
        Tier::Hot(items) => {
            raw::save_slice(io, &list_serialize(items));
        }
        Tier::Cold {
            backend_offset,
            value_len,
            ..
        } => {
            match crate::STORAGE
                .get()
                .and_then(|s| s.read_at_offset(*backend_offset, *value_len).ok())
            {
                Some(bytes) => raw::save_slice(io, &bytes),
                None => {
                    logging::log_warning(
                        "flash: rdb_save on Tier::Cold list: NVMe read failed; writing empty bytes",
                    );
                    raw::save_slice(io, &list_serialize(&VecDeque::new()));
                }
            }
        }
    }
}

/// # Safety
pub unsafe extern "C" fn rdb_load(io: *mut raw::RedisModuleIO, encver: i32) -> *mut c_void {
    if encver > ENCODING_VERSION {
        logging::log_warning(
            format!(
                "flash: rdb_load list: unsupported module encoding version {encver} \
                 (max supported: {ENCODING_VERSION})"
            )
            .as_str(),
        );
        return null_mut();
    }

    let version = match raw::load_unsigned(io) {
        Ok(v) => v,
        Err(_) => {
            logging::log_warning("flash: rdb_load list: short read on encoding_version");
            return null_mut();
        }
    };
    if version > ENCODING_VERSION as u64 {
        logging::log_warning(
            format!("flash: rdb_load list: unsupported encoding version {version}").as_str(),
        );
        return null_mut();
    }

    let tag = match raw::load_unsigned(io) {
        Ok(t) => t,
        Err(_) => {
            logging::log_warning("flash: rdb_load list: short read on shape_tag");
            return null_mut();
        }
    };
    if tag != SHAPE_TAG_LIST {
        logging::log_warning(
            format!("flash: rdb_load list: unexpected shape_tag {tag:#04x}").as_str(),
        );
        return null_mut();
    }

    let ttl_raw = match raw::load_signed(io) {
        Ok(t) => t,
        Err(_) => {
            logging::log_warning("flash: rdb_load list: short read on ttl_ms");
            return null_mut();
        }
    };
    let list_bytes = match raw::load_string_buffer(io) {
        Ok(buf) => buf.as_ref().to_vec(),
        Err(_) => {
            logging::log_warning("flash: rdb_load list: short read on list bytes");
            return null_mut();
        }
    };

    let items = match list_deserialize(&list_bytes) {
        Some(l) => l,
        None => {
            logging::log_warning("flash: rdb_load list: corrupt list bytes");
            return null_mut();
        }
    };
    let ttl_ms = if ttl_raw == TTL_NONE_SENTINEL {
        None
    } else {
        Some(ttl_raw)
    };

    Box::into_raw(Box::new(FlashListObject {
        tier: Tier::Hot(items),
        ttl_ms,
    }))
    .cast::<c_void>()
}

/// # Safety
///
/// Emit one `FLASH.RPUSH key elem` command per element to preserve list order.
/// TTL emitted as a separate `PEXPIREAT` command.
pub unsafe extern "C" fn aof_rewrite(
    aof: *mut raw::RedisModuleIO,
    key: *mut raw::RedisModuleString,
    value: *mut c_void,
) {
    let obj = &*value.cast::<FlashListObject>();

    let items = match &obj.tier {
        Tier::Hot(l) => l,
        Tier::Cold { .. } => {
            logging::log_warning(
                "flash: aof_rewrite on Tier::Cold list — cannot fetch from NVMe without key; \
                 skipping key",
            );
            return;
        }
    };

    if items.is_empty() {
        return;
    }

    let emit = match raw::RedisModule_EmitAOF {
        Some(f) => f,
        None => {
            logging::log_warning("flash: aof_rewrite list: RedisModule_EmitAOF is null");
            return;
        }
    };

    let rpush_cmd = match CString::new("FLASH.RPUSH") {
        Ok(s) => s,
        Err(_) => return,
    };
    let fmt_sb = match CString::new("sb") {
        Ok(s) => s,
        Err(_) => return,
    };

    for elem in items {
        emit(
            aof,
            rpush_cmd.as_ptr(),
            fmt_sb.as_ptr(),
            key,
            elem.as_ptr().cast::<c_char>(),
            elem.len(),
        );
    }

    if let Some(ttl) = obj.ttl_ms {
        let pexpireat_cmd = match CString::new("PEXPIREAT") {
            Ok(s) => s,
            Err(_) => return,
        };
        let fmt_sl = match CString::new("sl") {
            Ok(s) => s,
            Err(_) => return,
        };
        emit(
            aof,
            pexpireat_cmd.as_ptr(),
            fmt_sl.as_ptr(),
            key,
            ttl as std::os::raw::c_longlong,
        );
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
    value: *const c_void,
) -> *mut c_void {
    let src = &*value.cast::<FlashListObject>();
    match &src.tier {
        Tier::Hot(items) => {
            let new_obj = Box::new(FlashListObject {
                tier: Tier::Hot(items.clone()),
                ttl_ms: src.ttl_ms,
            });
            Box::into_raw(new_obj).cast::<c_void>()
        }
        Tier::Cold {
            backend_offset,
            value_len,
            ..
        } => {
            #[cfg(not(test))]
            {
                let storage = match crate::STORAGE.get() {
                    Some(s) => s,
                    None => return null_mut(),
                };
                match storage.read_at_offset(*backend_offset, *value_len) {
                    Ok(bytes) => match list_deserialize(&bytes) {
                        Some(items) => {
                            let new_obj = Box::new(FlashListObject {
                                tier: Tier::Hot(items),
                                ttl_ms: src.ttl_ms,
                            });
                            Box::into_raw(new_obj).cast::<c_void>()
                        }
                        None => null_mut(),
                    },
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

/// # Safety
pub unsafe extern "C" fn defrag(
    ctx: *mut RedisModuleDefragCtx,
    _key: *mut RedisModuleString,
    value: *mut *mut c_void,
) -> i32 {
    use std::mem;
    use valkey_module::defrag::Defrag;

    let dfg = Defrag::new(ctx);

    let new_struct = dfg.alloc(*value);
    if !new_struct.is_null() {
        *value = new_struct;
    }

    let obj: &mut FlashListObject = &mut *(*value).cast::<FlashListObject>();
    if let Tier::Hot(ref mut items) = obj.tier {
        for elem in items.iter_mut() {
            if elem.capacity() > 0 {
                let new_buf = dfg.alloc(elem.as_mut_ptr().cast::<c_void>());
                if !new_buf.is_null() {
                    let (old_len, old_cap) = (elem.len(), elem.capacity());
                    let old = mem::replace(
                        elem,
                        Vec::from_raw_parts(new_buf.cast::<u8>(), old_len, old_cap),
                    );
                    mem::forget(old);
                }
            }
        }
    }

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
    fn flash_list_object_hot_empty() {
        let obj = FlashListObject {
            tier: Tier::Hot(VecDeque::new()),
            ttl_ms: None,
        };
        assert_eq!(obj.tier, Tier::Hot(VecDeque::new()));
        assert!(obj.ttl_ms.is_none());
    }

    #[test]
    fn flash_list_object_cold() {
        let obj = FlashListObject {
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
    fn empty_list_roundtrip() {
        let list: VecDeque<Vec<u8>> = VecDeque::new();
        let bytes = list_serialize(&list);
        assert_eq!(list_deserialize(&bytes), Some(VecDeque::new()));
    }

    #[test]
    fn single_elem_roundtrip() {
        let mut list = VecDeque::new();
        list.push_back(b"hello".to_vec());
        let bytes = list_serialize(&list);
        let out = list_deserialize(&bytes).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0], b"hello");
    }

    #[test]
    fn multi_elem_preserves_order() {
        let mut list = VecDeque::new();
        list.push_back(b"a".to_vec());
        list.push_back(b"b".to_vec());
        list.push_back(b"c".to_vec());
        let bytes = list_serialize(&list);
        let out = list_deserialize(&bytes).unwrap();
        assert_eq!(out, list);
    }

    #[test]
    fn deserialize_truncated_returns_none() {
        let mut list = VecDeque::new();
        list.push_back(b"x".to_vec());
        let bytes = list_serialize(&list);
        assert!(list_deserialize(&bytes[..bytes.len() / 2]).is_none());
    }

    #[test]
    fn deserialize_empty_slice_returns_none() {
        assert!(list_deserialize(&[]).is_none());
    }

    #[test]
    fn binary_element_roundtrip() {
        let mut list = VecDeque::new();
        list.push_back(vec![0u8, 255, 128, 64]);
        let bytes = list_serialize(&list);
        let out = list_deserialize(&bytes).unwrap();
        assert_eq!(out[0], vec![0u8, 255, 128, 64]);
    }

    #[test]
    fn mem_usage2_hot_reports_struct_plus_elem_bytes() {
        let mut items = VecDeque::new();
        items.push_back(b"hello".to_vec()); // 5 bytes
        let obj = Box::new(FlashListObject {
            tier: Tier::Hot(items),
            ttl_ms: None,
        });
        let ptr = Box::into_raw(obj) as *const c_void;
        // SAFETY: ptr is freshly allocated via Box::into_raw, valid for the duration of this test.
        let size = unsafe { mem_usage2(std::ptr::null_mut(), ptr, 0) };
        // SAFETY: ptr was allocated by Box::into_raw above and has not been freed.
        unsafe { drop(Box::from_raw(ptr as *mut FlashListObject)) };
        assert_eq!(size, std::mem::size_of::<FlashListObject>() + 5);
    }

    #[test]
    fn mem_usage2_cold_reports_struct_only() {
        let obj = Box::new(FlashListObject {
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
        unsafe { drop(Box::from_raw(ptr as *mut FlashListObject)) };
        assert_eq!(size, std::mem::size_of::<FlashListObject>());
    }

    #[test]
    fn struct_size_regression() {
        const { assert!(std::mem::size_of::<FlashListObject>() <= 96) };
    }
}
