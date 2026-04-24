use std::collections::HashMap;
use std::ffi::CString;
use std::os::raw::{c_char, c_void};
use std::ptr::null_mut;
use valkey_module::native_types::ValkeyType;
use valkey_module::{RedisModuleDefragCtx, RedisModuleString, logging, raw};

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
///
/// Field count is capped at 1 << 20 (≈ 1 M) to prevent OOM from a corrupt
/// 4-byte count field on a degraded NVMe device.
pub fn hash_deserialize(bytes: &[u8]) -> Option<HashMap<Vec<u8>, Vec<u8>>> {
    const MAX_FIELDS: usize = 1 << 20;
    if bytes.len() < 4 {
        return None;
    }
    let count = u32::from_le_bytes(bytes[..4].try_into().ok()?) as usize;
    if count > MAX_FIELDS {
        return None;
    }
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

/// Like [`hash_deserialize`] but logs a warning and returns an empty map when
/// bytes are non-empty and parsing fails — preventing silent data loss.
pub fn hash_deserialize_or_warn(bytes: &[u8]) -> HashMap<Vec<u8>, Vec<u8>> {
    match hash_deserialize(bytes) {
        Some(m) => m,
        None => {
            if !bytes.is_empty() {
                logging::log_warning(
                    "flash: hash_deserialize: corrupt or truncated bytes — returning empty map",
                );
            }
            HashMap::new()
        }
    }
}

// ── FlashHashObject ───────────────────────────────────────────────────────────

pub struct FlashHashObject {
    pub tier: Tier<HashMap<Vec<u8>, Vec<u8>>>,
    pub ttl_ms: Option<i64>,
}

// ── Type registration ─────────────────────────────────────────────────────────

const ENCODING_VERSION: i32 = 1;

// RDB format constants (v1 inline format):
//   [u64 encoding_version][u64 shape_tag][i64 ttl_ms][serialized_hash_bytes]
const SHAPE_TAG_HASH: u64 = 0x02;
// Sentinel stored in the RDB ttl_ms field when the key has no expiry.
const TTL_NONE_SENTINEL: i64 = -1;

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
    unsafe {
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
            // See `types::string::free` for the LOADING-gate rationale.
            if !crate::replication::is_loading()
                && let Some(wal) = crate::WAL.get()
            {
                let _ = wal.append(crate::storage::wal::WalOp::Delete { key_hash });
            }
            if let Ok(mut map) = crate::TIERING_MAP.lock() {
                map.remove(&key_hash);
            }
        }
    }
}

/// # Safety
pub unsafe extern "C" fn mem_usage2(
    _ctx: *mut raw::RedisModuleKeyOptCtx,
    value: *const c_void,
    _sample_size: usize,
) -> usize {
    unsafe {
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
}

/// # Safety
///
/// Serialise a `FlashHashObject` into the RDB stream.
///
/// Wire format (v1):
///   [u64 encoding_version = 1][u64 shape_tag = 0x02][i64 ttl_ms|-1][hash_bytes]
///
/// `hash_bytes` is the `hash_serialize` encoding (same format used for NVMe/cache).
/// For Cold objects, the bytes are fetched from NVMe via `pread_at_offset` —
/// the fork-safe path (`rdb_save` runs inside the forked BGSAVE child).
pub unsafe extern "C" fn rdb_save(io: *mut raw::RedisModuleIO, value: *mut c_void) {
    unsafe {
        let obj = &*value.cast::<FlashHashObject>();

        raw::save_unsigned(io, ENCODING_VERSION as u64);
        raw::save_unsigned(io, SHAPE_TAG_HASH);

        let ttl = obj.ttl_ms.unwrap_or(TTL_NONE_SENTINEL);
        raw::save_signed(io, ttl);

        match &obj.tier {
            Tier::Hot(fields) => {
                let bytes = hash_serialize(fields);
                raw::save_slice(io, &bytes);
            }
            Tier::Cold {
                backend_offset,
                value_len,
                ..
            } => {
                match crate::STORAGE
                    .get()
                    .and_then(|s| s.pread_at_offset(*backend_offset, *value_len).ok())
                {
                    Some(bytes) => raw::save_slice(io, &bytes),
                    None => {
                        logging::log_warning(
                            "flash: rdb_save on Tier::Cold hash: NVMe read failed; writing empty bytes",
                        );
                        raw::save_slice(io, &hash_serialize(&HashMap::new()));
                    }
                }
            }
        }
    }
}

// ── Pure-Rust RDB payload parser ──────────────────────────────────────────────
//
// Flat wire format for the pure parser (mirrors rdb_save field order):
//   [u64-LE encoding_version][u64-LE shape_tag][i64-LE ttl_ms][u64-LE payload_len][bytes]
// where `bytes` is the `hash_serialize` output (same format used on NVMe).

/// Errors returned by [`parse_rdb_hash_payload`] and [`build_rdb_hash`].
#[derive(Debug, PartialEq)]
pub enum RdbHashParseError {
    Truncated,
    UnsupportedVersion(u64),
    UnexpectedTag(u64),
    PayloadTooLarge(usize),
    CorruptHashBytes,
}

impl std::fmt::Display for RdbHashParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RdbHashParseError::Truncated => write!(f, "truncated RDB payload"),
            RdbHashParseError::UnsupportedVersion(v) => {
                write!(
                    f,
                    "unsupported encoding version {v} (max {ENCODING_VERSION})"
                )
            }
            RdbHashParseError::UnexpectedTag(t) => write!(
                f,
                "unexpected shape_tag {t:#04x} (expected {SHAPE_TAG_HASH:#04x})"
            ),
            RdbHashParseError::PayloadTooLarge(n) => {
                write!(f, "hash payload too large: {n} bytes")
            }
            RdbHashParseError::CorruptHashBytes => write!(f, "corrupt hash bytes in RDB"),
        }
    }
}

// 256 MiB — prevents OOM on a maliciously large length prefix.
const MAX_HASH_PAYLOAD_BYTES: usize = 256 * 1024 * 1024;

/// Core validation and construction for FlashHash RDB data.
///
/// Accepts already-decoded primitives (as returned by Valkey's IO helpers or by
/// `parse_rdb_hash_payload`'s flat-LE decoder) and applies all invariant checks.
/// Both `rdb_load` and the fuzz harness call this function.
pub fn build_rdb_hash(
    version: u64,
    tag: u64,
    ttl_raw: i64,
    hash_bytes: Vec<u8>,
) -> Result<FlashHashObject, RdbHashParseError> {
    if version > ENCODING_VERSION as u64 {
        return Err(RdbHashParseError::UnsupportedVersion(version));
    }
    if tag != SHAPE_TAG_HASH {
        return Err(RdbHashParseError::UnexpectedTag(tag));
    }
    if hash_bytes.len() > MAX_HASH_PAYLOAD_BYTES {
        return Err(RdbHashParseError::PayloadTooLarge(hash_bytes.len()));
    }
    let ttl_ms = if ttl_raw == TTL_NONE_SENTINEL {
        None
    } else {
        Some(ttl_raw)
    };
    let fields = hash_deserialize(&hash_bytes).ok_or(RdbHashParseError::CorruptHashBytes)?;
    Ok(FlashHashObject {
        tier: Tier::Hot(fields),
        ttl_ms,
    })
}

/// Parse a FlashHash RDB payload from a flat LE byte buffer.
///
/// Wire format: `[u64-LE version][u64-LE shape_tag][i64-LE ttl_ms][u64-LE payload_len][bytes]`
/// where `bytes` is `hash_serialize` output.
///
/// Entry point for the fuzz harness; exercises the same [`build_rdb_hash`]
/// validation logic that [`rdb_load`] calls in production.
pub fn parse_rdb_hash_payload(data: &[u8]) -> Result<FlashHashObject, RdbHashParseError> {
    use std::io::{Cursor, Read};
    let mut cur = Cursor::new(data);

    macro_rules! ru64 {
        () => {{
            let mut b = [0u8; 8];
            cur.read_exact(&mut b)
                .map_err(|_| RdbHashParseError::Truncated)?;
            u64::from_le_bytes(b)
        }};
    }
    macro_rules! ri64 {
        () => {{
            let mut b = [0u8; 8];
            cur.read_exact(&mut b)
                .map_err(|_| RdbHashParseError::Truncated)?;
            i64::from_le_bytes(b)
        }};
    }

    let version = ru64!();
    let tag = ru64!();
    let ttl_raw = ri64!();
    let payload_len = ru64!() as usize;
    if payload_len > MAX_HASH_PAYLOAD_BYTES {
        return Err(RdbHashParseError::PayloadTooLarge(payload_len));
    }
    let remaining = (data.len() as u64).saturating_sub(cur.position()) as usize;
    if payload_len > remaining {
        return Err(RdbHashParseError::Truncated);
    }
    let mut hash_bytes = vec![0u8; payload_len];
    cur.read_exact(&mut hash_bytes)
        .map_err(|_| RdbHashParseError::Truncated)?;

    build_rdb_hash(version, tag, ttl_raw, hash_bytes)
}

/// # Safety
///
/// Deserialise a `FlashHashObject` from the RDB stream. Returns a raw pointer
/// to a heap-allocated object owned by Valkey, or `null_mut()` on any error.
///
/// All loaded values come back as `Tier::Hot` — cold tiering happens later via
/// the eviction path.
pub unsafe extern "C" fn rdb_load(io: *mut raw::RedisModuleIO, encver: i32) -> *mut c_void {
    if encver > ENCODING_VERSION {
        logging::log_warning(
            format!(
                "flash: rdb_load hash: unsupported module encoding version {encver} \
                 (max supported: {ENCODING_VERSION})"
            )
            .as_str(),
        );
        return null_mut();
    }

    // Read all fields from Valkey's ModuleIO, then delegate to build_rdb_hash
    // so the same validation runs in both production loads and the fuzz harness.
    let version = match raw::load_unsigned(io) {
        Ok(v) => v,
        Err(_) => {
            logging::log_warning("flash: rdb_load hash: short read on encoding_version");
            return null_mut();
        }
    };
    let tag = match raw::load_unsigned(io) {
        Ok(t) => t,
        Err(_) => {
            logging::log_warning("flash: rdb_load hash: short read on shape_tag");
            return null_mut();
        }
    };
    let ttl_raw = match raw::load_signed(io) {
        Ok(t) => t,
        Err(_) => {
            logging::log_warning("flash: rdb_load hash: short read on ttl_ms");
            return null_mut();
        }
    };
    let hash_bytes = match raw::load_string_buffer(io) {
        Ok(buf) => buf.as_ref().to_vec(),
        Err(_) => {
            logging::log_warning("flash: rdb_load hash: short read on hash bytes");
            return null_mut();
        }
    };

    match build_rdb_hash(version, tag, ttl_raw, hash_bytes) {
        Ok(obj) => {
            // SAFETY: Box::into_raw transfers ownership to Valkey's keyspace.
            Box::into_raw(Box::new(obj)).cast::<c_void>()
        }
        Err(e) => {
            logging::log_warning(format!("flash: rdb_load hash: {e}").as_str());
            null_mut()
        }
    }
}

/// # Safety
///
/// Emit `FLASH.HSET key field value` commands for each field in the hash.
/// One command per field-value pair.  TTL (if any) is emitted as a separate
/// `PEXPIREAT key abs_ms` command after all fields are written.
///
/// Hot tier: iterate the in-memory `HashMap` directly.
///
/// Cold tier: materialize the serialized payload from NVMe via the fork-safe
/// `pread_at_offset` path, deserialize, then emit the same FLASH.HSET
/// commands. `pread` avoids touching the parent's io_uring ring from inside
/// the BGREWRITEAOF forked child.
pub unsafe extern "C" fn aof_rewrite(
    aof: *mut raw::RedisModuleIO,
    key: *mut raw::RedisModuleString,
    value: *mut c_void,
) {
    unsafe {
        let obj = &*value.cast::<FlashHashObject>();

        let cold_fields: HashMap<Vec<u8>, Vec<u8>>;
        let fields: &HashMap<Vec<u8>, Vec<u8>> = match &obj.tier {
            Tier::Hot(f) => f,
            Tier::Cold {
                backend_offset,
                value_len,
                ..
            } => match crate::STORAGE
                .get()
                .and_then(|s| s.pread_at_offset(*backend_offset, *value_len).ok())
                .and_then(|bytes| hash_deserialize(&bytes))
            {
                Some(map) => {
                    cold_fields = map;
                    &cold_fields
                }
                None => {
                    logging::log_warning(
                        "flash: aof_rewrite on Tier::Cold hash: NVMe read/deserialize failed; \
                         skipping key",
                    );
                    return;
                }
            },
        };

        let emit = match raw::RedisModule_EmitAOF {
            Some(f) => f,
            None => {
                logging::log_warning("flash: aof_rewrite hash: RedisModule_EmitAOF is null");
                return;
            }
        };

        let hset_cmd = match CString::new("FLASH.HSET") {
            Ok(s) => s,
            Err(_) => return,
        };
        // format: key(s) field(b) value(b)
        let fmt_sbb = match CString::new("sbb") {
            Ok(s) => s,
            Err(_) => return,
        };

        // Sort fields for a deterministic AOF order.
        let mut pairs: Vec<(&Vec<u8>, &Vec<u8>)> = fields.iter().collect();
        pairs.sort_unstable_by_key(|(k, _)| *k);

        for (field, val) in &pairs {
            emit(
                aof,
                hset_cmd.as_ptr(),
                fmt_sbb.as_ptr(),
                key,
                field.as_ptr().cast::<c_char>(),
                field.len(),
                val.as_ptr().cast::<c_char>(),
                val.len(),
            );
        }

        // Emit TTL as a separate PEXPIREAT command so the absolute expiry is preserved
        // correctly across restarts (mirrors the PXAT approach used for FlashString).
        if let Some(ttl) = obj.ttl_ms {
            let pexpireat_cmd = match CString::new("PEXPIREAT") {
                Ok(s) => s,
                Err(_) => return,
            };
            // format: key(s) abs_ms(l)
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
}

/// # Safety
pub unsafe extern "C" fn digest(_md: *mut raw::RedisModuleDigest, _value: *mut c_void) {
    // stub — digest not yet implemented
}

/// # Safety
///
/// Called by Valkey's COPY/OBJECT COPY to deep-copy a FlashHashObject.
///
/// ## Pointer contract
/// - `value` — const pointer to the source `FlashHashObject`; must not be mutated.
/// - Returns an owned `*mut FlashHashObject` on success, or `null_mut()` on failure.
///
/// ## Cold-tier strategy (v1)
/// Cold objects are materialised via a synchronous NVMe read + `hash_deserialize`, then
/// returned as a Hot copy. Null is returned on read or deserialisation failure.
///
/// ## TTL handling
/// `ttl_ms` is copied verbatim; see the analogous comment in `types::string::copy`.
pub unsafe extern "C" fn copy(
    _from_key: *mut RedisModuleString,
    to_key: *mut RedisModuleString,
    value: *const c_void,
) -> *mut c_void {
    unsafe {
        if let Some(cache) = crate::CACHE.get() {
            let to_key_bytes = crate::util::module_string_bytes(to_key);
            cache.delete(&to_key_bytes);
        }
        let src: &FlashHashObject = &*value.cast::<FlashHashObject>();
        match &src.tier {
            Tier::Hot(map) => {
                let new_obj = Box::new(FlashHashObject {
                    tier: Tier::Hot(map.clone()),
                    ttl_ms: src.ttl_ms,
                });
                Box::into_raw(new_obj).cast::<c_void>()
            }
            Tier::Cold {
                backend_offset,
                value_len,
                ..
            } => {
                // Materialise cold value via synchronous NVMe read → deserialise → Hot copy.
                #[cfg(not(test))]
                {
                    let storage = match crate::STORAGE.get() {
                        Some(s) => s,
                        None => return null_mut(),
                    };
                    match storage.read_at_offset(*backend_offset, *value_len) {
                        Ok(bytes) => match hash_deserialize(&bytes) {
                            Some(map) => {
                                let new_obj = Box::new(FlashHashObject {
                                    tier: Tier::Hot(map),
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
}

/// Hashes at or below this entry count get a full `HashMap` rebuild during
/// defrag so the backing bucket-table is also relocated. Above this size,
/// a rebuild allocates O(n) bucket slots every time the defrag cursor
/// lands on the key — expensive enough that we keep the drain-and-reinsert
/// pattern and accept that the bucket table stays at its original
/// location. 128 is a rough crossover: Rust `HashMap`'s default capacity
/// formula keeps the table under 256 entries at this size, so the rebuild
/// allocation is comparable to a single entry buffer relocation.
const DEFRAG_FULL_REBUILD_MAX_ENTRIES: usize = 128;

/// Place the drained `entries` back into `map`. For small hashes (≤
/// `DEFRAG_FULL_REBUILD_MAX_ENTRIES`) this replaces `*map` with a
/// freshly-allocated `HashMap` so the backing bucket-table is also
/// relocated by the allocator; for larger hashes it reinserts into
/// `map` so we don't allocate a new bucket array every defrag pass.
///
/// Extracted from the `defrag` callback below so the pure data-movement
/// logic is unit-testable without a real `RedisModuleDefragCtx`.
pub(crate) fn reinsert_defrag_entries(
    map: &mut HashMap<Vec<u8>, Vec<u8>>,
    entries: Vec<(Vec<u8>, Vec<u8>)>,
) {
    let full_rebuild = entries.len() <= DEFRAG_FULL_REBUILD_MAX_ENTRIES;
    if full_rebuild {
        let mut rebuild_target: HashMap<Vec<u8>, Vec<u8>> = HashMap::with_capacity(entries.len());
        for (k, v) in entries {
            rebuild_target.insert(k, v);
        }
        // `*map = rebuild_target` drops the original HashMap (freeing
        // its bucket table via the Valkey allocator) and installs the
        // freshly-allocated one — which the allocator chose at a
        // defragmented address since we built it inside this defrag
        // callback.
        *map = rebuild_target;
    } else {
        for (k, v) in entries {
            map.insert(k, v);
        }
    }
}

/// # Safety
pub unsafe extern "C" fn defrag(
    ctx: *mut RedisModuleDefragCtx,
    _key: *mut RedisModuleString,
    value: *mut *mut c_void,
) -> i32 {
    unsafe {
        use std::mem;
        use valkey_module::defrag::Defrag;

        let dfg = Defrag::new(ctx);

        // Step 1: relocate the FlashHashObject struct itself
        let new_struct = dfg.alloc(*value);
        if !new_struct.is_null() {
            *value = new_struct;
        }

        // Step 2: for Hot tier, relocate each key and value Vec's backing
        // buffer. For small hashes, we also replace the backing HashMap
        // with a fresh one so its bucket table is relocated too (see
        // DEFRAG_FULL_REBUILD_MAX_ENTRIES for the threshold rationale).
        let obj: &mut FlashHashObject = &mut *(*value).cast::<FlashHashObject>();
        if let Tier::Hot(ref mut map) = obj.tier {
            // Drain the map, potentially relocating each key and value buffer, then
            // reinsert. Collect first to avoid borrowing `map` during drain.
            let mut entries: Vec<(Vec<u8>, Vec<u8>)> = map.drain().collect();
            for (k, v) in entries.iter_mut() {
                if k.capacity() > 0 {
                    let new_buf = dfg.alloc(k.as_mut_ptr().cast::<c_void>());
                    if !new_buf.is_null() {
                        let (old_len, old_cap) = (k.len(), k.capacity());
                        let old = mem::replace(
                            k,
                            Vec::from_raw_parts(new_buf.cast::<u8>(), old_len, old_cap),
                        );
                        mem::forget(old);
                    }
                }
                if v.capacity() > 0 {
                    let new_buf = dfg.alloc(v.as_mut_ptr().cast::<c_void>());
                    if !new_buf.is_null() {
                        let (old_len, old_cap) = (v.len(), v.capacity());
                        let old = mem::replace(
                            v,
                            Vec::from_raw_parts(new_buf.cast::<u8>(), old_len, old_cap),
                        );
                        mem::forget(old);
                    }
                }
            }
            reinsert_defrag_entries(map, entries);
        }
        // Cold tier: only primitive scalars — nothing to relocate.

        0
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
    fn reinsert_defrag_entries_rebuilds_small_hash_map() {
        // At ≤128 entries the helper must replace the backing HashMap so
        // the bucket table is freshly allocated. We can't verify the
        // allocator choice directly, but we can check that the contents
        // are intact and the new map's capacity matches the small-hash
        // with_capacity(n) path rather than the drained map's (which
        // keeps its prior — potentially larger — bucket allocation).
        let mut map: HashMap<Vec<u8>, Vec<u8>> = HashMap::with_capacity(1024);
        let entries = vec![
            (b"k1".to_vec(), b"v1".to_vec()),
            (b"k2".to_vec(), b"v2".to_vec()),
            (b"k3".to_vec(), b"v3".to_vec()),
        ];

        reinsert_defrag_entries(&mut map, entries);

        assert_eq!(map.len(), 3);
        assert_eq!(map.get(b"k1".as_ref()), Some(&b"v1".to_vec()));
        assert_eq!(map.get(b"k2".as_ref()), Some(&b"v2".to_vec()));
        assert_eq!(map.get(b"k3".as_ref()), Some(&b"v3".to_vec()));
        // Fresh allocation: capacity follows `with_capacity(entries.len())`,
        // not the pre-existing 1024. Anything ≫ 3 means we kept the old
        // bucket table (regression).
        assert!(
            map.capacity() < 1024,
            "small-hash path must rebuild the bucket table; capacity stayed at {}",
            map.capacity()
        );
    }

    #[test]
    fn reinsert_defrag_entries_reinserts_large_hash_in_place() {
        // Above the threshold the helper must reuse the drained map (no
        // allocation churn per defrag tick).
        let mut map: HashMap<Vec<u8>, Vec<u8>> = HashMap::with_capacity(1024);
        let pre_capacity = map.capacity();
        let n = DEFRAG_FULL_REBUILD_MAX_ENTRIES + 10;
        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..n)
            .map(|i| (format!("k{i}").into_bytes(), format!("v{i}").into_bytes()))
            .collect();

        reinsert_defrag_entries(&mut map, entries);

        assert_eq!(map.len(), n);
        for i in 0..n {
            assert_eq!(
                map.get(format!("k{i}").as_bytes()),
                Some(&format!("v{i}").into_bytes()),
                "entry k{i} missing after large-hash reinsert",
            );
        }
        // Large-hash path keeps the drained map; capacity may have grown
        // to fit `n` but must not have been replaced by a fresh
        // `with_capacity(n)` shrink.
        assert!(
            map.capacity() >= pre_capacity,
            "large-hash path must not shrink the bucket table; {} < {}",
            map.capacity(),
            pre_capacity,
        );
    }

    #[test]
    fn reinsert_defrag_entries_handles_boundary_128_entries() {
        // Exactly 128 entries falls on the rebuild side of the
        // boundary. Verify the helper handles the edge count without
        // losing data.
        let mut map: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..DEFRAG_FULL_REBUILD_MAX_ENTRIES)
            .map(|i| (format!("k{i}").into_bytes(), format!("v{i}").into_bytes()))
            .collect();

        reinsert_defrag_entries(&mut map, entries);

        assert_eq!(map.len(), DEFRAG_FULL_REBUILD_MAX_ENTRIES);
    }

    #[test]
    fn reinsert_defrag_entries_empty_input_produces_empty_map() {
        let mut map: HashMap<Vec<u8>, Vec<u8>> = HashMap::with_capacity(16);
        map.insert(b"stale".to_vec(), b"v".to_vec());

        reinsert_defrag_entries(&mut map, Vec::new());

        assert!(
            map.is_empty(),
            "empty-entries rebuild must leave an empty map, got {} entries",
            map.len()
        );
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
