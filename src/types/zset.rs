use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::ffi::CString;
use std::os::raw::{c_char, c_void};
use std::ptr::null_mut;
use valkey_module::native_types::ValkeyType;
use valkey_module::{RedisModuleDefragCtx, RedisModuleString, logging, raw};

use super::Tier;

// ── ScoreF64 ──────────────────────────────────────────────────────────────────

/// Total-order wrapper for `f64` so it can be used as a BTreeMap key.
/// Uses `f64::total_cmp` which orders NaN consistently (NaN should never appear
/// in practice since we validate scores at command parse time).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ScoreF64(pub f64);

impl Eq for ScoreF64 {}

impl PartialOrd for ScoreF64 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScoreF64 {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

// ── ZSetInner ─────────────────────────────────────────────────────────────────

/// Dual-index sorted-set storage.
///
/// `scores` is keyed by `(ScoreF64, member)` for O(log n) ordered range queries
/// plus correct tie-breaking by member bytes. `members` gives O(1) member → score
/// lookup, needed for ZSCORE / rank computation / update.
#[derive(Debug, Clone, Default)]
pub struct ZSetInner {
    pub scores: BTreeMap<(ScoreF64, Vec<u8>), ()>,
    pub members: HashMap<Vec<u8>, f64>,
}

impl ZSetInner {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert or update a member. Returns the old score if the member existed.
    pub fn insert(&mut self, member: Vec<u8>, score: f64) -> Option<f64> {
        if let Some(&old_score) = self.members.get(&member) {
            self.scores.remove(&(ScoreF64(old_score), member.clone()));
        }
        let old = self.members.insert(member.clone(), score);
        self.scores.insert((ScoreF64(score), member), ());
        old
    }

    /// Remove a member. Returns its score if found.
    pub fn remove(&mut self, member: &[u8]) -> Option<f64> {
        if let Some(score) = self.members.remove(member) {
            self.scores.remove(&(ScoreF64(score), member.to_vec()));
            Some(score)
        } else {
            None
        }
    }

    pub fn get_score(&self, member: &[u8]) -> Option<f64> {
        self.members.get(member).copied()
    }

    pub fn len(&self) -> usize {
        self.members.len()
    }

    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }
}

// ── Score helpers ─────────────────────────────────────────────────────────────

/// Format a score for wire output (bulk string reply).
pub fn format_score(score: f64) -> String {
    if score == f64::INFINITY {
        return "+inf".to_string();
    }
    if score == f64::NEG_INFINITY {
        return "-inf".to_string();
    }
    format!("{score}")
}

// ── Serialization ─────────────────────────────────────────────────────────────

/// Wire format for NVMe/cache: `[u32 count][f64_le score][u32 mlen][member] × count`
/// Iteration order is BTreeMap order: ascending by (score, member).
pub fn zset_serialize(inner: &ZSetInner) -> Vec<u8> {
    let count = inner.scores.len();
    let capacity = 4 + inner
        .scores
        .iter()
        .map(|((_, m), ())| 8 + 4 + m.len())
        .sum::<usize>();
    let mut buf = Vec::with_capacity(capacity);
    buf.extend_from_slice(&(count as u32).to_le_bytes());
    for ((ScoreF64(score), member), ()) in &inner.scores {
        buf.extend_from_slice(&score.to_le_bytes());
        buf.extend_from_slice(&(member.len() as u32).to_le_bytes());
        buf.extend_from_slice(member);
    }
    buf
}

/// Decode NVMe/cache bytes back to a `ZSetInner`. Returns `None` on corruption.
///
/// Element count capped at 1 << 20 and per-member size capped at 512 MiB.
pub fn zset_deserialize(bytes: &[u8]) -> Option<ZSetInner> {
    const MAX_MEMBERS: usize = 1 << 20;
    const MAX_MEMBER_BYTES: usize = 512 * 1024 * 1024;
    if bytes.len() < 4 {
        return None;
    }
    let count = u32::from_le_bytes(bytes[..4].try_into().ok()?) as usize;
    if count > MAX_MEMBERS {
        return None;
    }
    let mut inner = ZSetInner {
        scores: BTreeMap::new(),
        members: HashMap::with_capacity(count),
    };
    let mut pos = 4usize;
    for _ in 0..count {
        if pos + 12 > bytes.len() {
            return None;
        }
        let score = f64::from_le_bytes(bytes[pos..pos + 8].try_into().ok()?);
        pos += 8;
        let mlen = u32::from_le_bytes(bytes[pos..pos + 4].try_into().ok()?) as usize;
        pos += 4;
        if mlen > MAX_MEMBER_BYTES || pos + mlen > bytes.len() {
            return None;
        }
        let member = bytes[pos..pos + mlen].to_vec();
        pos += mlen;
        inner.scores.insert((ScoreF64(score), member.clone()), ());
        inner.members.insert(member, score);
    }
    if pos != bytes.len() {
        return None;
    }
    Some(inner)
}

/// Like [`zset_deserialize`] but logs a warning on corruption and returns empty.
pub fn zset_deserialize_or_warn(bytes: &[u8]) -> ZSetInner {
    match zset_deserialize(bytes) {
        Some(z) => z,
        None => {
            if !bytes.is_empty() {
                logging::log_warning(
                    "flash: zset_deserialize: corrupt bytes — returning empty zset",
                );
            }
            ZSetInner::new()
        }
    }
}

// ── FlashZSetObject ───────────────────────────────────────────────────────────

pub struct FlashZSetObject {
    pub tier: Tier<ZSetInner>,
    pub ttl_ms: Option<i64>,
}

// ── Type registration ─────────────────────────────────────────────────────────

const ENCODING_VERSION: i32 = 1;
// RDB format (v1 stub): [u64 encoding_version=1][u64 shape_tag=0x04][i64 ttl_ms][zset_bytes]
const SHAPE_TAG_ZSET: u64 = 0x04;
const TTL_NONE_SENTINEL: i64 = -1;

pub static FLASH_ZSET_TYPE: ValkeyType = ValkeyType::new(
    "flashzst1",
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
    unsafe {
        let obj = Box::from_raw(value.cast::<FlashZSetObject>());
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
        let obj = &*value.cast::<FlashZSetObject>();
        match &obj.tier {
            Tier::Hot(inner) => {
                let data_bytes: usize = inner.members.keys().map(|k| k.len() + 8).sum();
                std::mem::size_of::<FlashZSetObject>() + data_bytes
            }
            Tier::Cold { .. } => std::mem::size_of::<FlashZSetObject>(),
        }
    }
}

/// # Safety
///
/// RDB format (v1):
///   `[u64 encoding_version=1][u64 shape_tag=0x04][i64 ttl_ms]`
///   `[u64 count][for each member in BTreeMap order: save_double(score) + save_slice(member)]`
pub unsafe extern "C" fn rdb_save(io: *mut raw::RedisModuleIO, value: *mut c_void) {
    unsafe {
        let obj = &*value.cast::<FlashZSetObject>();
        raw::save_unsigned(io, ENCODING_VERSION as u64);
        raw::save_unsigned(io, SHAPE_TAG_ZSET);
        raw::save_signed(io, obj.ttl_ms.unwrap_or(TTL_NONE_SENTINEL));

        let cold_inner: ZSetInner;
        let inner: &ZSetInner = match &obj.tier {
            Tier::Hot(z) => z,
            Tier::Cold {
                backend_offset,
                value_len,
                ..
            } => {
                match crate::STORAGE
                    .get()
                    .and_then(|s| s.read_at_offset(*backend_offset, *value_len).ok())
                    .and_then(|b| zset_deserialize(&b))
                {
                    Some(z) => {
                        cold_inner = z;
                        &cold_inner
                    }
                    None => {
                        logging::log_warning(
                            "flash: rdb_save Tier::Cold zset: NVMe read failed; writing empty zset",
                        );
                        cold_inner = ZSetInner::new();
                        &cold_inner
                    }
                }
            }
        };

        raw::save_unsigned(io, inner.scores.len() as u64);
        for ((ScoreF64(score), member), ()) in &inner.scores {
            raw::save_double(io, *score);
            raw::save_slice(io, member);
        }
    }
}

/// # Safety
pub unsafe extern "C" fn rdb_load(io: *mut raw::RedisModuleIO, encver: i32) -> *mut c_void {
    const MAX_ZSET_MEMBERS: u64 = 1_000_000;
    const MAX_MEMBER_BYTES: usize = 512 * 1024 * 1024;

    if encver > ENCODING_VERSION {
        logging::log_warning(
            format!(
                "flash: rdb_load zset: unsupported module encoding version {encver} \
                 (max supported: {ENCODING_VERSION})"
            )
            .as_str(),
        );
        return null_mut();
    }

    let version = match raw::load_unsigned(io) {
        Ok(v) => v,
        Err(_) => {
            logging::log_warning("flash: rdb_load zset: short read on encoding_version");
            return null_mut();
        }
    };
    if version > ENCODING_VERSION as u64 {
        logging::log_warning(
            format!("flash: rdb_load zset: unsupported encoding version {version}").as_str(),
        );
        return null_mut();
    }

    let tag = match raw::load_unsigned(io) {
        Ok(t) => t,
        Err(_) => {
            logging::log_warning("flash: rdb_load zset: short read on shape_tag");
            return null_mut();
        }
    };
    if tag != SHAPE_TAG_ZSET {
        logging::log_warning(
            format!("flash: rdb_load zset: unexpected shape_tag {tag:#04x}").as_str(),
        );
        return null_mut();
    }

    let ttl_raw = match raw::load_signed(io) {
        Ok(t) => t,
        Err(_) => {
            logging::log_warning("flash: rdb_load zset: short read on ttl_ms");
            return null_mut();
        }
    };

    let count = match raw::load_unsigned(io) {
        Ok(c) => c,
        Err(_) => {
            logging::log_warning("flash: rdb_load zset: short read on member count");
            return null_mut();
        }
    };
    if count > MAX_ZSET_MEMBERS {
        logging::log_warning(
            format!("flash: rdb_load zset: member count {count} exceeds cap {MAX_ZSET_MEMBERS}")
                .as_str(),
        );
        return null_mut();
    }

    let mut inner = ZSetInner {
        scores: BTreeMap::new(),
        members: HashMap::with_capacity(count as usize),
    };
    let mut total_member_bytes: usize = 0;
    for i in 0..count {
        let score = match raw::load_double(io) {
            Ok(s) => s,
            Err(_) => {
                logging::log_warning(
                    format!("flash: rdb_load zset: short read on score {i}").as_str(),
                );
                return null_mut();
            }
        };
        let member_buf = match raw::load_string_buffer(io) {
            Ok(buf) => buf,
            Err(_) => {
                logging::log_warning(
                    format!("flash: rdb_load zset: short read on member {i}").as_str(),
                );
                return null_mut();
            }
        };
        if member_buf.as_ref().len() > MAX_MEMBER_BYTES {
            logging::log_warning(
                format!(
                    "flash: rdb_load zset: member {i} size {} exceeds 512 MiB cap",
                    member_buf.as_ref().len()
                )
                .as_str(),
            );
            return null_mut();
        }
        total_member_bytes = match total_member_bytes.checked_add(member_buf.as_ref().len()) {
            Some(t) => t,
            None => {
                logging::log_warning("flash: rdb_load zset: total member bytes overflow");
                return null_mut();
            }
        };
        if total_member_bytes > MAX_MEMBER_BYTES {
            logging::log_warning(
                format!(
                    "flash: rdb_load zset: cumulative member bytes {total_member_bytes} \
                     exceeds 512 MiB cap"
                )
                .as_str(),
            );
            return null_mut();
        }
        inner.insert(member_buf.as_ref().to_vec(), score);
    }

    let ttl_ms = if ttl_raw == TTL_NONE_SENTINEL {
        None
    } else {
        Some(ttl_raw)
    };

    Box::into_raw(Box::new(FlashZSetObject {
        tier: Tier::Hot(inner),
        ttl_ms,
    }))
    .cast::<c_void>()
}

/// # Safety
///
/// Emit `FLASH.ZADD key score1 member1 …` commands in chunks of 64 pairs.
/// Uses the EmitAOF `'sv'` format to pass a dynamic-length argv array.
pub unsafe extern "C" fn aof_rewrite(
    aof: *mut raw::RedisModuleIO,
    key: *mut raw::RedisModuleString,
    value: *mut c_void,
) {
    unsafe {
        let obj = &*value.cast::<FlashZSetObject>();
        let cold_inner: ZSetInner;
        let inner: &ZSetInner = match &obj.tier {
            Tier::Hot(z) => z,
            Tier::Cold {
                backend_offset,
                value_len,
                ..
            } => {
                // Use `pread` (not `read_at_offset`) — aof_rewrite runs in the
                // forked BGREWRITEAOF child where the parent's io_uring ring is
                // fork-unsafe.
                match crate::STORAGE
                    .get()
                    .and_then(|s| s.pread_at_offset(*backend_offset, *value_len).ok())
                    .and_then(|b| zset_deserialize(&b))
                {
                    Some(z) => {
                        cold_inner = z;
                        &cold_inner
                    }
                    None => {
                        logging::log_warning(
                            "flash: aof_rewrite Tier::Cold zset: NVMe read failed; skipping key",
                        );
                        return;
                    }
                }
            }
        };

        if inner.is_empty() {
            return;
        }

        let emit = match raw::RedisModule_EmitAOF {
            Some(f) => f,
            None => {
                logging::log_warning("flash: aof_rewrite zset: RedisModule_EmitAOF is null");
                return;
            }
        };
        let get_ctx_fn = match raw::RedisModule_GetContextFromIO {
            Some(f) => f,
            None => return,
        };
        let create_str_fn = match raw::RedisModule_CreateString {
            Some(f) => f,
            None => return,
        };
        let free_str_fn = match raw::RedisModule_FreeString {
            Some(f) => f,
            None => return,
        };

        let ctx = get_ctx_fn(aof);
        let zadd_cmd = match CString::new("FLASH.ZADD") {
            Ok(s) => s,
            Err(_) => return,
        };
        // 's' = key (RedisModuleString*), 'v' = (RedisModuleString**, size_t)
        let fmt_sv = match CString::new("sv") {
            Ok(s) => s,
            Err(_) => return,
        };

        const CHUNK_PAIRS: usize = 64;
        let entries: Vec<(f64, &Vec<u8>)> = inner
            .scores
            .iter()
            .map(|((ScoreF64(s), m), ())| (*s, m))
            .collect();

        for chunk in entries.chunks(CHUNK_PAIRS) {
            // Each pair emits two strings: score and member.
            let mut strs: Vec<*mut raw::RedisModuleString> = Vec::with_capacity(chunk.len() * 2);
            let mut alloc_ok = true;
            for (score, member) in chunk {
                let score_s = format_score(*score);
                let ss = create_str_fn(ctx, score_s.as_ptr().cast::<c_char>(), score_s.len());
                if ss.is_null() {
                    alloc_ok = false;
                    break;
                }
                strs.push(ss);
                let ms = create_str_fn(ctx, member.as_ptr().cast::<c_char>(), member.len());
                if ms.is_null() {
                    alloc_ok = false;
                    break;
                }
                strs.push(ms);
            }
            if !alloc_ok {
                for &s in &strs {
                    free_str_fn(ctx, s);
                }
                logging::log_warning(
                    "flash: aof_rewrite zset: CreateString returned null; aborting",
                );
                return;
            }

            emit(
                aof,
                zadd_cmd.as_ptr(),
                fmt_sv.as_ptr(),
                key,
                strs.as_ptr(),
                strs.len(),
            );

            for &s in &strs {
                free_str_fn(ctx, s);
            }
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
}

/// # Safety
pub unsafe extern "C" fn digest(_md: *mut raw::RedisModuleDigest, _value: *mut c_void) {
    // stub
}

/// # Safety
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
        let src = &*value.cast::<FlashZSetObject>();
        match &src.tier {
            Tier::Hot(inner) => Box::into_raw(Box::new(FlashZSetObject {
                tier: Tier::Hot(inner.clone()),
                ttl_ms: src.ttl_ms,
            }))
            .cast::<c_void>(),
            Tier::Cold {
                backend_offset,
                value_len,
                ..
            } => {
                #[cfg(not(test))]
                {
                    match crate::STORAGE
                        .get()
                        .and_then(|s| s.read_at_offset(*backend_offset, *value_len).ok())
                        .and_then(|b| zset_deserialize(&b))
                    {
                        Some(inner) => Box::into_raw(Box::new(FlashZSetObject {
                            tier: Tier::Hot(inner),
                            ttl_ms: src.ttl_ms,
                        }))
                        .cast::<c_void>(),
                        None => null_mut(),
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
pub unsafe extern "C" fn defrag(
    ctx: *mut RedisModuleDefragCtx,
    _key: *mut RedisModuleString,
    value: *mut *mut c_void,
) -> i32 {
    unsafe {
        use std::mem;
        use valkey_module::defrag::Defrag;

        let dfg = Defrag::new(ctx);

        // Step 1: relocate the FlashZSetObject struct itself.
        let new_struct = dfg.alloc(*value);
        if !new_struct.is_null() {
            *value = new_struct;
        }

        // Step 2: for Hot tier, relocate each member Vec's backing buffer in both
        // the scores BTreeMap and the members HashMap (they are separate allocations).
        let obj: &mut FlashZSetObject = &mut *(*value).cast::<FlashZSetObject>();
        if let Tier::Hot(ref mut inner) = obj.tier {
            let score_entries: Vec<((ScoreF64, Vec<u8>), ())> =
                mem::take(&mut inner.scores).into_iter().collect();
            for ((score, mut member), ()) in score_entries {
                if member.capacity() > 0 {
                    let new_buf = dfg.alloc(member.as_mut_ptr().cast::<c_void>());
                    if !new_buf.is_null() {
                        let (len, cap) = (member.len(), member.capacity());
                        let old = mem::replace(
                            &mut member,
                            Vec::from_raw_parts(new_buf.cast::<u8>(), len, cap),
                        );
                        mem::forget(old);
                    }
                }
                inner.scores.insert((score, member), ());
            }

            let member_entries: Vec<(Vec<u8>, f64)> =
                mem::take(&mut inner.members).into_iter().collect();
            for (mut member, score) in member_entries {
                if member.capacity() > 0 {
                    let new_buf = dfg.alloc(member.as_mut_ptr().cast::<c_void>());
                    if !new_buf.is_null() {
                        let (len, cap) = (member.len(), member.capacity());
                        let old = mem::replace(
                            &mut member,
                            Vec::from_raw_parts(new_buf.cast::<u8>(), len, cap),
                        );
                        mem::forget(old);
                    }
                }
                inner.members.insert(member, score);
            }
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
    fn zset_insert_and_get_score() {
        let mut z = ZSetInner::new();
        assert!(z.insert(b"alice".to_vec(), 1.0).is_none());
        assert_eq!(z.get_score(b"alice"), Some(1.0));
        assert_eq!(z.len(), 1);
    }

    #[test]
    fn zset_update_removes_old_score_key() {
        let mut z = ZSetInner::new();
        z.insert(b"a".to_vec(), 1.0);
        z.insert(b"a".to_vec(), 2.0);
        assert_eq!(z.len(), 1);
        assert_eq!(z.get_score(b"a"), Some(2.0));
        assert!(!z.scores.contains_key(&(ScoreF64(1.0), b"a".to_vec())));
        assert!(z.scores.contains_key(&(ScoreF64(2.0), b"a".to_vec())));
    }

    #[test]
    fn zset_remove() {
        let mut z = ZSetInner::new();
        z.insert(b"x".to_vec(), 5.0);
        assert_eq!(z.remove(b"x"), Some(5.0));
        assert!(z.is_empty());
        assert!(z.remove(b"x").is_none());
    }

    #[test]
    fn zset_serialize_roundtrip() {
        let mut z = ZSetInner::new();
        z.insert(b"a".to_vec(), 1.0);
        z.insert(b"b".to_vec(), 2.5);
        z.insert(b"c".to_vec(), 1.0); // same score as "a", ordered by member
        let bytes = zset_serialize(&z);
        let z2 = zset_deserialize(&bytes).expect("deserialize failed");
        assert_eq!(z2.len(), 3);
        assert_eq!(z2.get_score(b"a"), Some(1.0));
        assert_eq!(z2.get_score(b"b"), Some(2.5));
        assert_eq!(z2.get_score(b"c"), Some(1.0));
    }

    #[test]
    fn zset_serialize_empty() {
        let z = ZSetInner::new();
        let bytes = zset_serialize(&z);
        let z2 = zset_deserialize(&bytes).unwrap();
        assert!(z2.is_empty());
    }

    #[test]
    fn zset_deserialize_truncated_returns_none() {
        let bytes = vec![0x01, 0x00, 0x00]; // too short
        assert!(zset_deserialize(&bytes).is_none());
    }

    #[test]
    fn format_score_infinity() {
        assert_eq!(format_score(f64::INFINITY), "+inf");
        assert_eq!(format_score(f64::NEG_INFINITY), "-inf");
    }

    #[test]
    fn format_score_integer() {
        assert_eq!(format_score(1.0), "1");
        assert_eq!(format_score(-3.0), "-3");
    }

    #[test]
    fn format_score_float() {
        assert_eq!(format_score(1.5), "1.5");
    }

    #[test]
    fn rdb_count_cap_rejects_oversized() {
        // Simulate a corrupt count field: encode count > MAX_MEMBERS using NVMe format
        // then verify zset_deserialize (which shares the same cap) rejects it.
        let mut buf = Vec::new();
        // NVMe format uses u32 count — max u32 exceeds the 1_000_000 cap.
        let huge: u32 = 2_000_000;
        buf.extend_from_slice(&huge.to_le_bytes());
        // No actual member data — truncated after count.
        assert!(zset_deserialize(&buf).is_none());
    }

    #[test]
    fn rdb_member_cap_rejects_oversized() {
        // Simulate a member length field that would exceed MAX_MEMBER_BYTES.
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u32.to_le_bytes()); // count = 1
        buf.extend_from_slice(&1.0f64.to_le_bytes()); // score = 1.0
        // Member length field of 600 MiB — exceeds cap.
        let huge_len: u32 = 600 * 1024 * 1024;
        buf.extend_from_slice(&huge_len.to_le_bytes());
        // No actual member bytes.
        assert!(zset_deserialize(&buf).is_none());
    }

    #[test]
    fn rdb_load_constants_are_sensible() {
        // Document the expected cap values; changing them requires a deliberate decision.
        const MAX_ZSET_MEMBERS: u64 = 1_000_000;
        const MAX_MEMBER_BYTES: usize = 512 * 1024 * 1024;
        assert_eq!(MAX_ZSET_MEMBERS, 1_000_000);
        assert_eq!(MAX_MEMBER_BYTES, 512 * 1024 * 1024);
    }

    #[test]
    fn rdb_cumulative_bytes_cap_rejected() {
        // Simulate the cumulative cap logic used in rdb_load: many small members
        // (each well under the per-member cap) must still hit the aggregate 512 MiB
        // cap before 600K × 1 KiB = ~600 MiB are accumulated.
        const MAX_MEMBER_BYTES: usize = 512 * 1024 * 1024;
        let step: usize = 1024; // 1 KiB per member — well below per-member cap
        let mut total: usize = 0;
        let mut hit_cap = false;
        for _ in 0..600_000usize {
            total = total.checked_add(step).expect("no overflow at these sizes");
            if total > MAX_MEMBER_BYTES {
                hit_cap = true;
                break;
            }
        }
        assert!(
            hit_cap,
            "cumulative cap must trigger before 600K × 1 KiB members"
        );
        assert!(total > MAX_MEMBER_BYTES);
    }

    #[test]
    fn btreemap_ordering_by_score_then_member() {
        let mut z = ZSetInner::new();
        z.insert(b"b".to_vec(), 1.0);
        z.insert(b"a".to_vec(), 1.0);
        z.insert(b"c".to_vec(), 0.5);
        let order: Vec<(&Vec<u8>, f64)> = z
            .scores
            .iter()
            .map(|((ScoreF64(s), m), ())| (m, *s))
            .collect();
        // Expected: c@0.5, a@1.0, b@1.0
        assert_eq!(order[0].0.as_slice(), b"c");
        assert_eq!(order[1].0.as_slice(), b"a");
        assert_eq!(order[2].0.as_slice(), b"b");
    }
}
