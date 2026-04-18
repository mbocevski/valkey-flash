pub mod throttle;

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;
use valkey_module::{logging, raw, Context};

/// `true` when this instance is running in Valkey cluster mode.
///
/// Set at module load from the `flash.cluster-mode-enabled` config knob (default
/// `"auto"`: detected from `ContextFlags::CLUSTER`). Writers use `Release`;
/// readers use `Acquire` so the flag is visible across threads.
pub static IS_CLUSTER: AtomicBool = AtomicBool::new(false);

#[inline]
pub fn is_cluster() -> bool {
    IS_CLUSTER.load(Ordering::Acquire)
}

// ── AtomicSlotMigration subevent constants (valkeymodule.h:670-675, Valkey 9.0) ──
pub const SLOT_MIGRATION_IMPORT_STARTED: u64 = 0;
pub const SLOT_MIGRATION_EXPORT_STARTED: u64 = 1;
pub const SLOT_MIGRATION_IMPORT_ABORTED: u64 = 2;
pub const SLOT_MIGRATION_EXPORT_ABORTED: u64 = 3;
pub const SLOT_MIGRATION_IMPORT_COMPLETED: u64 = 4;
pub const SLOT_MIGRATION_EXPORT_COMPLETED: u64 = 5;

// ── C-compatible migration info structs (valkeymodule.h:837-849) ─────────────
//
// These structs are not in the crate's bundled header (added in Valkey 9.0) so
// we define them manually. Layout must match the C definitions exactly.

#[repr(C)]
pub struct ValkeyModuleSlotRange {
    pub start_slot: ::std::os::raw::c_int,
    pub end_slot: ::std::os::raw::c_int,
}

#[repr(C)]
pub struct ValkeyModuleAtomicSlotMigrationInfoV1 {
    pub version: u64,                           // uint64_t in valkeymodule.h:843
    pub job_name: [::std::os::raw::c_char; 41],
    pub slot_ranges: *const ValkeyModuleSlotRange,
    pub num_slot_ranges: u32,                   // uint32_t in valkeymodule.h:847
}

// ── Cluster slot computation ──────────────────────────────────────────────────
//
// CRC-16/XMODEM (poly 0x1021, init 0, no reflection) — same as Redis/Valkey's
// crc16.c. Table generated from Valkey's src/crc16.c constant array.

#[rustfmt::skip]
const CRC16_TABLE: [u16; 256] = [
    0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7,
    0x8108, 0x9129, 0xa14a, 0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef,
    0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6,
    0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de,
    0x2462, 0x3443, 0x0420, 0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485,
    0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
    0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4,
    0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d, 0xc7bc,
    0x48c4, 0x58e5, 0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823,
    0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b,
    0x5af5, 0x4ad4, 0x7ab7, 0x6a96, 0x1a71, 0x0a50, 0x3a33, 0x2a12,
    0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a,
    0x6ca6, 0x7c87, 0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41,
    0xedae, 0xfd8f, 0xcdec, 0xddcd, 0xad2a, 0xbd0b, 0x8d68, 0x9d49,
    0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70,
    0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78,
    0x9188, 0x81a9, 0xb1ca, 0xa1eb, 0xd10c, 0xc12d, 0xf14e, 0xe16f,
    0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
    0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e,
    0x02b1, 0x1290, 0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256,
    0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
    0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
    0xa7db, 0xb7fa, 0x8799, 0x97b8, 0xe75f, 0xf77e, 0xc71d, 0xd73c,
    0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634,
    0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab,
    0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x08e1, 0x3882, 0x28a3,
    0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
    0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92,
    0xfd2e, 0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8, 0x8dc9,
    0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1,
    0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8,
    0x6e17, 0x7e36, 0x4e55, 0x5e74, 0x2e93, 0x3eb2, 0x0ed1, 0x1ef0,
];

/// Compute the Redis/Valkey cluster slot for `key` (0..16383).
///
/// Supports hash tags: if the key contains `{tag}`, only the `tag` bytes are
/// hashed. An empty tag or unmatched brace falls back to hashing the full key.
pub fn key_slot(key: &[u8]) -> u16 {
    let hash_data = extract_hash_tag(key);
    let mut crc: u16 = 0;
    for &b in hash_data {
        crc = (crc << 8) ^ CRC16_TABLE[((crc >> 8) ^ (b as u16)) as usize];
    }
    crc % 16384
}

fn extract_hash_tag(key: &[u8]) -> &[u8] {
    if let Some(open) = key.iter().position(|&b| b == b'{') {
        if let Some(rel_close) = key[open + 1..].iter().position(|&b| b == b'}') {
            let tag = &key[open + 1..open + 1 + rel_close];
            if !tag.is_empty() {
                return tag;
            }
        }
    }
    key
}

/// Returns `true` if `slot` falls within any of the inclusive `[start, end]` ranges.
pub fn slot_in_ranges(slot: u16, ranges: &[(u16, u16)]) -> bool {
    ranges
        .iter()
        .any(|&(start, end)| slot >= start && slot <= end)
}

// ── Subscribe to AtomicSlotMigration events ───────────────────────────────────

/// Subscribe the real handler for `ValkeyModuleEvent_AtomicSlotMigration` (event 19).
pub(crate) fn subscribe_cluster_events(ctx: &Context) {
    const EVENT_ATOMIC_SLOT_MIGRATION: u64 = 19;
    let status = raw::subscribe_to_server_event(
        ctx.ctx,
        raw::RedisModuleEvent {
            id: EVENT_ATOMIC_SLOT_MIGRATION,
            dataver: 1,
        },
        Some(on_slot_migration),
    );
    if status != raw::Status::Ok {
        logging::log_warning("flash: cluster: failed to subscribe to AtomicSlotMigration events");
    }
}

// ── Migration event handler ───────────────────────────────────────────────────

extern "C" fn on_slot_migration(
    ctx: *mut raw::RedisModuleCtx,
    _eid: raw::RedisModuleEvent,
    subevent: u64,
    data: *mut ::std::os::raw::c_void,
) {
    match subevent {
        SLOT_MIGRATION_EXPORT_STARTED => {
            handle_export_started(ctx, data);
        }
        SLOT_MIGRATION_EXPORT_ABORTED => {
            logging::log_notice("flash: cluster: slot migration export aborted");
        }
        SLOT_MIGRATION_EXPORT_COMPLETED => {
            logging::log_notice("flash: cluster: slot migration export completed");
        }
        SLOT_MIGRATION_IMPORT_STARTED => {
            logging::log_notice("flash: cluster: slot migration import started");
        }
        SLOT_MIGRATION_IMPORT_ABORTED => {
            logging::log_notice("flash: cluster: slot migration import aborted");
        }
        SLOT_MIGRATION_IMPORT_COMPLETED => {
            logging::log_notice("flash: cluster: slot migration import completed");
        }
        _ => {}
    }
}

// ── EXPORT_STARTED pre-warm ───────────────────────────────────────────────────

/// State collected during the keyspace scan for cold-key pre-warming.
struct ExportScanState {
    slot_ranges: Vec<(u16, u16)>,
    max_key_bytes: usize,
    deadline: Instant,
    // (key_bytes, is_flash_string)
    cold_keys: Vec<(Vec<u8>, bool)>,
    timed_out: bool,
}

#[allow(static_mut_refs)]
fn handle_export_started(ctx: *mut raw::RedisModuleCtx, data: *mut ::std::os::raw::c_void) {
    use crate::config::{
        FLASH_MIGRATION_BANDWIDTH_MBPS, FLASH_MIGRATION_CHUNK_TIMEOUT_SEC,
        FLASH_MIGRATION_MAX_KEY_BYTES,
    };

    if data.is_null() {
        logging::log_notice(
            "flash: cluster: EXPORT_STARTED with null migration info — skipping pre-warm",
        );
        return;
    }

    // SAFETY: Valkey guarantees `data` points to a valid ValkeyModuleAtomicSlotMigrationInfoV1
    // for the duration of this callback, and the struct layout matches exactly.
    let info = unsafe { &*(data as *const ValkeyModuleAtomicSlotMigrationInfoV1) };

    // Parse slot ranges from the migration info.
    let mut slot_ranges: Vec<(u16, u16)> = Vec::new();
    if !info.slot_ranges.is_null() && info.num_slot_ranges > 0 {
        let ranges =
            unsafe { std::slice::from_raw_parts(info.slot_ranges, info.num_slot_ranges as usize) };
        for r in ranges {
            slot_ranges.push((r.start_slot as u16, r.end_slot as u16));
        }
    }

    // Decode job_name (null-terminated C string).
    let job_name_bytes: &[u8] = unsafe {
        let ptr = info.job_name.as_ptr() as *const u8;
        let max_len = info.job_name.len();
        let nul = (0..max_len).find(|&i| *ptr.add(i) == 0).unwrap_or(max_len);
        std::slice::from_raw_parts(ptr, nul)
    };
    let job_name = std::str::from_utf8(job_name_bytes).unwrap_or("<invalid>");

    logging::log_notice(
        format!(
            "flash: cluster: EXPORT_STARTED job={job_name} ranges={slot_ranges:?} — pre-warming cold keys"
        )
        .as_str(),
    );

    let timeout_sec = FLASH_MIGRATION_CHUNK_TIMEOUT_SEC.load(Ordering::Relaxed) as u64;
    let max_key_bytes = FLASH_MIGRATION_MAX_KEY_BYTES.load(Ordering::Relaxed) as usize;
    let bw_mbps = FLASH_MIGRATION_BANDWIDTH_MBPS.load(Ordering::Relaxed) as u64;

    // ── Phase 1: scan keyspace and collect Cold Flash keys in migrating slots ──

    let mut scan_state = ExportScanState {
        slot_ranges,
        max_key_bytes,
        deadline: Instant::now() + std::time::Duration::from_secs(timeout_sec),
        cold_keys: Vec::new(),
        timed_out: false,
    };

    unsafe {
        if let (Some(cursor_create), Some(cursor_destroy), Some(scan_fn)) = (
            raw::RedisModule_ScanCursorCreate,
            raw::RedisModule_ScanCursorDestroy,
            raw::RedisModule_Scan,
        ) {
            let cursor = cursor_create();
            if !cursor.is_null() {
                let privdata =
                    &mut scan_state as *mut ExportScanState as *mut ::std::os::raw::c_void;
                // scan_fn returns 1 while more items remain, 0 when done.
                while scan_fn(ctx, cursor, Some(scan_collect_cold_keys), privdata) == 1 {
                    if scan_state.timed_out {
                        break;
                    }
                }
                cursor_destroy(cursor);
            }
        }
    }

    if scan_state.timed_out {
        logging::log_notice(
            "flash: cluster: EXPORT_STARTED scan timed out — remaining cold keys will be read from NVMe during migration",
        );
    }

    // ── Phase 2: promote collected Cold keys to Hot tier ─────────────────────

    let ctx_wrapper = Context { ctx };
    let storage = match crate::STORAGE.get() {
        Some(s) => s,
        None => {
            logging::log_notice("flash: cluster: no NVMe storage — skipping cold-key pre-warm");
            return;
        }
    };
    let cache = match crate::CACHE.get() {
        Some(c) => c,
        None => return,
    };

    let mut bytes_warmed: u64 = 0;
    let mut keys_warmed: u64 = 0;
    let mut keys_skipped: u64 = 0;

    let mut throttle = throttle::BandwidthThrottle::new(bw_mbps);

    for (key_bytes, is_flash_string) in scan_state.cold_keys {
        if Instant::now() >= scan_state.deadline {
            keys_skipped += 1;
            continue;
        }

        let key_str = ctx_wrapper.create_string(key_bytes.as_slice());
        let key_handle = ctx_wrapper.open_key_writable(&key_str);

        let promoted = if is_flash_string {
            promote_flash_string(&key_handle, &key_bytes, storage, cache)
        } else {
            promote_flash_hash(&key_handle, &key_bytes, storage, cache)
        };

        if let Some(value_len) = promoted {
            bytes_warmed += value_len as u64;
            keys_warmed += 1;
            throttle.account(value_len as u64);
        } else {
            keys_skipped += 1;
        }
    }

    logging::log_notice(
        format!(
            "flash: cluster: EXPORT_STARTED pre-warm done: \
             {keys_warmed} keys ({bytes_warmed} bytes) promoted, {keys_skipped} skipped"
        )
        .as_str(),
    );
}

// ── Scan callback (Phase 1) ───────────────────────────────────────────────────

#[allow(static_mut_refs)]
extern "C" fn scan_collect_cold_keys(
    _ctx: *mut raw::RedisModuleCtx,
    keyname: *mut raw::RedisModuleString,
    key: *mut raw::RedisModuleKey,
    privdata: *mut ::std::os::raw::c_void,
) {
    // SAFETY: privdata is always a &mut ExportScanState cast to *mut c_void.
    let state = unsafe { &mut *(privdata as *mut ExportScanState) };

    if state.timed_out {
        return;
    }
    if Instant::now() >= state.deadline {
        state.timed_out = true;
        return;
    }

    // Get key name bytes.
    let mut key_len: usize = 0;
    let key_ptr =
        unsafe { raw::RedisModule_StringPtrLen.unwrap()(keyname as *const _, &mut key_len) };
    if key_ptr.is_null() {
        return;
    }
    let key_bytes = unsafe { std::slice::from_raw_parts(key_ptr as *const u8, key_len) };

    // Only process keys whose slot is in a migrating range.
    let slot = key_slot(key_bytes);
    if !slot_in_ranges(slot, &state.slot_ranges) {
        return;
    }

    // Check whether this is a module-type key.
    let key_type = unsafe { raw::RedisModule_KeyType.unwrap()(key) as isize };
    if key_type != raw::REDISMODULE_KEYTYPE_MODULE {
        return;
    }

    // Identify the specific module type.
    let module_type = unsafe { raw::RedisModule_ModuleTypeGetType.unwrap()(key) };
    if module_type.is_null() {
        return;
    }

    let string_raw = *crate::types::string::FLASH_STRING_TYPE.raw_type.borrow();
    let hash_raw = *crate::types::hash::FLASH_HASH_TYPE.raw_type.borrow();
    let is_flash_string = module_type == string_raw;
    let is_flash_hash = module_type == hash_raw;

    if !is_flash_string && !is_flash_hash {
        return;
    }

    // Get the module value and check if it is Cold tier.
    let value_ptr = unsafe { raw::RedisModule_ModuleTypeGetValue.unwrap()(key) };
    if value_ptr.is_null() {
        return;
    }

    let (is_cold, value_len) = if is_flash_string {
        let obj = unsafe { &*(value_ptr as *const crate::types::string::FlashStringObject) };
        match obj.tier {
            crate::types::Tier::Cold { value_len, .. } => (true, value_len as usize),
            _ => (false, 0),
        }
    } else {
        let obj = unsafe { &*(value_ptr as *const crate::types::hash::FlashHashObject) };
        match obj.tier {
            crate::types::Tier::Cold { value_len, .. } => (true, value_len as usize),
            _ => (false, 0),
        }
    };

    if !is_cold {
        return;
    }

    if value_len > state.max_key_bytes {
        logging::log_notice(
            format!(
                "flash: cluster: migration skipping oversized key ({value_len}B > max {}B)",
                state.max_key_bytes
            )
            .as_str(),
        );
        return;
    }

    state.cold_keys.push((key_bytes.to_vec(), is_flash_string));
}

// ── Per-key promotion helpers (Phase 2) ──────────────────────────────────────

fn promote_flash_string(
    key_handle: &valkey_module::key::ValkeyKeyWritable,
    key_bytes: &[u8],
    storage: &crate::storage::file_io_uring::FileIoUringBackend,
    cache: &crate::storage::cache::FlashCache,
) -> Option<u32> {
    use crate::types::{string::FlashStringObject, Tier};
    let obj = key_handle
        .get_value::<FlashStringObject>(&crate::types::string::FLASH_STRING_TYPE)
        .ok()??;
    let (backend_offset, value_len) = match obj.tier {
        Tier::Cold {
            backend_offset,
            value_len,
            ..
        } => (backend_offset, value_len),
        _ => return None, // already hot
    };
    let bytes = storage.read_at_offset(backend_offset, value_len).ok()?;
    cache.put(key_bytes, bytes.clone());
    obj.tier = Tier::Hot(bytes);
    Some(value_len)
}

fn promote_flash_hash(
    key_handle: &valkey_module::key::ValkeyKeyWritable,
    key_bytes: &[u8],
    storage: &crate::storage::file_io_uring::FileIoUringBackend,
    cache: &crate::storage::cache::FlashCache,
) -> Option<u32> {
    use crate::types::{hash::FlashHashObject, Tier};
    let obj = key_handle
        .get_value::<FlashHashObject>(&crate::types::hash::FLASH_HASH_TYPE)
        .ok()??;
    let (backend_offset, value_len) = match obj.tier {
        Tier::Cold {
            backend_offset,
            value_len,
            ..
        } => (backend_offset, value_len),
        _ => return None,
    };
    let bytes = storage.read_at_offset(backend_offset, value_len).ok()?;
    // Deserialize the hash from NVMe bytes and put serialized form in cache.
    cache.put(key_bytes, bytes.clone());
    let fields = crate::types::hash::hash_deserialize(&bytes)?;
    obj.tier = Tier::Hot(fields);
    Some(value_len)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn reset() {
        IS_CLUSTER.store(false, Ordering::SeqCst);
    }

    #[test]
    fn is_cluster_starts_false() {
        reset();
        assert!(!is_cluster());
    }

    #[test]
    fn is_cluster_true_after_store() {
        IS_CLUSTER.store(true, Ordering::SeqCst);
        assert!(is_cluster());
        reset();
    }

    #[test]
    fn is_cluster_false_after_clear() {
        IS_CLUSTER.store(true, Ordering::SeqCst);
        IS_CLUSTER.store(false, Ordering::SeqCst);
        assert!(!is_cluster());
    }

    #[test]
    fn slot_in_ranges_matches_inclusive() {
        assert!(slot_in_ranges(0, &[(0, 5462)]));
        assert!(slot_in_ranges(5462, &[(0, 5462)]));
        assert!(!slot_in_ranges(5463, &[(0, 5462)]));
        assert!(slot_in_ranges(8000, &[(0, 5462), (7000, 10000)]));
    }

    #[test]
    fn key_slot_known_values() {
        // "foo" → crc16 = 0xaf96, slot = 44950 % 16384 = 12182 (verified by Python
        // using Valkey's crc16tab, same algorithm as keyHashSlot in cluster.c)
        assert_eq!(key_slot(b"foo"), 12182);
        // "{foo}.bar" → hashes only "foo" → same slot as "foo"
        assert_eq!(key_slot(b"{foo}.bar"), key_slot(b"foo"));
    }

    #[test]
    fn key_slot_empty_brace_uses_full_key() {
        // "{}" — empty tag, fall back to full key
        assert_eq!(key_slot(b"{}foo"), key_slot(b"{}foo"));
        assert_ne!(key_slot(b"{}foo"), key_slot(b"foo"));
    }

    #[test]
    fn key_slot_unmatched_brace_uses_full_key() {
        assert_eq!(key_slot(b"{foo"), key_slot(b"{foo"));
    }

    #[test]
    fn extract_hash_tag_inner() {
        assert_eq!(extract_hash_tag(b"{foo}.bar"), b"foo");
        assert_eq!(extract_hash_tag(b"{}foo"), b"{}foo"); // empty tag → full key
        assert_eq!(extract_hash_tag(b"nobraces"), b"nobraces");
    }

    #[test]
    fn crc16_known_value() {
        // "123456789" → 0x31C3 per CRC-CCITT spec
        let mut crc: u16 = 0;
        for &b in b"123456789" {
            crc = (crc << 8) ^ CRC16_TABLE[((crc >> 8) ^ (b as u16)) as usize];
        }
        assert_eq!(crc, 0x31C3);
    }
}
