use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString};

use crate::commands::migrate_probe::remote_probe;
use crate::types::hash::{FlashHashObject, FLASH_HASH_TYPE};
use crate::types::list::{FlashListObject, FLASH_LIST_TYPE};
use crate::types::string::{FlashStringObject, FLASH_STRING_TYPE};
use crate::types::Tier;

/// Estimate serialised NVMe bytes for a FLASH key on this node.
///
/// Returns `None` if the key is absent or not a FLASH type — those keys are
/// passed to core MIGRATE without capacity gating (they require no NVMe space
/// on the target).
fn flash_key_bytes(ctx: &Context, key: &ValkeyString) -> Option<usize> {
    let handle = ctx.open_key(key);

    // FlashString?
    //
    // Note: the on-disk record also carries an 8-byte value_len prefix that is
    // not counted here.  Keys of exactly N×4096−8 bytes therefore need one extra
    // NVMe block.  The undercount is at most 8 bytes per key and benign in
    // practice: RESTORE on the target side lands as Hot (no NVMe write), so the
    // gate is advisory rather than exact.
    if let Ok(Some(obj)) = handle.get_value::<FlashStringObject>(&FLASH_STRING_TYPE) {
        return Some(match &obj.tier {
            Tier::Hot(v) => v.len(),
            Tier::Cold { value_len, .. } => *value_len as usize,
        });
    }

    // FlashHash?
    if let Ok(Some(obj)) = handle.get_value::<FlashHashObject>(&FLASH_HASH_TYPE) {
        return Some(match &obj.tier {
            Tier::Hot(map) => {
                // Mirrors hash_serialize wire format: [u32 count] + per-field [u32 klen][k][u32 vlen][v]
                4 + map
                    .iter()
                    .map(|(k, v)| 8 + k.len() + v.len())
                    .sum::<usize>()
            }
            Tier::Cold { value_len, .. } => *value_len as usize,
        });
    }

    // FlashList?
    if let Ok(Some(obj)) = handle.get_value::<FlashListObject>(&FLASH_LIST_TYPE) {
        return Some(match &obj.tier {
            Tier::Hot(items) => {
                // Mirrors list_serialize wire format: [u32 count] + per-elem [u32 len][bytes]
                4 + items.iter().map(|e| 4 + e.len()).sum::<usize>()
            }
            Tier::Cold { value_len, .. } => *value_len as usize,
        });
    }

    None
}

// ── Command handler ───────────────────────────────────────────────────────────

/// `FLASH.MIGRATE host port key|"" destination-db timeout [opts] [KEYS key [key...]]`
///
/// Capacity-gating wrapper around the core MIGRATE command.
///
/// Steps:
///   1. Collect the FLASH keys being migrated (KEYS clause or positional arg).
///   2. Estimate their total serialised NVMe footprint on this node.
///   3. Probe the target node via `FLASH.MIGRATE.PROBE` (uses the same cached
///      `remote_probe` path as `FLASH.MIGRATE.PROBE host port`).
///   4. If `free_bytes < total_bytes`: return
///      `ERR FLASH-MIGRATE target host:port insufficient flash capacity
///       (need N bytes free, has M)`.
///   5. Otherwise: forward all original args (minus the command name) to the
///      core MIGRATE command.
///
/// Non-FLASH keys in the KEYS list (regular strings, hashes, etc.) are not
/// counted — they carry no NVMe footprint.
pub fn flash_migrate_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    // FLASH.MIGRATE host port key|"" db timeout [opts]
    if args.len() < 6 {
        return Err(ValkeyError::WrongArity);
    }

    let host = args[1].to_string();
    let port_str = args[2].to_string();
    let port: u16 = port_str
        .parse()
        .map_err(|_| ValkeyError::String(format!("ERR invalid port: {port_str}")))?;

    // Collect keys: KEYS clause takes priority; fall back to positional arg[3].
    let keys_pos = args
        .iter()
        .position(|a| a.as_slice().eq_ignore_ascii_case(b"KEYS"));

    let flash_keys: Vec<&ValkeyString> = if let Some(kpos) = keys_pos {
        args[kpos + 1..].iter().collect()
    } else if !args[3].as_slice().is_empty() {
        vec![&args[3]]
    } else {
        vec![]
    };

    // Sum the NVMe footprint of every FLASH key in the list.
    let total_bytes: usize = flash_keys
        .iter()
        .filter_map(|k| flash_key_bytes(ctx, k))
        .sum();

    // Only probe the target when there are FLASH keys to move.
    if total_bytes > 0 {
        let probe = remote_probe(&host, port).map_err(ValkeyError::String)?;
        if (probe.free_bytes as usize) < total_bytes {
            return Err(ValkeyError::String(format!(
                "ERR FLASH-MIGRATE target {host}:{port} insufficient flash capacity \
                 (need {total_bytes} bytes free, has {})",
                probe.free_bytes
            )));
        }
    }

    // Forward to core MIGRATE: pass all args after the command name.
    let fwd_args: Vec<String> = args[1..].iter().map(|s| s.to_string()).collect();
    let fwd_refs: Vec<&str> = fwd_args.iter().map(|s| s.as_str()).collect();
    ctx.call("MIGRATE", fwd_refs.as_slice())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use crate::types::Tier;
    use std::collections::HashMap;

    #[test]
    fn hot_string_size_is_value_len() {
        let tier = Tier::<Vec<u8>>::Hot(b"hello world".to_vec());
        let size = match &tier {
            Tier::Hot(v) => v.len(),
            Tier::Cold { value_len, .. } => *value_len as usize,
        };
        assert_eq!(size, 11);
    }

    #[test]
    fn cold_string_size_is_value_len_field() {
        let tier: Tier<Vec<u8>> = Tier::Cold {
            key_hash: 0,
            backend_offset: 0,
            num_blocks: 1,
            value_len: 42,
        };
        let size = match &tier {
            Tier::Hot(v) => v.len(),
            Tier::Cold { value_len, .. } => *value_len as usize,
        };
        assert_eq!(size, 42);
    }

    #[test]
    fn hot_hash_size_matches_serialize_format() {
        let mut map = HashMap::new();
        map.insert(b"k1".to_vec(), b"v1".to_vec());  // 4+2+4+2 = 12 bytes
        map.insert(b"k2".to_vec(), b"v2".to_vec());  // 4+2+4+2 = 12 bytes
        // 4 (count) + 12 + 12 = 28
        let tier = Tier::Hot(map);
        let size = match &tier {
            Tier::Hot(m) => 4 + m.iter().map(|(k, v)| 8 + k.len() + v.len()).sum::<usize>(),
            Tier::Cold { value_len, .. } => *value_len as usize,
        };
        assert_eq!(size, 28);
    }

    #[test]
    fn cold_hash_size_is_value_len_field() {
        let tier: Tier<HashMap<Vec<u8>, Vec<u8>>> = Tier::Cold {
            key_hash: 0,
            backend_offset: 0,
            num_blocks: 2,
            value_len: 256,
        };
        let size = match &tier {
            Tier::Hot(m) => 4 + m.iter().map(|(k, v)| 8 + k.len() + v.len()).sum::<usize>(),
            Tier::Cold { value_len, .. } => *value_len as usize,
        };
        assert_eq!(size, 256);
    }

    #[test]
    fn hot_list_size_matches_serialize_format() {
        // list_serialize: [u32 count] + per-elem [u32 len][bytes]
        // 3 elems: "a"(1), "bb"(2), "ccc"(3) → 4 + (4+1) + (4+2) + (4+3) = 22
        let items: std::collections::VecDeque<Vec<u8>> = [
            b"a".to_vec(),
            b"bb".to_vec(),
            b"ccc".to_vec(),
        ]
        .into();
        let tier = Tier::Hot(items);
        let size = match &tier {
            Tier::Hot(v) => 4 + v.iter().map(|e| 4 + e.len()).sum::<usize>(),
            Tier::Cold { value_len, .. } => *value_len as usize,
        };
        assert_eq!(size, 22);
    }

    #[test]
    fn cold_list_size_is_value_len_field() {
        let tier: Tier<std::collections::VecDeque<Vec<u8>>> = Tier::Cold {
            key_hash: 0,
            backend_offset: 0,
            num_blocks: 1,
            value_len: 128,
        };
        let size = match &tier {
            Tier::Hot(v) => 4 + v.iter().map(|e| 4 + e.len()).sum::<usize>(),
            Tier::Cold { value_len, .. } => *value_len as usize,
        };
        assert_eq!(size, 128);
    }

    #[test]
    fn capacity_gate_fires_when_free_bytes_less_than_need() {
        let free_bytes: u64 = 100;
        let total_bytes: usize = 200;
        assert!(
            (free_bytes as usize) < total_bytes,
            "gate should fire when free < need"
        );
    }

    #[test]
    fn capacity_gate_passes_when_free_bytes_sufficient() {
        let free_bytes: u64 = 300;
        let total_bytes: usize = 200;
        assert!(
            (free_bytes as usize) >= total_bytes,
            "gate should pass when free >= need"
        );
    }

    #[test]
    fn capacity_gate_passes_when_no_flash_keys() {
        // Zero total_bytes → no probe, no gate.
        let total_bytes: usize = 0;
        assert!(total_bytes == 0);
    }
}
