use std::collections::HashMap;
use std::path::Path;
use std::time::Instant;

use crate::persistence::aux::AuxState;
use crate::storage::wal::{Wal, WalError, WalOp, WalSyncMode};

// ── Module state ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModuleState {
    Recovering,
    Ready,
    Error,
}

// ── Tiering map entry ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TierEntry {
    pub offset: u64,
    pub value_hash: u64,
}

// ── Recovery result ───────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct RecoveryStats {
    pub records_applied: u64,
    pub elapsed_ms: u128,
    /// Non-fatal warnings (e.g. torn-tail truncation). Logged by `initialize()`.
    pub warnings: Vec<String>,
}

// ── Recovery error ────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum RecoveryError {
    ConfigMismatch {
        field: &'static str,
        aux: String,
        current: String,
    },
    Wal(WalError),
}

impl std::fmt::Display for RecoveryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecoveryError::ConfigMismatch {
                field,
                aux,
                current,
            } => write!(
                f,
                "config mismatch on '{field}': \
                 aux had '{aux}', current config has '{current}'"
            ),
            RecoveryError::Wal(e) => write!(f, "WAL error: {e}"),
        }
    }
}

impl From<WalError> for RecoveryError {
    fn from(e: WalError) -> Self {
        RecoveryError::Wal(e)
    }
}

// ── Recovery ──────────────────────────────────────────────────────────────────

/// Replay the WAL and reconstruct the in-memory tiering map.
///
/// Steps:
/// 1. Validate stored config against current config (path, capacity).
/// 2. Seed tiering map from aux tiering entries (cold-tier snapshot at save time).
/// 3. Open the WAL and iterate from `wal_cursor` (0 = fresh start).
/// 4. Apply Put/Delete/Checkpoint ops idempotently.
/// 5. On `Corruption` (CRC mismatch or torn tail): truncate WAL at that offset,
///    log a warning, and continue — module will be `Ready`.
/// 6. On any other WAL error (I/O, bad magic, bad version): return `Err` →
///    `initialize()` returns `Status::Err` and module load fails.
pub fn run_recovery(
    aux: Option<&AuxState>,
    wal_path: &Path,
    current_path: &str,
    current_capacity: u64,
) -> Result<(RecoveryStats, HashMap<u64, TierEntry>), RecoveryError> {
    let start = Instant::now();
    let mut tiering_map: HashMap<u64, TierEntry> = HashMap::new();

    // ── 1. Config mismatch guard ──────────────────────────────────────────────

    if let Some(state) = aux {
        if state.before.path != current_path {
            return Err(RecoveryError::ConfigMismatch {
                field: "path",
                aux: state.before.path.clone(),
                current: current_path.to_string(),
            });
        }
        if state.before.capacity_bytes != current_capacity {
            return Err(RecoveryError::ConfigMismatch {
                field: "capacity-bytes",
                aux: state.before.capacity_bytes.to_string(),
                current: current_capacity.to_string(),
            });
        }

        // ── 2. Seed tiering map from aux entries ──────────────────────────────
        // Tier tag 1 = Cold; those entries live on NVMe and need map entries.
        // Aux doesn't store offset/value_hash yet, so use 0.
        for entry in &state.before.entries {
            if entry.tier_tag == 1 {
                tiering_map.insert(
                    entry.key_hash,
                    TierEntry {
                        offset: 0,
                        value_hash: 0,
                    },
                );
            }
        }
    }

    // ── 3. Determine start cursor ─────────────────────────────────────────────

    let wal_cursor = aux.map(|a| a.before.wal_cursor).unwrap_or(0);

    // ── 4. Open WAL (creates header if new) ──────────────────────────────────

    let wal = Wal::open(wal_path, WalSyncMode::No).map_err(|e| match e {
        WalError::BadMagic | WalError::BadVersion(_) | WalError::Io(_) => RecoveryError::Wal(e),
        other => RecoveryError::Wal(other),
    })?;

    // ── 5. Replay from cursor ─────────────────────────────────────────────────

    let iter = wal.iter_records_from(wal_cursor)?;
    let mut records_applied: u64 = 0;
    let mut truncate_at: Option<u64> = None;
    let mut warnings: Vec<String> = Vec::new();

    for result in iter {
        match result {
            Ok(op) => {
                match op {
                    WalOp::Put {
                        key_hash,
                        offset,
                        value_hash,
                    } => {
                        tiering_map.insert(key_hash, TierEntry { offset, value_hash });
                        records_applied += 1;
                    }
                    WalOp::Delete { key_hash } => {
                        tiering_map.remove(&key_hash);
                        records_applied += 1;
                    }
                    WalOp::Checkpoint { .. } => {
                        // Cursor already handled by iter_records_from start point.
                        records_applied += 1;
                    }
                }
            }
            // CRC mismatch or torn tail → truncate and stop (not fatal).
            Err(WalError::Corruption { offset, reason }) => {
                warnings.push(format!(
                    "flash: recovery: WAL corruption at offset {offset} ({reason}); \
                     truncating — this is expected after an unclean shutdown"
                ));
                truncate_at = Some(offset);
                break;
            }
            // Real I/O error → fatal: do not proceed with potentially partial state.
            Err(e) => {
                return Err(RecoveryError::Wal(e));
            }
        }
    }

    // ── 6. Truncate torn tail if detected ────────────────────────────────────

    if let Some(offset) = truncate_at {
        wal.truncate_at(offset)?;
    }

    let elapsed_ms = start.elapsed().as_millis();
    Ok((
        RecoveryStats {
            records_applied,
            elapsed_ms,
            warnings,
        },
        tiering_map,
    ))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::aux::{AuxBeforePayload, AuxState};
    use crate::storage::wal::{WalOp, WalSyncMode};
    use tempfile::tempdir;

    fn aux_state(path: &str, capacity: u64, cursor: u64) -> AuxState {
        AuxState {
            before: AuxBeforePayload {
                magic: 0x5853_4C46,
                version: 1,
                entries: Vec::new(),
                path: path.to_string(),
                capacity_bytes: capacity,
                io_uring_entries: 256,
                wal_cursor: cursor,
                nvme_next_block: 0,
                free_blocks: Vec::new(),
            },
            after: None,
        }
    }

    fn put(key_hash: u64) -> WalOp {
        WalOp::Put {
            key_hash,
            offset: key_hash * 4096,
            value_hash: key_hash ^ 0xABCD,
        }
    }

    fn del(key_hash: u64) -> WalOp {
        WalOp::Delete { key_hash }
    }

    #[test]
    fn fresh_start_no_aux_empty_wal() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");
        let path = "/tmp/test.bin";
        let capacity = 1 << 20;

        let (stats, map) = run_recovery(None, &wal_path, path, capacity).unwrap();
        assert_eq!(stats.records_applied, 0);
        assert!(map.is_empty());
    }

    #[test]
    fn put_and_delete_applied() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");
        let path = "/tmp/test.bin";
        let capacity = 1 << 20;

        {
            let wal = Wal::open(&wal_path, WalSyncMode::No).unwrap();
            wal.append(put(1)).unwrap();
            wal.append(put(2)).unwrap();
            wal.append(del(1)).unwrap();
        }

        let (stats, map) = run_recovery(None, &wal_path, path, capacity).unwrap();
        assert_eq!(stats.records_applied, 3);
        assert!(!map.contains_key(&1));
        assert!(map.contains_key(&2));
        assert_eq!(
            map[&2],
            TierEntry {
                offset: 2 * 4096,
                value_hash: 2 ^ 0xABCD
            }
        );
    }

    #[test]
    fn cursor_skips_earlier_records() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");
        let path = "/tmp/test.bin";
        let capacity = 1 << 20;

        let cursor = {
            let wal = Wal::open(&wal_path, WalSyncMode::No).unwrap();
            wal.append(put(10)).unwrap();
            wal.append(put(20)).unwrap();
            // Cursor = after these two records (simulates BGSAVE point)
            wal.current_offset().unwrap()
        };

        {
            let wal = Wal::open(&wal_path, WalSyncMode::No).unwrap();
            wal.append(put(30)).unwrap(); // after cursor — should be applied
        }

        let state = aux_state(path, capacity, cursor);
        let (stats, map) = run_recovery(Some(&state), &wal_path, path, capacity).unwrap();
        // Only the record after cursor (put(30)) is applied.
        assert_eq!(stats.records_applied, 1);
        assert!(!map.contains_key(&10));
        assert!(!map.contains_key(&20));
        assert!(map.contains_key(&30));
    }

    #[test]
    fn config_mismatch_path_is_fatal() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");
        let state = aux_state("/old/path.bin", 1 << 20, 0);
        let result = run_recovery(Some(&state), &wal_path, "/new/path.bin", 1 << 20);
        assert!(
            matches!(
                result,
                Err(RecoveryError::ConfigMismatch { field: "path", .. })
            ),
            "expected ConfigMismatch(path), got {result:?}"
        );
    }

    #[test]
    fn config_mismatch_capacity_is_fatal() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");
        let state = aux_state("/tmp/f.bin", 1 << 20, 0);
        let result = run_recovery(Some(&state), &wal_path, "/tmp/f.bin", 1 << 30);
        assert!(
            matches!(
                result,
                Err(RecoveryError::ConfigMismatch {
                    field: "capacity-bytes",
                    ..
                })
            ),
            "expected ConfigMismatch(capacity-bytes), got {result:?}"
        );
    }

    #[test]
    fn crc_corrupt_record_truncates_and_module_continues() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");
        let path = "/tmp/test.bin";
        let capacity = 1 << 20;

        {
            let wal = Wal::open(&wal_path, WalSyncMode::No).unwrap();
            wal.append(put(1)).unwrap();
            wal.append(put(2)).unwrap();
        }

        // Corrupt byte 24 (first payload byte of record 1) to trigger CRC mismatch.
        {
            use std::fs::OpenOptions;
            use std::os::unix::fs::FileExt;
            let f = OpenOptions::new().write(true).open(&wal_path).unwrap();
            f.write_at(&[0xFF], 24).unwrap();
        }

        let (stats, map) = run_recovery(None, &wal_path, path, capacity).unwrap();
        assert_eq!(stats.records_applied, 0);
        assert!(map.is_empty());
        // WAL should be truncated to the corrupt frame start (offset 16).
        let file_len = std::fs::metadata(&wal_path).unwrap().len();
        assert_eq!(
            file_len, 16,
            "WAL should be truncated to header-only after corruption"
        );
    }

    #[test]
    fn idempotent_replay_same_put_twice() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");
        let path = "/tmp/test.bin";
        let capacity = 1 << 20;

        {
            let wal = Wal::open(&wal_path, WalSyncMode::No).unwrap();
            wal.append(put(5)).unwrap();
            wal.append(put(5)).unwrap(); // same key — idempotent overwrite
        }

        let (stats, map) = run_recovery(None, &wal_path, path, capacity).unwrap();
        assert_eq!(stats.records_applied, 2);
        assert_eq!(map.len(), 1);
        assert_eq!(
            map[&5],
            TierEntry {
                offset: 5 * 4096,
                value_hash: 5 ^ 0xABCD
            }
        );
    }

    #[test]
    fn checkpoint_op_counted() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");
        let path = "/tmp/test.bin";
        let capacity = 1 << 20;

        {
            let wal = Wal::open(&wal_path, WalSyncMode::No).unwrap();
            wal.append(WalOp::Checkpoint { at: 100 }).unwrap();
        }

        let (stats, map) = run_recovery(None, &wal_path, path, capacity).unwrap();
        assert_eq!(stats.records_applied, 1);
        assert!(map.is_empty());
    }
}
