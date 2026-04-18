pub mod hash;
pub mod string;

// ── Tier ──────────────────────────────────────────────────────────────────────

/// Identifies whether the object's payload lives in RAM (hot) or on NVMe (cold).
///
/// Command handlers create `Tier::Hot(payload)` when writing a new key.
/// The tiering logic demotes to `Tier::Cold { .. }` when evicting to NVMe.
#[derive(Debug, PartialEq)]
pub enum Tier<T> {
    /// Payload is in RAM. The `T` holds the actual value bytes.
    Hot(T),
    /// Payload is on NVMe only. All fields needed for read, reclaim, and WAL.
    Cold {
        /// FNV-1a key hash — used to emit `WalOp::Delete` when the key is freed.
        key_hash: u64,
        /// NVMe byte offset of the first block (= block_index × 4096).
        backend_offset: u64,
        /// Number of contiguous 4 KiB NVMe blocks occupied.
        num_blocks: u32,
        /// Serialised byte length of the value, for NVMe reads (task #57).
        value_len: u32,
    },
}
