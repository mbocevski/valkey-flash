pub mod backend;
pub mod cache;
pub mod file_io_uring;
pub mod wal;

use serde::{Deserialize, Serialize};

/// A contiguous range of 4 KiB NVMe blocks freed by a delete or overwrite.
/// Stored in the free-list and persisted to aux for cross-session reclaim.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockRange {
    /// Index of the first block in the range (not a byte offset).
    pub start: u64,
    /// Number of contiguous blocks.
    pub len: u32,
}
