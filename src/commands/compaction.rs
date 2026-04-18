use std::sync::atomic::Ordering;
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

use crate::storage::file_io_uring::{BYTES_RECLAIMED, COMPACTION_RUNS};

/// `FLASH.COMPACTION.STATS`
///
/// Returns compaction metrics as an array of bulk strings in `key:value` format:
///   - `compaction_runs` — total coalesce ticks since module load
///   - `bytes_reclaimed` — cumulative NVMe bytes freed by delete/overwrite
///   - `free_blocks` — current number of free 4 KiB NVMe blocks in the free-list
pub fn flash_compaction_stats_command(_ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() != 1 {
        return Err(ValkeyError::WrongArity);
    }
    let runs = COMPACTION_RUNS.load(Ordering::Relaxed);
    let reclaimed = BYTES_RECLAIMED.load(Ordering::Relaxed);
    let free = crate::STORAGE
        .get()
        .map(|s| s.free_block_count())
        .unwrap_or(0);

    Ok(ValkeyValue::Array(vec![
        ValkeyValue::BulkString(format!("compaction_runs:{runs}")),
        ValkeyValue::BulkString(format!("bytes_reclaimed:{reclaimed}")),
        ValkeyValue::BulkString(format!("free_blocks:{free}")),
    ]))
}

/// `FLASH.COMPACTION.TRIGGER`
///
/// Synchronously run one compaction tick (free-list coalesce) and return `OK`.
/// Intended for testing and manual operation; the background thread runs ticks
/// automatically per `flash.compaction-interval-sec`.
pub fn flash_compaction_trigger_command(_ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() != 1 {
        return Err(ValkeyError::WrongArity);
    }
    match crate::STORAGE.get() {
        Some(storage) => {
            storage.run_compaction_tick();
            Ok(ValkeyValue::SimpleString("OK".into()))
        }
        None => Err(ValkeyError::Str("ERR flash module not initialized")),
    }
}
