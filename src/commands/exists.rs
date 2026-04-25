use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

use crate::types::hash::{FLASH_HASH_TYPE, FlashHashObject};
use crate::types::list::{FLASH_LIST_TYPE, FlashListObject};
use crate::types::string::{FLASH_STRING_TYPE, FlashStringObject};
use crate::types::zset::{FLASH_ZSET_TYPE, FlashZSetObject};

// ── Command handler ───────────────────────────────────────────────────────────

/// `FLASH.EXISTS key [key ...]`
///
/// Returns the count of keys that exist as one of the four flash-tier types
/// (FlashString, FlashHash, FlashList, FlashZSet). Mirrors native `EXISTS`
/// semantics: counts argument occurrences rather than unique keys —
/// `FLASH.EXISTS k k` returns 2 when `k` exists.
///
/// Keys that hold non-flash types (native Valkey types or other modules) or
/// that don't exist contribute 0 to the count. Unlike `FLASH.DEL`, no
/// `WRONGTYPE` error is raised; existence checks are best-effort and never
/// partial-fail. This matches what wrappers expect: the policy layer routes a
/// key to flash, but a previously-flash key may have been drained back to
/// native, and the wrapper should observe "not in flash" cleanly rather than
/// see an error.
///
/// Pure metadata read — no NVMe IO, no demotion-pipeline interaction, no
/// replication. The check inspects only the in-RAM keyspace entry and its
/// type tag.
pub fn flash_exists_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 2 {
        return Err(ValkeyError::WrongArity);
    }

    let mut count: i64 = 0;
    for key in &args[1..] {
        let kh = ctx.open_key(key);
        // get_value returns Ok(Some(_)) when the key exists and is the
        // requested type; Ok(None) when the key is absent; Err when present
        // but a different type. We count only Ok(Some(_)) for any of the
        // four flash types — anything else is "not flash, doesn't count".
        if matches!(
            kh.get_value::<FlashStringObject>(&FLASH_STRING_TYPE),
            Ok(Some(_))
        ) || matches!(
            kh.get_value::<FlashHashObject>(&FLASH_HASH_TYPE),
            Ok(Some(_))
        ) || matches!(
            kh.get_value::<FlashListObject>(&FLASH_LIST_TYPE),
            Ok(Some(_))
        ) || matches!(
            kh.get_value::<FlashZSetObject>(&FLASH_ZSET_TYPE),
            Ok(Some(_))
        ) {
            count += 1;
        }
        // kh (ValkeyKey) drops here → CloseKey
    }

    Ok(ValkeyValue::Integer(count))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    // The handler itself requires a live ctx and a populated keyspace, which
    // only the integration suite provides. Unit tests here cover the small
    // amount of pure logic — duplicate-counting and absent-key contribution
    // — at the level of a stand-in counter loop, mirroring del.rs's pattern.

    #[test]
    fn count_increments_per_present_key() {
        let present: Vec<bool> = vec![true, true, true];
        let count: i64 = present.iter().filter(|p| **p).count() as i64;
        assert_eq!(count, 3);
    }

    #[test]
    fn duplicates_count_per_argument_not_per_unique_key() {
        // FLASH.EXISTS k k → 2 if k exists; loop iterates per argument.
        let args: &[&str] = &["k", "k", "missing"];
        let exists = |arg: &str| arg == "k";
        let count: i64 = args.iter().filter(|a| exists(a)).count() as i64;
        assert_eq!(count, 2);
    }

    #[test]
    fn empty_present_set_returns_zero() {
        let present: Vec<bool> = vec![false, false];
        let count: i64 = present.iter().filter(|p| **p).count() as i64;
        assert_eq!(count, 0);
    }
}
