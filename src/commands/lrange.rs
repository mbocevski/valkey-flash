use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

use crate::CACHE;
use crate::commands::list_common::resolve_range;
use crate::types::Tier;
use crate::types::list::{FLASH_LIST_TYPE, FlashListObject, list_deserialize, list_serialize};

// ── LRangeCompletionHandle ────────────────────────────────────────────────────

#[cfg(not(test))]
struct LRangeCompletionHandle {
    tsc: valkey_module::ThreadSafeContext<valkey_module::BlockedClient<()>>,
    key: Vec<u8>,
    start: i64,
    stop: i64,
    cache: &'static crate::storage::cache::FlashCache,
}

#[cfg(not(test))]
impl crate::async_io::CompletionHandle for LRangeCompletionHandle {
    fn complete(self: Box<Self>, result: crate::storage::backend::StorageResult<Vec<u8>>) {
        let reply = match result {
            Err(e) => Err(valkey_module::ValkeyError::String(e.to_string())),
            Ok(bytes) => {
                self.cache.put(&self.key, bytes.clone());
                match list_deserialize(&bytes) {
                    None => Ok(ValkeyValue::Array(vec![])),
                    Some(items) => {
                        let (s, e) = resolve_range(self.start, self.stop, items.len());
                        let elems = items
                            .range(s..e)
                            .map(|v| ValkeyValue::StringBuffer(v.clone()))
                            .collect();
                        Ok(ValkeyValue::Array(elems))
                    }
                }
            }
        };
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.tsc.reply(reply);
        }));
    }
}

// ── Command handler ───────────────────────────────────────────────────────────

/// `FLASH.LRANGE key start stop`
pub fn flash_lrange_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() != 4 {
        return Err(ValkeyError::WrongArity);
    }

    let key = &args[1];

    let start: i64 = std::str::from_utf8(args[2].as_slice())
        .map_err(|_| ValkeyError::Str("ERR value is not an integer or out of range"))?
        .parse()
        .map_err(|_| ValkeyError::Str("ERR value is not an integer or out of range"))?;
    let stop: i64 = std::str::from_utf8(args[3].as_slice())
        .map_err(|_| ValkeyError::Str("ERR value is not an integer or out of range"))?
        .parse()
        .map_err(|_| ValkeyError::Str("ERR value is not an integer or out of range"))?;

    let cache = CACHE
        .get()
        .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;

    if let Some(cached_bytes) = cache.get(key.as_slice()) {
        let items = list_deserialize(&cached_bytes).unwrap_or_default();
        let (s, e) = resolve_range(start, stop, items.len());
        let elems = items
            .range(s..e)
            .map(|v| ValkeyValue::StringBuffer(v.clone()))
            .collect();
        return Ok(ValkeyValue::Array(elems));
    }

    let cold_info: Option<(u64, u32)>;
    {
        let key_handle = ctx.open_key(key);
        let obj = match key_handle.get_value::<FlashListObject>(&FLASH_LIST_TYPE) {
            Err(_) => return Err(ValkeyError::WrongType),
            Ok(None) => return Ok(ValkeyValue::Array(vec![])),
            Ok(Some(obj)) => obj,
        };

        match &obj.tier {
            Tier::Hot(items) => {
                let serialized = list_serialize(items);
                cache.put(key.as_slice(), serialized);
                let (s, e) = resolve_range(start, stop, items.len());
                let elems = items
                    .range(s..e)
                    .map(|v| ValkeyValue::StringBuffer(v.clone()))
                    .collect();
                return Ok(ValkeyValue::Array(elems));
            }
            Tier::Cold {
                backend_offset,
                value_len,
                ..
            } => {
                cold_info = Some((*backend_offset, *value_len));
            }
        }
    }

    let (backend_offset, value_len) = cold_info.unwrap();

    #[cfg(not(test))]
    {
        let storage = crate::STORAGE
            .get()
            .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;
        let pool = crate::POOL
            .get()
            .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;
        let key_bytes = key.as_slice().to_vec();
        let bc = ctx.block_client();
        let handle = Box::new(LRangeCompletionHandle {
            tsc: valkey_module::ThreadSafeContext::with_blocked_client(bc),
            key: key_bytes,
            start,
            stop,
            cache,
        });
        pool.submit_or_complete(handle, move || {
            storage.read_at_offset(backend_offset, value_len)
        });
        return Ok(ValkeyValue::NoReply);
    }

    #[allow(unused_variables, unreachable_code)]
    {
        let _ = (backend_offset, value_len);
        Ok(ValkeyValue::Array(vec![]))
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    #[test]
    fn lrange_full_list() {
        let mut items = VecDeque::new();
        items.push_back(b"a".to_vec());
        items.push_back(b"b".to_vec());
        items.push_back(b"c".to_vec());
        let (s, e) = resolve_range(0, -1, items.len());
        assert_eq!(
            &items.range(s..e).cloned().collect::<Vec<_>>(),
            &[b"a", b"b", b"c"]
        );
    }

    #[test]
    fn lrange_partial() {
        let mut items = VecDeque::new();
        for c in b"abcde" {
            items.push_back(vec![*c]);
        }
        let (s, e) = resolve_range(1, 3, items.len());
        let result: Vec<Vec<u8>> = items.range(s..e).cloned().collect();
        assert_eq!(result, vec![b"b".to_vec(), b"c".to_vec(), b"d".to_vec()]);
    }

    #[test]
    fn lrange_out_of_range_returns_empty() {
        let items: VecDeque<Vec<u8>> = VecDeque::new();
        let (s, e) = resolve_range(5, 10, items.len());
        assert_eq!(s, e);
    }
}
