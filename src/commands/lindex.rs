use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

use crate::CACHE;
use crate::commands::list_common::resolve_index;
use crate::types::Tier;
use crate::types::list::{FLASH_LIST_TYPE, FlashListObject, list_deserialize, list_serialize};

// ── LIndexCompletionHandle ────────────────────────────────────────────────────

#[cfg(not(test))]
struct LIndexCompletionHandle {
    tsc: valkey_module::ThreadSafeContext<valkey_module::BlockedClient<()>>,
    key: Vec<u8>,
    index: i64,
    cache: &'static crate::storage::cache::FlashCache,
}

#[cfg(not(test))]
impl crate::async_io::CompletionHandle for LIndexCompletionHandle {
    fn complete(self: Box<Self>, result: crate::storage::backend::StorageResult<Vec<u8>>) {
        let reply = match result {
            Err(e) => Err(valkey_module::ValkeyError::String(e.to_string())),
            Ok(bytes) => {
                self.cache.put(&self.key, bytes.clone());
                let val = list_deserialize(&bytes).and_then(|l| {
                    resolve_index(self.index, l.len()).and_then(|i| l.get(i).cloned())
                });
                Ok(match val {
                    Some(v) => ValkeyValue::StringBuffer(v),
                    None => ValkeyValue::Null,
                })
            }
        };
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.tsc.reply(reply);
        }));
    }
}

// ── Command handler ───────────────────────────────────────────────────────────

/// `FLASH.LINDEX key index`
///
/// Returns the element at index (0-based, negative from tail), or nil.
pub fn flash_lindex_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() != 3 {
        return Err(ValkeyError::WrongArity);
    }

    let key = &args[1];
    let index: i64 = std::str::from_utf8(args[2].as_slice())
        .map_err(|_| ValkeyError::Str("ERR value is not an integer or out of range"))?
        .parse()
        .map_err(|_| ValkeyError::Str("ERR value is not an integer or out of range"))?;

    let cache = CACHE
        .get()
        .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;

    if let Some(cached_bytes) = cache.get(key.as_slice()) {
        let val = list_deserialize(&cached_bytes)
            .and_then(|l| resolve_index(index, l.len()).and_then(|i| l.get(i).cloned()));
        return Ok(match val {
            Some(v) => ValkeyValue::StringBuffer(v),
            None => ValkeyValue::Null,
        });
    }

    let cold_info: Option<(u64, u32)>;
    {
        let key_handle = ctx.open_key(key);
        let obj = match key_handle.get_value::<FlashListObject>(&FLASH_LIST_TYPE) {
            Err(_) => return Err(ValkeyError::WrongType),
            Ok(None) => return Ok(ValkeyValue::Null),
            Ok(Some(obj)) => obj,
        };

        match &obj.tier {
            Tier::Hot(items) => {
                let val = resolve_index(index, items.len()).and_then(|i| items.get(i).cloned());
                cache.put(key.as_slice(), list_serialize(items));
                return Ok(match val {
                    Some(v) => ValkeyValue::StringBuffer(v),
                    None => ValkeyValue::Null,
                });
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
        let handle = Box::new(LIndexCompletionHandle {
            tsc: valkey_module::ThreadSafeContext::with_blocked_client(bc),
            key: key_bytes,
            index,
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
        Ok(ValkeyValue::Null)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    #[test]
    fn lindex_positive() {
        let mut items = VecDeque::new();
        items.push_back(b"a".to_vec());
        items.push_back(b"b".to_vec());
        assert_eq!(
            resolve_index(1, items.len()).and_then(|i| items.get(i).cloned()),
            Some(b"b".to_vec())
        );
    }

    #[test]
    fn lindex_negative() {
        let mut items = VecDeque::new();
        items.push_back(b"a".to_vec());
        items.push_back(b"b".to_vec());
        assert_eq!(
            resolve_index(-1, items.len()).and_then(|i| items.get(i).cloned()),
            Some(b"b".to_vec())
        );
    }

    #[test]
    fn lindex_out_of_range() {
        let items: VecDeque<Vec<u8>> = VecDeque::new();
        assert_eq!(resolve_index(0, items.len()), None);
    }
}
