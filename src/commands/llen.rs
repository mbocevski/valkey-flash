use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

use crate::types::list::{list_deserialize, list_serialize, FlashListObject, FLASH_LIST_TYPE};
use crate::types::Tier;
use crate::CACHE;

// ── LLenCompletionHandle ──────────────────────────────────────────────────────

#[cfg(not(test))]
struct LLenCompletionHandle {
    tsc: valkey_module::ThreadSafeContext<valkey_module::BlockedClient<()>>,
    key: Vec<u8>,
    cache: &'static crate::storage::cache::FlashCache,
}

#[cfg(not(test))]
impl crate::async_io::CompletionHandle for LLenCompletionHandle {
    fn complete(self: Box<Self>, result: crate::storage::backend::StorageResult<Vec<u8>>) {
        let reply = match result {
            Err(e) => Err(valkey_module::ValkeyError::String(e.to_string())),
            Ok(bytes) => {
                self.cache.put(&self.key, bytes.clone());
                let len = list_deserialize(&bytes).map_or(0, |l| l.len() as i64);
                Ok(ValkeyValue::Integer(len))
            }
        };
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.tsc.reply(reply);
        }));
    }
}

// ── Command handler ───────────────────────────────────────────────────────────

/// `FLASH.LLEN key`
///
/// Returns the length of the list, or 0 if the key does not exist.
pub fn flash_llen_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() != 2 {
        return Err(ValkeyError::WrongArity);
    }

    let key = &args[1];
    let cache = CACHE
        .get()
        .ok_or(ValkeyError::Str("ERR flash module not initialized"))?;

    if let Some(cached_bytes) = cache.get(key.as_slice()) {
        let len = list_deserialize(&cached_bytes).map_or(0, |l| l.len() as i64);
        return Ok(ValkeyValue::Integer(len));
    }

    let cold_info: Option<(u64, u32)>;
    {
        let key_handle = ctx.open_key(key);
        let obj = match key_handle.get_value::<FlashListObject>(&FLASH_LIST_TYPE) {
            Err(_) => return Err(ValkeyError::WrongType),
            Ok(None) => return Ok(ValkeyValue::Integer(0)),
            Ok(Some(obj)) => obj,
        };

        match &obj.tier {
            Tier::Hot(items) => {
                let len = items.len() as i64;
                cache.put(key.as_slice(), list_serialize(items));
                return Ok(ValkeyValue::Integer(len));
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
        let handle = Box::new(LLenCompletionHandle {
            tsc: valkey_module::ThreadSafeContext::with_blocked_client(bc),
            key: key_bytes,
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
        Ok(ValkeyValue::Integer(0))
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    #[test]
    fn llen_empty_list() {
        let items: VecDeque<Vec<u8>> = VecDeque::new();
        assert_eq!(items.len() as i64, 0);
    }

    #[test]
    fn llen_nonempty_list() {
        let mut items = VecDeque::new();
        items.push_back(b"a".to_vec());
        items.push_back(b"b".to_vec());
        assert_eq!(items.len() as i64, 2);
    }
}
