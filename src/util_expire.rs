//! Helper for reading the Valkey-native key-level expiry as absolute-ms.
//!
//! The valkey-module 0.1.11 Rust crate wraps `SetExpire` and `RemoveExpire`
//! but not `GetExpire`. Commands that write via `ValkeyModule_ModuleTypeSetValue`
//! (i.e. every FLASH.* mutator that replaces the whole object) lose the key's
//! expiry as a side-effect because `VM_DeleteKey` is called internally before
//! `setKey(...)` — there is no SETKEY_KEEPTTL flag available.
//!
//! To preserve a native `PEXPIRE` / `EXPIRE` set outside the module, read
//! `RedisModule_GetExpire` before the write and call `set_expire` after.

use std::time::Duration;

use valkey_module::{Context, ValkeyString, raw};

use crate::commands::list_common::current_time_ms;

/// Read the current absolute-ms expiry for `key` via `RedisModule_GetExpire`.
///
/// Returns `None` if the key has no TTL (GetExpire returns `-1`) or if the
/// GetExpire FFI symbol is unavailable.
///
/// `VM_GetExpire` itself returns the **remaining** milliseconds, not an
/// absolute Unix timestamp. We convert to absolute here so the rest of the
/// module can use one convention (`ttl_ms: Option<i64>` as absolute unix ms,
/// matching `obj.ttl_ms` on every FLASH.* typed object).
pub fn read_native_expire_ms(ctx: &Context, key: &ValkeyString) -> Option<i64> {
    // SAFETY: raw::open_key accepts the context and key pointer we already own
    // from the caller. KeyMode::READ is the documented read-only mode. We
    // close the key with CloseKey below so the handle doesn't leak.
    let remaining_ms = unsafe {
        let key_inner = raw::open_key(ctx.ctx, key.inner, raw::KeyMode::READ);
        if key_inner.is_null() {
            return None;
        }

        #[allow(static_mut_refs)]
        let ms = raw::RedisModule_GetExpire.map(|f| f(key_inner));

        // CloseKey is the documented cleanup call for a key opened via open_key.
        #[allow(static_mut_refs)]
        if let Some(close) = raw::RedisModule_CloseKey {
            close(key_inner);
        }

        ms.and_then(|v| if v < 0 { None } else { Some(v) })?
    };
    Some(current_time_ms() + remaining_ms)
}

/// Prefer a caller-tracked TTL, fall back to the native key expiry.
///
/// Call this before `set_value` so the returned value can be passed to
/// `set_expire` after the write, restoring the TTL that
/// `VM_ModuleTypeSetValue` clears.
pub fn preserve_ttl(ctx: &Context, key: &ValkeyString, tracked_ttl: Option<i64>) -> Option<i64> {
    tracked_ttl.or_else(|| read_native_expire_ms(ctx, key))
}

/// Convert an absolute-ms TTL to the remaining-ms `Duration` that
/// `ModuleKey::set_expire` expects. Returns `None` if the expiry has already
/// passed (do not re-apply).
pub fn remaining_ttl_duration(abs_ms: i64) -> Option<Duration> {
    let remaining = abs_ms - current_time_ms();
    if remaining <= 0 {
        None
    } else {
        Some(Duration::from_millis(remaining as u64))
    }
}
