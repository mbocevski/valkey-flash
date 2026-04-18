use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

use crate::persistence::aux::LOADED_AUX_STATE;

/// `FLASH.AUX.INFO`
///
/// Returns a flat list of key-value strings describing the loaded aux state.
/// Returns an empty array if no aux state has been loaded (fresh module start).
/// Intended for integration tests and debugging only.
pub fn flash_aux_info_command(_ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() != 1 {
        return Err(ValkeyError::WrongArity);
    }

    let guard = LOADED_AUX_STATE
        .lock()
        .map_err(|_| ValkeyError::Str("ERR aux state mutex poisoned"))?;

    let state = match guard.as_ref() {
        None => return Ok(ValkeyValue::Array(vec![])),
        Some(s) => s,
    };

    let before = &state.before;
    let mut result = vec![
        ValkeyValue::SimpleStringStatic("before.entries"),
        ValkeyValue::Integer(before.entries.len() as i64),
        ValkeyValue::SimpleStringStatic("before.path"),
        ValkeyValue::BulkString(before.path.clone()),
        ValkeyValue::SimpleStringStatic("before.capacity_bytes"),
        ValkeyValue::Integer(before.capacity_bytes as i64),
        ValkeyValue::SimpleStringStatic("before.io_uring_entries"),
        ValkeyValue::Integer(before.io_uring_entries as i64),
        ValkeyValue::SimpleStringStatic("before.wal_cursor"),
        ValkeyValue::Integer(before.wal_cursor as i64),
        ValkeyValue::SimpleStringStatic("before.version"),
        ValkeyValue::Integer(before.version as i64),
    ];

    if let Some(after) = &state.after {
        result.push(ValkeyValue::SimpleStringStatic("after.saved_at_unix_ms"));
        result.push(ValkeyValue::Integer(after.saved_at_unix_ms as i64));
        result.push(ValkeyValue::SimpleStringStatic("after.version"));
        result.push(ValkeyValue::Integer(after.version as i64));
    }

    Ok(ValkeyValue::Array(result))
}
