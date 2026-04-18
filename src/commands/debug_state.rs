use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

use crate::recovery::ModuleState;
use crate::MODULE_STATE;

/// FLASH.DEBUG.STATE — return the current module lifecycle state.
///
/// Returns one of: `recovering` | `ready` | `error`.
/// Intended for integration tests and operational diagnostics only.
pub fn flash_debug_state_command(_ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() != 1 {
        return Err(ValkeyError::WrongArity);
    }
    let state = MODULE_STATE
        .lock()
        .map_err(|_| ValkeyError::Str("MODULE_STATE lock poisoned"))?;
    let s = match *state {
        ModuleState::Recovering => "recovering",
        ModuleState::Ready => "ready",
        ModuleState::Error => "error",
    };
    Ok(ValkeyValue::SimpleString(s.to_string()))
}
