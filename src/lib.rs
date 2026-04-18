use valkey_module::{valkey_module, Context, Status, ValkeyString};

pub mod async_io;
pub mod storage;
pub mod types;

use crate::types::hash::FLASH_HASH_TYPE;
use crate::types::string::FLASH_STRING_TYPE;

pub const MODULE_NAME: &str = "flash";
pub const MODULE_VERSION: i32 = 1;

fn initialize(_ctx: &Context, _args: &[ValkeyString]) -> Status {
    Status::Ok
}

fn deinitialize(_ctx: &Context) -> Status {
    Status::Ok
}

valkey_module! {
    name: MODULE_NAME,
    version: MODULE_VERSION,
    allocator: (valkey_module::alloc::ValkeyAlloc, valkey_module::alloc::ValkeyAlloc),
    data_types: [
        FLASH_STRING_TYPE,
        FLASH_HASH_TYPE,
    ],
    init: initialize,
    deinit: deinitialize,
    commands: [],
}
