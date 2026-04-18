use valkey_module::{valkey_module, Context, Status, ValkeyString};

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
    data_types: [],
    init: initialize,
    deinit: deinitialize,
    commands: [],
}
