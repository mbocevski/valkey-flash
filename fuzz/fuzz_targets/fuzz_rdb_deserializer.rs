#![no_main]
use libfuzzer_sys::fuzz_target;
use valkey_flash::types::string::fuzz_decode_rdb;

// Exercises the RDB parsing invariants (version check, shape_tag check, TTL
// sentinel handling, value-size bounds) through a pure-Rust path that mirrors
// the logic in `rdb_load`. Note: `rdb_load` itself uses `RedisModule_LoadUnsigned`
// FFI and cannot be called without a live Valkey module context.
fuzz_target!(|data: &[u8]| {
    let _ = fuzz_decode_rdb(data);
});
