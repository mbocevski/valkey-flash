#![no_main]
use libfuzzer_sys::fuzz_target;
use valkey_flash::types::hash::parse_rdb_hash_payload;
use valkey_flash::types::string::parse_rdb_payload;

// Exercises the RDB parsing invariants (version check, shape_tag check, TTL
// sentinel handling, value-size bounds, hash field-count cap) through the same
// pure-Rust helpers that `rdb_load` delegates to in production.
//
// Both `parse_rdb_payload` and `parse_rdb_hash_payload` call `build_rdb_string`
// and `build_rdb_hash` respectively — the canonical validation functions also
// called by the FFI-backed `rdb_load` shims. Fuzzing these paths therefore
// exercises the identical code that runs on every real RDB load.
fuzz_target!(|data: &[u8]| {
    let _ = parse_rdb_payload(data);
    let _ = parse_rdb_hash_payload(data);
});
