#![no_main]
use libfuzzer_sys::fuzz_target;
use valkey_flash::storage::cache::FlashCache;

fuzz_target!(|data: &[u8]| {
    let cache = FlashCache::new(64 * 1024); // 64 KiB — stays fast under fuzzer
    let mut pos = 0;

    while pos < data.len() {
        // byte 0: op (mod 4): 0=put, 1=get, 2=delete, 3=contains
        let op = data[pos] % 4;
        pos += 1;

        // byte 1: key_len 1–32
        if pos >= data.len() {
            break;
        }
        let key_len = (data[pos] as usize % 32) + 1;
        pos += 1;

        if pos + key_len > data.len() {
            break;
        }
        let key = &data[pos..pos + key_len];
        pos += key_len;

        match op {
            0 => {
                // put: byte N is val_len 0–63, followed by val_len bytes
                if pos >= data.len() {
                    break;
                }
                let val_len = data[pos] as usize % 64;
                pos += 1;
                let val_end = (pos + val_len).min(data.len());
                let value = data[pos..val_end].to_vec();
                pos = val_end;
                cache.put(key, value);
            }
            1 => {
                let _ = cache.get(key);
            }
            2 => {
                let _ = cache.delete(key);
            }
            3 => {
                let _ = cache.contains(key);
            }
            _ => unreachable!(),
        }

        // Invariant checks — must never panic or overflow.
        let _ = cache.approx_bytes();
    }

    let _ = cache.evict_candidate();
    let _ = cache.metrics();
});
