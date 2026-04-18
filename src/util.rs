// ── Key hash ──────────────────────────────────────────────────────────────────

/// FNV-1a 64-bit hash of `key`. Deterministic across process restarts — safe
/// to store in WAL records and compare after a crash + recovery.
pub fn key_hash(key: &[u8]) -> u64 {
    let mut hash: u64 = 14_695_981_039_346_656_037;
    for &byte in key {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(1_099_511_628_211);
    }
    hash
}

/// CRC32C (Castagnoli) of `value`, widened to u64. Stored in WAL Put records
/// so recovery can detect corrupt NVMe data.
pub fn value_hash(value: &[u8]) -> u64 {
    crc32c::crc32c(value) as u64
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_hash_deterministic() {
        assert_eq!(key_hash(b"hello"), key_hash(b"hello"));
    }

    #[test]
    fn key_hash_differs_for_different_inputs() {
        assert_ne!(key_hash(b"a"), key_hash(b"b"));
    }

    #[test]
    fn key_hash_empty_does_not_panic() {
        let _ = key_hash(b"");
    }

    #[test]
    fn value_hash_deterministic() {
        assert_eq!(value_hash(b"v1"), value_hash(b"v1"));
    }

    #[test]
    fn value_hash_differs_for_different_inputs() {
        assert_ne!(value_hash(b"x"), value_hash(b"y"));
    }

    #[test]
    fn key_and_value_hash_differ() {
        // key_hash and value_hash use different algorithms — no accidental collision.
        assert_ne!(key_hash(b"same"), value_hash(b"same"));
    }
}
