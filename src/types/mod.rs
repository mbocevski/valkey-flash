pub mod hash;
pub mod string;

// ── Tier ──────────────────────────────────────────────────────────────────────

/// Identifies whether the object's payload lives in RAM (hot) or on NVMe (cold).
///
/// Command handlers create `Tier::Hot(payload)` when writing a new key.
/// The tiering logic demotes to `Tier::Cold { size_hint }` when evicting to NVMe.
#[derive(Debug, PartialEq)]
pub enum Tier<T> {
    /// Payload is in RAM. The `T` holds the actual value bytes.
    Hot(T),
    /// Payload is on NVMe only. `size_hint` records the serialised byte count
    /// for capacity accounting without a round-trip read.
    Cold { size_hint: u32 },
}
