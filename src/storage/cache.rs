use std::collections::VecDeque;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Mutex,
};

use quick_cache::{sync::Cache, Weighter};

// ── BytesWeighter ─────────────────────────────────────────────────────────────

#[derive(Clone)]
struct BytesWeighter;

impl Weighter<Vec<u8>, Vec<u8>> for BytesWeighter {
    fn weight(&self, key: &Vec<u8>, val: &Vec<u8>) -> u64 {
        (key.len() + val.len()) as u64
    }
}

// ── CacheMetrics ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct CacheMetrics {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
}

// ── FlashCache ────────────────────────────────────────────────────────────────

// Bound on the demotion-hint queue; prevents unbounded growth.
const MAX_CANDIDATES: usize = 65_536;

pub struct FlashCache {
    inner: Cache<Vec<u8>, Vec<u8>, BytesWeighter>,
    capacity_bytes: u64,
    approx_bytes: AtomicU64,
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
    // Insertion-ordered queue used by evict_candidate() to find demotion targets.
    // Keys are added on first put(); auto-evicted keys are skipped on pop.
    candidates: Mutex<VecDeque<Vec<u8>>>,
}

impl FlashCache {
    pub fn new(capacity_bytes: u64) -> Self {
        // Assume average 256-byte entry for pre-sizing the item count estimate.
        let estimated_items = ((capacity_bytes / 256).max(16)) as usize;
        FlashCache {
            inner: Cache::with_weighter(estimated_items, capacity_bytes, BytesWeighter),
            capacity_bytes,
            approx_bytes: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            candidates: Mutex::new(VecDeque::new()),
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        match self.inner.get(key) {
            Some(val) => {
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(val)
            }
            None => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    pub fn put(&self, key: &[u8], value: Vec<u8>) {
        let new_weight = (key.len() + value.len()) as u64;
        // Use peek (no recency update) to check for an existing entry.
        let old_weight = self
            .inner
            .peek(key)
            .map(|v| (key.len() + v.len()) as u64)
            .unwrap_or(0);
        if old_weight == 0 {
            // New key: enqueue as a potential demotion candidate.
            if let Ok(mut q) = self.candidates.lock() {
                if q.len() < MAX_CANDIDATES {
                    q.push_back(key.to_vec());
                }
            }
        }
        self.inner.insert(key.to_vec(), value);
        self.approx_bytes.fetch_add(new_weight, Ordering::Relaxed);
        if old_weight > 0 {
            self.approx_bytes
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                    Some(cur.saturating_sub(old_weight))
                })
                .ok();
        }
    }

    /// Remove `key`. Returns `true` if the key was present.
    pub fn delete(&self, key: &[u8]) -> bool {
        // remove() returns the (Key, Val) pair so we can compute the freed weight.
        match self.inner.remove(key) {
            Some((k, v)) => {
                let weight = (k.len() + v.len()) as u64;
                self.approx_bytes
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                        Some(cur.saturating_sub(weight))
                    })
                    .ok();
                self.evictions.fetch_add(1, Ordering::Relaxed);
                true
            }
            None => false,
        }
    }

    /// Update `key`'s recency without returning its value.
    pub fn touch(&self, key: &[u8]) {
        // inner.get() updates S3-FIFO recency bits.
        let _ = self.inner.get(key);
    }

    /// Return the oldest-inserted key that is still present in the cache.
    /// The caller is responsible for writing the value to the NVMe backend and
    /// then calling `delete` to free the RAM slot.
    /// Returns `None` when no candidates remain.
    pub fn evict_candidate(&self) -> Option<Vec<u8>> {
        let mut q = self.candidates.lock().ok()?;
        while let Some(key) = q.pop_front() {
            // Use peek so the existence check doesn't update recency.
            if self.inner.peek(key.as_slice()).is_some() {
                return Some(key);
            }
            // Already auto-evicted or explicitly deleted — skip.
        }
        None
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        // peek doesn't update recency; correct for a pure existence check.
        self.inner.peek(key).is_some()
    }

    /// Approximate total byte footprint of all current entries (key + value).
    /// Slightly overestimates when the cache auto-evicts entries under capacity
    /// pressure (auto-evictions are not tracked; only explicit `delete` calls are).
    pub fn approx_bytes(&self) -> u64 {
        self.approx_bytes.load(Ordering::Relaxed)
    }

    /// Configured capacity in bytes (set at construction).
    pub fn capacity_bytes(&self) -> u64 {
        self.capacity_bytes
    }

    pub fn metrics(&self) -> CacheMetrics {
        CacheMetrics {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn cache() -> FlashCache {
        FlashCache::new(1 << 20) // 1 MiB — plenty of room for unit tests
    }

    #[test]
    fn get_missing_returns_none() {
        assert!(cache().get(b"absent").is_none());
    }

    #[test]
    fn get_hit_returns_value() {
        let c = cache();
        c.put(b"k", b"v".to_vec());
        assert_eq!(c.get(b"k"), Some(b"v".to_vec()));
    }

    #[test]
    fn put_then_get_roundtrip() {
        let c = cache();
        c.put(b"hello", b"world".to_vec());
        assert_eq!(c.get(b"hello"), Some(b"world".to_vec()));
    }

    #[test]
    fn delete_present_key() {
        let c = cache();
        c.put(b"k", b"v".to_vec());
        assert!(c.delete(b"k"));
        assert!(c.get(b"k").is_none());
    }

    #[test]
    fn delete_missing_returns_false() {
        assert!(!cache().delete(b"nope"));
    }

    #[test]
    fn eviction_under_capacity_pressure() {
        // 100-byte cache; each entry is 5 + 15 = 20 bytes → holds at most 5.
        let c = FlashCache::new(100);
        for i in 0..10u8 {
            c.put(&[i; 5], vec![i; 15]);
        }
        let present = (0..10u8)
            .filter(|&i| c.get(&[i; 5] as &[u8]).is_some())
            .count();
        assert!(
            present <= 5,
            "expected ≤5 entries after eviction pressure, got {present}"
        );
    }

    #[test]
    fn approx_bytes_tracks_put_delete() {
        let c = cache();
        assert_eq!(c.approx_bytes(), 0);
        c.put(b"key", b"value".to_vec()); // 3 + 5 = 8 bytes
        assert_eq!(c.approx_bytes(), 8);
        assert!(c.delete(b"key"));
        assert_eq!(c.approx_bytes(), 0);
    }

    #[test]
    fn metrics_counters_tick_correctly() {
        let c = cache();
        c.put(b"k", b"v".to_vec());
        let _ = c.get(b"k"); // hit
        let _ = c.get(b"no"); // miss
        assert!(c.delete(b"k")); // explicit eviction
        let m = c.metrics();
        assert_eq!(m.hits, 1);
        assert_eq!(m.misses, 1);
        assert_eq!(m.evictions, 1);
    }

    #[test]
    fn contains_present_and_absent() {
        let c = cache();
        c.put(b"x", b"y".to_vec());
        assert!(c.contains(b"x"));
        assert!(!c.contains(b"z"));
    }

    #[test]
    fn touch_keeps_key_present() {
        let c = cache();
        c.put(b"k", b"v".to_vec());
        c.touch(b"k");
        assert_eq!(c.get(b"k"), Some(b"v".to_vec()));
    }

    #[test]
    fn evict_candidate_returns_inserted_key() {
        let c = cache();
        c.put(b"a", b"1".to_vec());
        c.put(b"b", b"2".to_vec());
        let candidate = c.evict_candidate().expect("should have a candidate");
        assert!(candidate == b"a" || candidate == b"b");
    }

    #[test]
    fn evict_candidate_skips_deleted_keys() {
        let c = cache();
        c.put(b"gone", b"val".to_vec());
        c.put(b"kept", b"val".to_vec());
        c.delete(b"gone");
        // "gone" is at the front of the queue but was deleted; should be skipped.
        let candidate = c.evict_candidate();
        assert_eq!(candidate, Some(b"kept".to_vec()));
    }
}
