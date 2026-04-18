use std::time::{Duration, Instant};

/// Cumulative bandwidth throttle for migration pre-warm.
///
/// After each `account(bytes)` call the throttle sleeps the deficit between
/// "how long this many bytes should have taken at `limit_bps`" and "how long
/// it actually took". This ensures even the first sub-millisecond window is
/// paced correctly.
///
/// `bandwidth_mbps = 0` means unlimited — `account` returns immediately.
pub struct BandwidthThrottle {
    /// Bytes per second limit. 0 = no cap.
    pub(crate) limit_bps: u64,
    window_start: Instant,
    pub(crate) bytes_this_window: u64,
}

impl BandwidthThrottle {
    /// Create a throttle at `bandwidth_mbps` Mbps. Pass 0 for unlimited.
    pub fn new(bandwidth_mbps: u64) -> Self {
        Self {
            limit_bps: bandwidth_mbps * 125_000, // Mbps → bytes/s: ×1 000 000 ÷ 8
            window_start: Instant::now(),
            bytes_this_window: 0,
        }
    }

    /// Update the bandwidth limit live (e.g. from CONFIG SET mid-migration).
    /// Resets the accounting window to avoid carrying over stale debt.
    pub fn set_bandwidth_mbps(&mut self, bandwidth_mbps: u64) {
        self.limit_bps = bandwidth_mbps * 125_000;
        self.window_start = Instant::now();
        self.bytes_this_window = 0;
    }

    /// `true` when no bandwidth cap is configured (`bandwidth_mbps == 0`).
    pub fn is_unlimited(&self) -> bool {
        self.limit_bps == 0
    }

    /// Record `bytes` transferred and sleep if the cumulative rate exceeds the
    /// configured limit. Returns immediately when unlimited.
    ///
    /// Single-call sleep is capped at 100 ms to keep individual key promotions
    /// responsive; the next call compensates for any remaining deficit.
    pub fn account(&mut self, bytes: u64) {
        if self.limit_bps == 0 {
            return;
        }
        self.bytes_this_window += bytes;

        // How long should it take to transfer bytes_this_window at limit_bps?
        let expected_us = self.bytes_this_window * 1_000_000 / self.limit_bps;
        let actual_us = self.window_start.elapsed().as_micros() as u64;

        if expected_us > actual_us {
            let wait_us = (expected_us - actual_us).min(100_000); // cap at 100 ms
            std::thread::sleep(Duration::from_micros(wait_us));
        }

        // Reset window after ~1 s to prevent u64 overflow and stale credit build-up.
        if self.window_start.elapsed().as_secs() >= 1 {
            self.window_start = Instant::now();
            self.bytes_this_window = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_unlimited_has_zero_bps() {
        let t = BandwidthThrottle::new(0);
        assert_eq!(t.limit_bps, 0);
        assert!(t.is_unlimited());
    }

    #[test]
    fn new_computes_correct_bps() {
        // 100 Mbps = 100 × 125 000 = 12 500 000 B/s
        let t = BandwidthThrottle::new(100);
        assert_eq!(t.limit_bps, 12_500_000);
    }

    #[test]
    fn set_bandwidth_updates_limit_and_resets_window() {
        let mut t = BandwidthThrottle::new(100);
        t.bytes_this_window = 9_999_999;
        t.set_bandwidth_mbps(200);
        assert_eq!(t.limit_bps, 200 * 125_000);
        assert_eq!(t.bytes_this_window, 0);
    }

    #[test]
    fn set_bandwidth_to_zero_disables_throttle() {
        let mut t = BandwidthThrottle::new(100);
        t.set_bandwidth_mbps(0);
        assert!(t.is_unlimited());
    }

    #[test]
    fn unlimited_does_not_sleep() {
        let mut t = BandwidthThrottle::new(0);
        let start = Instant::now();
        t.account(1024 * 1024 * 1024); // 1 GiB — should be instant
        assert!(
            start.elapsed().as_millis() < 50,
            "unlimited throttle slept unexpectedly"
        );
    }

    #[test]
    fn within_limit_does_not_sleep() {
        // At 10 Gbps a 1 MiB chunk should be allowed instantly (expected < 1 ms).
        let mut t = BandwidthThrottle::new(10_000);
        let start = Instant::now();
        t.account(1024 * 1024);
        assert!(
            start.elapsed().as_millis() < 50,
            "throttle slept for data within budget"
        );
    }

    #[test]
    fn sustained_rate_within_tolerance() {
        // 800 Mbps (100 MB/s): send 10 MiB in 128 KiB chunks, expect ~105 ms ±50%.
        let mut t = BandwidthThrottle::new(800);
        let data: u64 = 10 * 1024 * 1024;
        let chunk: u64 = 128 * 1024;
        let start = Instant::now();
        let mut rem = data;
        while rem > 0 {
            let n = rem.min(chunk);
            t.account(n);
            rem -= n;
        }
        let elapsed_ms = start.elapsed().as_millis() as u64;
        let expected_ms = data * 1000 / (800 * 125_000); // ~105 ms
        assert!(
            elapsed_ms >= expected_ms / 2 && elapsed_ms <= expected_ms * 3 / 2,
            "elapsed={elapsed_ms}ms expected={expected_ms}ms (±50%)"
        );
    }

    /// Full-scale timing test: 32 MiB at 10 Mbps ≈ 26.8 s (±20%).
    /// Run with: `cargo test -- --ignored throttle_32mib_at_10mbps`
    #[test]
    #[ignore]
    fn throttle_32mib_at_10mbps() {
        let mut t = BandwidthThrottle::new(10);
        let data: u64 = 32 * 1024 * 1024;
        let chunk: u64 = 128 * 1024;
        let start = Instant::now();
        let mut rem = data;
        while rem > 0 {
            let n = rem.min(chunk);
            t.account(n);
            rem -= n;
        }
        let elapsed = start.elapsed().as_secs_f64();
        let expected = data as f64 / (10_000_000.0 / 8.0); // ~26.8 s
        assert!(
            (elapsed - expected).abs() / expected <= 0.20,
            "elapsed={elapsed:.1}s expected={expected:.1}s (±20%)"
        );
    }
}
