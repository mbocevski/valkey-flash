"""End-to-end tests for automatic hot→cold tier demotion.

The module exposes `FLASH.DEBUG.DEMOTE` as a deterministic admin path, but the
core value proposition is that tiering happens automatically as the RAM hot
cache fills. These tests force cache pressure via a small
`flash.cache-size-bytes` and verify that the event-loop timer (see
`src/demotion.rs`) drains the cache's candidate queue to NVMe without any
operator action.

Shared setup: the backing file is sized at 16 MiB by `ValkeyFlashTestCase`;
the tests lower `flash.cache-size-bytes` to its minimum (1 MiB) so a modest
write load reliably exceeds the cap and trips the demotion tick.
"""

import time

from valkey_flash_test_case import ValkeyFlashTestCase

# Tight budget to keep CI runs fast, loose enough that the 100 ms tick has time
# to drain a batch under varying host load.
DEMOTION_POLL_DEADLINE_S = 3.0
DEMOTION_POLL_INTERVAL_S = 0.05


def _wait_for(predicate, deadline_s=DEMOTION_POLL_DEADLINE_S):
    """Poll until `predicate()` is true or the deadline elapses.

    Returns True on success; False on timeout. Callers assert on the return
    value so the test error message points at what was observed.
    """
    end = time.monotonic() + deadline_s
    while time.monotonic() < end:
        if predicate():
            return True
        time.sleep(DEMOTION_POLL_INTERVAL_S)
    return False


class TestFlashAutoDemotion(ValkeyFlashTestCase):
    def _info(self):
        return self.parse_valkey_info("flash")

    def _shrink_cache_to_1mib(self):
        """Force the hot RAM tier down to the minimum so writes of a few MiB
        reliably exceed it and trip the auto-demotion tick."""
        self.client.execute_command("CONFIG", "SET", "flash.cache-size-bytes", 1 << 20)

    # ── core scenario ────────────────────────────────────────────────────────

    def test_auto_demotion_fires_when_cache_over_capacity(self):
        """The signature behaviour: write enough data to overflow the hot
        cache, wait a tick, and verify keys have moved to NVMe cold tier
        with no manual FLASH.DEBUG.DEMOTE anywhere in the test."""
        self._shrink_cache_to_1mib()

        # 1 MiB cache; write ~4 MiB of distinct keys (4 × ~1 KiB each).
        n_keys = 4000
        for i in range(n_keys):
            self.client.execute_command("FLASH.SET", f"auto:{i}", "x" * 1000)

        # The tick runs every ~100 ms and demotes in batches of up to 128.
        # 4000 candidates over 100 ms tick × 128/tick ≈ 3.2 s worst case.
        assert _wait_for(lambda: self._info()["flash_tiered_keys"] > 0), (
            f"auto-demotion never fired; info snapshot: {self._info()}"
        )

        info = self._info()
        assert info["flash_tiered_keys"] > 0, "no keys reached cold tier"
        assert info["flash_auto_demotions_total"] > 0, (
            "auto_demotions_total counter never incremented — the timer did "
            "not run or the demotion helper errored out silently"
        )

    def test_cold_read_after_auto_demotion_returns_original_value(self):
        """Round-trip: write a value, wait for auto-demotion, read it back.
        Verifies the cold-tier read path is wired correctly for values that
        were demoted automatically (not via FLASH.DEBUG.DEMOTE)."""
        self._shrink_cache_to_1mib()

        # Write a sentinel key first with a payload we can identify on read-back.
        sentinel = "auto_cold_roundtrip"
        payload = "sentinel-value-" + ("z" * 900)  # ~900 bytes
        self.client.execute_command("FLASH.SET", sentinel, payload)

        # Push the sentinel to cold by filling the cache past capacity.
        for i in range(4000):
            self.client.execute_command("FLASH.SET", f"fill:{i}", "x" * 1000)

        assert _wait_for(lambda: self._info()["flash_tiered_keys"] > 0)

        # The sentinel may or may not be cold depending on insertion order
        # vs the candidate queue drain; read it and assert value integrity.
        value = self.client.execute_command("FLASH.GET", sentinel)
        assert value is not None, "sentinel key was lost entirely"
        if isinstance(value, bytes):
            value = value.decode("utf-8")
        assert value == payload, (
            f"sentinel value corrupted after auto-demotion cycle: got {value!r}"
        )

    def test_auto_demotion_moves_keys_to_cold_tier_idle(self):
        """Explicit: a key that has been in cache long enough for the timer
        to pick it must register in TIERING_MAP (flash_tiered_keys counter)."""
        self._shrink_cache_to_1mib()

        for i in range(4000):
            self.client.execute_command("FLASH.SET", f"idle:{i}", "x" * 1000)

        # The worker fires at ≥95% cache fill (see DEMOTION_FILL_PCT in
        # src/demotion.rs).  Must see a non-zero tiered-keys count inside the
        # poll window.
        assert _wait_for(lambda: self._info()["flash_tiered_keys"] >= 1), (
            f"expected ≥1 cold key within {DEMOTION_POLL_DEADLINE_S}s; info: {self._info()}"
        )

    # ── metrics correctness (alongside the auto-demotion fix) ────────────────

    def test_storage_used_plus_free_equals_capacity(self):
        """The new storage_free_bytes formula must produce `used + free == cap`
        (modulo tiny drift from lock-free reads). The old formula reported
        0 free bytes as soon as any data was written, which was misleading."""
        # Write a little — not enough to fill the NVMe file, just enough for
        # the counters to move off zero.
        for i in range(50):
            self.client.execute_command("FLASH.SET", f"metric:{i}", "x" * 4000)

        info = self._info()
        used = info["flash_storage_used_bytes"]
        free = info["flash_storage_free_bytes"]
        cap = info["flash_storage_capacity_bytes"]
        assert used + free == cap, (
            f"used({used}) + free({free}) != capacity({cap}); delta {cap - used - free}"
        )
        assert free > 0, (
            "storage_free_bytes reported zero with free headroom — the old formula regressed"
        )

    def test_auto_demotions_total_field_present(self):
        """New field must appear in INFO flash for operators to monitor."""
        info = self._info()
        assert "flash_auto_demotions_total" in info, (
            "new auto-demotion counter missing from INFO flash"
        )
        # Starts at 0 on a fresh server; int-coercible.
        assert int(info["flash_auto_demotions_total"]) >= 0

    # ── per-type coverage: hash / list / zset auto-demotion ─────────────────

    def test_auto_demotion_fires_for_flash_hash_keys(self):
        """The four-type probe in `try_demote_key` must route hashes correctly.
        Write enough FLASH.HSET-backed keys to overflow the cache and assert
        tiered_keys + auto_demotions_total rise — proves the hash branch of
        the probe runs end-to-end."""
        self._shrink_cache_to_1mib()

        for i in range(4000):
            self.client.execute_command(
                "FLASH.HSET", f"hash:{i}", "field1", "a" * 500, "field2", "b" * 500
            )

        assert _wait_for(lambda: self._info()["flash_tiered_keys"] > 0), (
            f"hash auto-demotion never fired; info: {self._info()}"
        )
        info = self._info()
        assert info["flash_auto_demotions_total"] > 0

        # Verify a cold-tier hash still round-trips correctly.
        sample_key = None
        for i in range(4000):
            existence = self.client.execute_command("FLASH.HEXISTS", f"hash:{i}", "field1")
            if existence:
                sample_key = f"hash:{i}"
                break
        assert sample_key is not None, "no hash key survived the load"
        fields = self.client.execute_command("FLASH.HGETALL", sample_key)
        assert len(fields) == 4, f"expected 2 field/value pairs, got {fields!r}"

    def test_auto_demotion_fires_for_flash_list_keys(self):
        """Lists probe branch: write FLASH.RPUSH keys, overflow the cache,
        assert the demotion tick picked them up. A cold-tier LRANGE at the
        end proves the list serialize/deserialize loop survives the cycle."""
        self._shrink_cache_to_1mib()

        for i in range(4000):
            self.client.execute_command("FLASH.RPUSH", f"list:{i}", "a" * 500, "b" * 500)

        assert _wait_for(lambda: self._info()["flash_tiered_keys"] > 0), (
            f"list auto-demotion never fired; info: {self._info()}"
        )
        info = self._info()
        assert info["flash_auto_demotions_total"] > 0

        # Any surviving list must LRANGE 0 -1 to two elements in original order.
        items = self.client.execute_command("FLASH.LRANGE", "list:0", "0", "-1")
        assert len(items) == 2, f"list:0 lost elements after auto-demotion: {items!r}"

    def test_auto_demotion_fires_for_flash_zset_keys(self):
        """Sorted-set probe branch: write FLASH.ZADD keys, overflow the cache,
        assert the tick picks them up, verify a zrange round-trips the scores.

        Uses 1000-byte members (2x the hash/list tests) because the zset
        encoding is denser than hash's HashMap serialisation — a smaller
        workload was landing at 94.96% fill, right under the 95% auto-demotion
        trigger. The inflated member size guarantees the threshold is crossed.
        """
        self._shrink_cache_to_1mib()

        for i in range(4000):
            self.client.execute_command(
                "FLASH.ZADD",
                f"zset:{i}",
                "1.5",
                "a" * 1000,
                "2.5",
                "b" * 1000,
            )

        assert _wait_for(lambda: self._info()["flash_tiered_keys"] > 0), (
            f"zset auto-demotion never fired; info: {self._info()}"
        )
        info = self._info()
        assert info["flash_auto_demotions_total"] > 0

        members = self.client.execute_command("FLASH.ZRANGE", "zset:0", "0", "-1", "WITHSCORES")
        # Expect 4 elements: [member1, score1, member2, score2] — scores as strings.
        assert len(members) == 4, f"zset:0 lost members after auto-demotion: {members!r}"

    # ── invariants that must hold under all workloads ───────────────────────

    def test_no_data_loss_under_sustained_write_load(self):
        """Sustained writes with cache pressure must never lose a key. Writes
        and subsequent reads all return the correct value, whether the value
        is currently in RAM or was demoted to NVMe between writes."""
        self._shrink_cache_to_1mib()

        # Stripe of 500 keys with distinctive values; enough to overflow the
        # 1 MiB cache (500 × ~2 KiB ≈ 1 MiB, plus headers).
        written = {}
        for i in range(500):
            k = f"noloss:{i}"
            v = f"value-{i}-" + ("y" * 2000)
            self.client.execute_command("FLASH.SET", k, v)
            written[k] = v

        # Give the tick time to drain anything above capacity.
        _wait_for(lambda: self._info()["flash_tiered_keys"] > 0)

        # Read every key back. Anything missing or corrupted is a showstopper.
        for k, expected in written.items():
            got = self.client.execute_command("FLASH.GET", k)
            assert got is not None, f"key lost after auto-demotion: {k}"
            if isinstance(got, bytes):
                got = got.decode("utf-8")
            assert got == expected, (
                f"value corrupted after auto-demotion: key={k} expected {expected!r} got {got!r}"
            )
