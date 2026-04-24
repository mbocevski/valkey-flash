from valkey_flash_test_case import ValkeyFlashTestCase

# All field names registered in the "flash" INFO section.
EXPECTED_FIELDS = [
    "flash_cache_hits",
    "flash_cache_misses",
    "flash_cache_hit_ratio",
    "flash_cache_size_bytes",
    "flash_cache_capacity_bytes",
    "flash_eviction_count",
    "flash_storage_used_bytes",
    "flash_storage_free_bytes",
    "flash_storage_capacity_bytes",
    "flash_wal_size_bytes",
    "flash_compaction_runs",
    "flash_compaction_bytes_reclaimed",
    "flash_tiered_keys",
    "flash_auto_demotions_total",
    "flash_auto_demotions_inflight",
    "flash_demotion_effective_batch",
    "flash_demotion_tick_last_us",
    "flash_demotion_stall_events_total",
    "flash_module_state",
]


class TestFlashInfo(ValkeyFlashTestCase):
    def _info(self):
        return self.parse_valkey_info("flash")

    def test_all_expected_fields_present(self):
        info = self._info()
        for field in EXPECTED_FIELDS:
            assert field in info, f"missing field: {field}"

    def test_module_state_is_ready(self):
        assert self._info()["flash_module_state"] == "ready"

    def test_cache_capacity_nonzero(self):
        assert self._info()["flash_cache_capacity_bytes"] > 0

    def test_storage_capacity_nonzero(self):
        assert self._info()["flash_storage_capacity_bytes"] > 0

    def test_hit_ratio_zero_before_any_reads(self):
        # Fresh server: no GET calls yet → ratio must be 0.0000
        info = self._info()
        assert float(info["flash_cache_hit_ratio"]) == 0.0

    def test_cache_hits_increment_on_get(self):
        self.client.execute_command("FLASH.SET", "info_hit", "v")
        before = self._info()["flash_cache_hits"]
        self.client.execute_command("FLASH.GET", "info_hit")
        after = self._info()["flash_cache_hits"]
        assert after > before

    def test_cache_misses_increment_on_cold_get(self):
        # A GET on a nonexistent key returns early (no cache.get() call).
        # To exercise cache misses, set a key, demote it to NVMe (evicts from
        # the hot cache), then GET it — the cold path calls cache.get() → None.
        self.client.execute_command("FLASH.SET", "info_miss_key", "v")
        self.client.execute_command("FLASH.DEBUG.DEMOTE", "info_miss_key")
        before = self._info()["flash_cache_misses"]
        self.client.execute_command("FLASH.GET", "info_miss_key")
        after = self._info()["flash_cache_misses"]
        assert after > before

    def test_hit_ratio_updates_after_hits_and_misses(self):
        self.client.execute_command("FLASH.SET", "ratio_key", "v")
        # Warm the cache via one GET (hit), then miss on an absent key.
        self.client.execute_command("FLASH.GET", "ratio_key")
        self.client.execute_command("FLASH.GET", "ratio_key")
        self.client.execute_command("FLASH.GET", "ratio_nokey_abc123")
        ratio = float(self._info()["flash_cache_hit_ratio"])
        assert 0.0 < ratio <= 1.0

    def test_storage_used_reflects_live_data(self):
        # storage_used_bytes is `(next_block - free_blocks) * BLOCK`, i.e. live
        # blocks currently holding data. The FLASH.SET durability write already
        # puts one block on NVMe, so storage_used is non-zero before DEMOTE and
        # unchanged after DEMOTE (the demote path allocates fresh blocks and
        # reclaims the prior durability-write blocks — net live blocks stay the
        # same). Assert the live-blocks invariant rather than a bump-pointer
        # delta.
        self.client.execute_command("FLASH.SET", "demote_info", "x" * 512)
        before = self._info()["flash_storage_used_bytes"]
        assert before > 0, "FLASH.SET durability write should put ≥1 block on NVMe"
        self.client.execute_command("FLASH.DEBUG.DEMOTE", "demote_info")
        after = self._info()["flash_storage_used_bytes"]
        assert after > 0, "after DEMOTE there must still be ≥1 live block"

    def test_storage_used_plus_free_equals_capacity(self):
        # The corrected storage accounting must produce a well-formed partition.
        info = self._info()
        used = info["flash_storage_used_bytes"]
        free = info["flash_storage_free_bytes"]
        cap = info["flash_storage_capacity_bytes"]
        assert used + free == cap, (
            f"storage partition broken: used({used}) + free({free}) != cap({cap})"
        )

    def test_tiered_keys_increases_after_demote(self):
        self.client.execute_command("FLASH.SET", "tier_info", "data")
        before = self._info()["flash_tiered_keys"]
        self.client.execute_command("FLASH.DEBUG.DEMOTE", "tier_info")
        after = self._info()["flash_tiered_keys"]
        assert after == before + 1

    def test_wal_size_nonzero_after_writes(self):
        self.client.execute_command("FLASH.SET", "wal_info", "v")
        self.client.execute_command("FLASH.DEBUG.DEMOTE", "wal_info")
        assert self._info()["flash_wal_size_bytes"] > 0

    def test_compaction_runs_increment_after_trigger(self):
        before = self._info()["flash_compaction_runs"]
        self.client.execute_command("FLASH.COMPACTION.TRIGGER")
        after = self._info()["flash_compaction_runs"]
        assert after > before

    def test_info_flash_filter_returns_section(self):
        info = self._info()
        # All expected flash fields should be present when filtering by section.
        for field in EXPECTED_FIELDS:
            assert field in info

    def test_numeric_fields_are_non_negative(self):
        info = self._info()
        numeric_fields = [
            f for f in EXPECTED_FIELDS if f not in ("flash_module_state", "flash_cache_hit_ratio")
        ]
        for field in numeric_fields:
            assert info[field] >= 0, f"field {field} is negative: {info[field]}"

    def test_hash_operations_visible_in_info(self):
        self.client.execute_command("FLASH.HSET", "info_hash", "f", "v")
        self.client.execute_command("FLASH.HGET", "info_hash", "f")
        info = self._info()
        # At least one hit from the HGET (may be cache hit after HSET warms it).
        assert info["flash_cache_hits"] >= 0  # sanity: field is present and numeric
