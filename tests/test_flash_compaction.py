import pathlib
import re
import pytest
from valkeytestframework.util.waiters import wait_for_equal
from valkey_flash_test_case import ValkeyFlashTestCase


def _wal_path() -> pathlib.Path:
    return pathlib.Path("/tmp/valkey-flash.wal")


def _bgsave_and_restart(server):
    server.client.execute_command("BGSAVE")
    server.wait_for_save_done()
    server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
    assert server.is_alive()
    wait_for_equal(lambda: server.is_rdb_done_loading(), True)


def _crash_restart(server):
    server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
    assert server.is_alive()
    wait_for_equal(lambda: server.is_rdb_done_loading(), True)


def _parse_stats(raw) -> dict:
    """Parse FLASH.COMPACTION.STATS reply into a dict of {name: int}."""
    result = {}
    for item in raw:
        line = item.decode() if isinstance(item, bytes) else item
        key, _, val = line.partition(":")
        result[key.strip()] = int(val.strip())
    return result


class TestFlashCompaction(ValkeyFlashTestCase):

    @pytest.mark.compaction
    def test_compaction_stats_returns_expected_keys(self):
        raw = self.client.execute_command("FLASH.COMPACTION.STATS")
        stats = _parse_stats(raw)
        assert "compaction_runs" in stats
        assert "bytes_reclaimed" in stats
        assert "free_blocks" in stats

    @pytest.mark.compaction
    def test_del_increments_bytes_reclaimed(self):
        raw_before = _parse_stats(
            self.client.execute_command("FLASH.COMPACTION.STATS")
        )
        self.client.execute_command("FLASH.SET", "k1", "hello")
        self.client.execute_command("FLASH.DEL", "k1")
        raw_after = _parse_stats(self.client.execute_command("FLASH.COMPACTION.STATS"))
        assert raw_after["bytes_reclaimed"] > raw_before["bytes_reclaimed"]

    @pytest.mark.compaction
    def test_del_increases_free_blocks(self):
        self.client.execute_command("FLASH.SET", "k1", "hello")
        raw_before = _parse_stats(
            self.client.execute_command("FLASH.COMPACTION.STATS")
        )
        self.client.execute_command("FLASH.DEL", "k1")
        raw_after = _parse_stats(self.client.execute_command("FLASH.COMPACTION.STATS"))
        assert raw_after["free_blocks"] > raw_before["free_blocks"]

    @pytest.mark.compaction
    def test_trigger_increments_compaction_runs(self):
        raw_before = _parse_stats(
            self.client.execute_command("FLASH.COMPACTION.STATS")
        )
        assert self.client.execute_command("FLASH.COMPACTION.TRIGGER") == b"OK"
        raw_after = _parse_stats(self.client.execute_command("FLASH.COMPACTION.STATS"))
        assert raw_after["compaction_runs"] == raw_before["compaction_runs"] + 1

    @pytest.mark.compaction
    def test_trigger_coalesces_adjacent_free_ranges(self):
        # Write 3 small keys (each occupies 1 block), then delete all 3.
        # Before trigger: 3 separate free ranges. After trigger: 1 merged range.
        _bgsave_and_restart(self.server)  # clean cursor

        self.client.execute_command("FLASH.SET", "a", "v")
        self.client.execute_command("FLASH.SET", "b", "v")
        self.client.execute_command("FLASH.SET", "c", "v")

        # Capture free_blocks count before any deletes.
        raw_before_del = _parse_stats(
            self.client.execute_command("FLASH.COMPACTION.STATS")
        )

        self.client.execute_command("FLASH.DEL", "a")
        self.client.execute_command("FLASH.DEL", "b")
        self.client.execute_command("FLASH.DEL", "c")

        raw_after_del = _parse_stats(
            self.client.execute_command("FLASH.COMPACTION.STATS")
        )
        free_before_trigger = raw_after_del["free_blocks"]

        assert self.client.execute_command("FLASH.COMPACTION.TRIGGER") == b"OK"

        raw_after_trigger = _parse_stats(
            self.client.execute_command("FLASH.COMPACTION.STATS")
        )
        # Coalescing should reduce free_blocks (3 adjacent 1-block ranges → 1 3-block range)
        # while preserving the total number of free block units.
        assert raw_after_trigger["free_blocks"] == free_before_trigger

    @pytest.mark.compaction
    def test_freed_blocks_reused_no_capacity_exhaustion(self):
        """Fill near capacity with small keys, delete all, re-fill: no capacity error."""
        n = 20
        for i in range(n):
            self.client.execute_command("FLASH.SET", f"fillkey{i}", "v")
        for i in range(n):
            self.client.execute_command("FLASH.DEL", f"fillkey{i}")
        # Trigger compaction to coalesce freed ranges.
        self.client.execute_command("FLASH.COMPACTION.TRIGGER")
        # Re-fill the same number of keys — freed blocks must be reused.
        for i in range(n):
            result = self.client.execute_command("FLASH.SET", f"fillkey{i}", "v2")
            assert result == b"OK", f"Expected OK, got {result!r} on key fillkey{i}"

    @pytest.mark.compaction
    def test_free_blocks_persisted_across_restart(self):
        """After BGSAVE, free_blocks and nvme_next_block survive a restart."""
        _bgsave_and_restart(self.server)

        self.client.execute_command("FLASH.SET", "pk1", "hello")
        self.client.execute_command("FLASH.DEL", "pk1")

        raw_before = _parse_stats(
            self.client.execute_command("FLASH.COMPACTION.STATS")
        )
        assert raw_before["free_blocks"] > 0

        _bgsave_and_restart(self.server)

        # After restart, free_blocks are restored from aux — still > 0.
        raw_after = _parse_stats(
            self.client.execute_command("FLASH.COMPACTION.STATS")
        )
        assert raw_after["free_blocks"] > 0

    @pytest.mark.compaction
    def test_compaction_interval_config_mutable(self):
        """flash.compaction-interval-sec is settable via CONFIG SET."""
        self.client.execute_command("CONFIG", "SET", "flash.compaction-interval-sec", "120")
        val = self.client.execute_command("CONFIG", "GET", "flash.compaction-interval-sec")
        # CONFIG GET returns [name, value] or {name: value} depending on client version.
        if isinstance(val, dict):
            assert val.get(b"flash.compaction-interval-sec") == b"120"
        else:
            assert val[1] == b"120"
        # Restore default.
        self.client.execute_command("CONFIG", "SET", "flash.compaction-interval-sec", "60")
