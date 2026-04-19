import pytest
from valkey import ResponseError
from valkey_flash_test_case import ValkeyFlashTestCase


def _parse_stats(raw) -> dict:
    """Parse FLASH.COMPACTION.STATS reply into a dict of {name: int}."""
    result = {}
    for item in raw:
        line = item.decode() if isinstance(item, bytes) else item
        key, _, val = line.partition(":")
        result[key.strip()] = int(val.strip())
    return result


class TestFlashTtlReclaim(ValkeyFlashTestCase):
    @pytest.mark.ttl_reclaim
    def test_debug_demote_returns_ok(self):
        """FLASH.DEBUG.DEMOTE on a hot key returns OK."""
        self.client.execute_command("FLASH.SET", "dmk1", "hello")
        result = self.client.execute_command("FLASH.DEBUG.DEMOTE", "dmk1")
        assert result == b"OK"

    @pytest.mark.ttl_reclaim
    def test_debug_demote_nonexistent_key_errors(self):
        """FLASH.DEBUG.DEMOTE on a missing key returns an error."""
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.DEBUG.DEMOTE", "no_such_key")

    @pytest.mark.ttl_reclaim
    def test_debug_demote_already_cold_returns_already_cold(self):
        """Demoting a cold key again is a no-op."""
        self.client.execute_command("FLASH.SET", "dmk2", "world")
        self.client.execute_command("FLASH.DEBUG.DEMOTE", "dmk2")
        result = self.client.execute_command("FLASH.DEBUG.DEMOTE", "dmk2")
        assert result == b"ALREADY_COLD"

    @pytest.mark.ttl_reclaim
    def test_cold_get_returns_correct_value(self):
        """FLASH.GET on a demoted key returns the original value."""
        self.client.execute_command("FLASH.SET", "cold_read_k", "payload123")
        self.client.execute_command("FLASH.DEBUG.DEMOTE", "cold_read_k")
        val = self.client.execute_command("FLASH.GET", "cold_read_k")
        assert val == b"payload123"

    @pytest.mark.ttl_reclaim
    def test_cold_key_expiry_reclaims_blocks(self):
        """After TTL expires a cold key, bytes_reclaimed increases."""
        stats_before = _parse_stats(self.client.execute_command("FLASH.COMPACTION.STATS"))

        self.client.execute_command("FLASH.SET", "ttl_cold_k", "data")
        self.client.execute_command("FLASH.DEBUG.DEMOTE", "ttl_cold_k")

        # Set a short TTL so expiry fires quickly.
        self.client.execute_command("PEXPIRE", "ttl_cold_k", "200")

        # Wait for Valkey to expire the key.
        self.wait_for_key_expiry(self.client, "ttl_cold_k", timeout_s=3)

        # Trigger lazy expiry scan to ensure free() has been called.
        self.client.execute_command("DEBUG", "SLEEP", "0")

        stats_after = _parse_stats(self.client.execute_command("FLASH.COMPACTION.STATS"))
        assert stats_after["bytes_reclaimed"] > stats_before["bytes_reclaimed"]

    @pytest.mark.ttl_reclaim
    def test_cold_key_expiry_increases_free_blocks(self):
        """After TTL expires a cold key, the free-list gains blocks."""
        self.client.execute_command("FLASH.SET", "ttl_fb_k", "data")
        self.client.execute_command("FLASH.DEBUG.DEMOTE", "ttl_fb_k")

        stats_before = _parse_stats(self.client.execute_command("FLASH.COMPACTION.STATS"))

        self.client.execute_command("PEXPIRE", "ttl_fb_k", "200")
        self.wait_for_key_expiry(self.client, "ttl_fb_k", timeout_s=3)
        self.client.execute_command("DEBUG", "SLEEP", "0")

        stats_after = _parse_stats(self.client.execute_command("FLASH.COMPACTION.STATS"))
        assert stats_after["free_blocks"] > stats_before["free_blocks"]

    @pytest.mark.ttl_reclaim
    def test_expired_blocks_reusable_by_new_key(self):
        """Blocks freed on TTL expiry can be reused by a subsequent FLASH.SET."""
        n = 10
        for i in range(n):
            self.client.execute_command("FLASH.SET", f"reuse_cold_{i}", "v")
            self.client.execute_command("FLASH.DEBUG.DEMOTE", f"reuse_cold_{i}")
            self.client.execute_command("PEXPIRE", f"reuse_cold_{i}", "200")

        # Wait for all keys to expire.
        for i in range(n):
            self.wait_for_key_expiry(self.client, f"reuse_cold_{i}", timeout_s=3)
        self.client.execute_command("DEBUG", "SLEEP", "0")

        # Trigger compaction to coalesce freed ranges.
        self.client.execute_command("FLASH.COMPACTION.TRIGGER")

        # Re-fill: freed blocks should be reused, not exhausting capacity.
        for i in range(n):
            result = self.client.execute_command("FLASH.SET", f"reuse_new_{i}", "v2")
            assert result == b"OK", f"Expected OK on key reuse_new_{i}, got {result!r}"

    @pytest.mark.ttl_reclaim
    def test_demote_does_not_corrupt_adjacent_keys(self):
        """Demoting one key does not corrupt reads for adjacent keys."""
        self.client.execute_command("FLASH.SET", "adj_a", "aaa")
        self.client.execute_command("FLASH.SET", "adj_b", "bbb")
        self.client.execute_command("FLASH.SET", "adj_c", "ccc")

        self.client.execute_command("FLASH.DEBUG.DEMOTE", "adj_b")

        assert self.client.execute_command("FLASH.GET", "adj_a") == b"aaa"
        assert self.client.execute_command("FLASH.GET", "adj_b") == b"bbb"
        assert self.client.execute_command("FLASH.GET", "adj_c") == b"ccc"
