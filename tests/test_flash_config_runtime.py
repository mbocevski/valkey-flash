import pytest
from valkey_flash_test_case import ValkeyFlashTestCase
from valkeytestframework.conftest import resource_port_tracker


class TestFlashConfigRuntime(ValkeyFlashTestCase):

    def _config_get(self, name):
        result = self.client.execute_command("CONFIG", "GET", name)
        # result is [name, value] flat list
        if isinstance(result, dict):
            return result.get(name)
        if result and len(result) >= 2:
            return result[1]
        return None

    def _config_set(self, name, value):
        self.client.execute_command("CONFIG", "SET", name, str(value))

    # ── flash.cache-size-bytes ────────────────────────────────────────────────

    def test_cache_size_config_get_returns_default(self):
        val = self._config_get("flash.cache-size-bytes")
        assert val is not None
        assert int(val) > 0

    def test_cache_size_config_set_updates_value(self):
        new_size = 1 << 21  # 2 MiB
        self._config_set("flash.cache-size-bytes", new_size)
        val = self._config_get("flash.cache-size-bytes")
        assert int(val) == new_size

    def test_cache_size_decrease_still_serves_reads(self):
        # Write a key, shrink cache to 1 MiB, verify the key is still readable
        # (may be served from NVMe on cold miss, but no error).
        self.client.execute_command("FLASH.SET", "cfg_k", "cfg_v")
        self._config_set("flash.cache-size-bytes", 1 << 20)
        result = self.client.execute_command("FLASH.GET", "cfg_k")
        assert result == b"cfg_v"

    def test_cache_size_increase_config_get_reflects_change(self):
        self._config_set("flash.cache-size-bytes", 1 << 20)
        self._config_set("flash.cache-size-bytes", 1 << 29)
        val = self._config_get("flash.cache-size-bytes")
        assert int(val) == 1 << 29

    # ── flash.sync ───────────────────────────────────────────────────────────

    def test_sync_config_get_returns_default(self):
        val = self._config_get("flash.sync")
        assert val in (b"everysec", "everysec")

    def test_sync_config_set_always(self):
        self._config_set("flash.sync", "always")
        val = self._config_get("flash.sync")
        assert val in (b"always", "always")
        # Writes should still succeed.
        assert self.client.execute_command("FLASH.SET", "sync_k", "v") == b"OK"
        # Restore.
        self._config_set("flash.sync", "everysec")

    def test_sync_config_set_no(self):
        self._config_set("flash.sync", "no")
        val = self._config_get("flash.sync")
        assert val in (b"no", "no")
        assert self.client.execute_command("FLASH.SET", "sync_no_k", "v") == b"OK"
        self._config_set("flash.sync", "everysec")

    def test_sync_config_set_everysec(self):
        self._config_set("flash.sync", "always")
        self._config_set("flash.sync", "everysec")
        val = self._config_get("flash.sync")
        assert val in (b"everysec", "everysec")

    # ── flash.compaction-interval-sec ────────────────────────────────────────

    def test_compaction_interval_config_get_returns_default(self):
        val = self._config_get("flash.compaction-interval-sec")
        assert int(val) == 60

    def test_compaction_interval_config_set_updates_value(self):
        self._config_set("flash.compaction-interval-sec", 10)
        val = self._config_get("flash.compaction-interval-sec")
        assert int(val) == 10
        self._config_set("flash.compaction-interval-sec", 60)

    def test_compaction_interval_min_boundary(self):
        self._config_set("flash.compaction-interval-sec", 1)
        val = self._config_get("flash.compaction-interval-sec")
        assert int(val) == 1
        self._config_set("flash.compaction-interval-sec", 60)

    # ── Immutable knobs reject CONFIG SET ─────────────────────────────────────

    def _assert_immutable(self, name, value):
        with pytest.raises(Exception) as exc_info:
            self._config_set(name, value)
        err = str(exc_info.value).lower()
        assert "immutable" in err or "can't set" in err or "not supported" in err or "error" in err

    def test_path_is_immutable(self):
        self._assert_immutable("flash.path", "/new/path")

    def test_capacity_bytes_is_immutable(self):
        self._assert_immutable("flash.capacity-bytes", 1 << 31)

    def test_io_threads_is_immutable(self):
        self._assert_immutable("flash.io-threads", 4)

    def test_io_uring_entries_is_immutable(self):
        self._assert_immutable("flash.io-uring-entries", 512)
