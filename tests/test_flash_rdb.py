import os

from valkey_flash_test_case import ValkeyFlashTestCase
from valkeytestframework.util.waiters import wait_for_equal

# ── Helpers ───────────────────────────────────────────────────────────────────


def _binaries_dir():
    return (
        f"{os.path.dirname(os.path.realpath(__file__))}"
        f"/build/binaries/{os.environ['SERVER_VERSION']}"
    )


def _server_path():
    return os.path.join(_binaries_dir(), "valkey-server")


def _server_args():
    return {
        "enable-debug-command": "yes",
        "loadmodule": os.getenv("MODULE_PATH"),
    }


def _bgsave_and_restart(server):
    """Trigger BGSAVE, wait for it to finish, then restart with the RDB intact."""
    server.client.execute_command("BGSAVE")
    server.wait_for_save_done()
    server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
    assert server.is_alive()
    wait_for_equal(server.is_rdb_done_loading, True)


# ── RDB round-trip tests ──────────────────────────────────────────────────────


class TestFlashRdb(ValkeyFlashTestCase):
    def test_hot_string_survives_restart(self):
        self.client.execute_command("FLASH.SET", "rdbkey", "rdbval")
        _bgsave_and_restart(self.server)
        assert self.client.execute_command("FLASH.GET", "rdbkey") == b"rdbval"

    def test_binary_value_survives_restart(self):
        binary = bytes(range(256))
        self.client.execute_command("FLASH.SET", "binkey", binary)
        _bgsave_and_restart(self.server)
        assert self.client.execute_command("FLASH.GET", "binkey") == binary

    def test_empty_value_survives_restart(self):
        self.client.execute_command("FLASH.SET", "empty", "")
        _bgsave_and_restart(self.server)
        assert self.client.execute_command("FLASH.GET", "empty") == b""

    def test_multiple_keys_survive_restart(self):
        for i in range(10):
            self.client.execute_command("FLASH.SET", f"multi:{i}", f"val{i}")
        _bgsave_and_restart(self.server)
        for i in range(10):
            assert self.client.execute_command("FLASH.GET", f"multi:{i}") == f"val{i}".encode()

    def test_key_count_preserved_after_restart(self):
        for i in range(5):
            self.client.execute_command("FLASH.SET", f"cnt:{i}", "v")
        before = self.server.num_keys()
        _bgsave_and_restart(self.server)
        assert self.server.num_keys() == before

    def test_deleted_key_absent_after_restart(self):
        self.client.execute_command("FLASH.SET", "gone", "v")
        self.client.execute_command("FLASH.DEL", "gone")
        _bgsave_and_restart(self.server)
        assert self.client.execute_command("FLASH.GET", "gone") is None

    def test_overwritten_value_reflects_last_write(self):
        self.client.execute_command("FLASH.SET", "ow", "first")
        self.client.execute_command("FLASH.SET", "ow", "second")
        _bgsave_and_restart(self.server)
        assert self.client.execute_command("FLASH.GET", "ow") == b"second"

    def test_large_value_survives_restart(self):
        large = b"x" * (128 * 1024)
        self.client.execute_command("FLASH.SET", "large", large)
        _bgsave_and_restart(self.server)
        assert self.client.execute_command("FLASH.GET", "large") == large

    def test_rdb_loads_log_message(self):
        self.client.execute_command("FLASH.SET", "logtest", "v")
        _bgsave_and_restart(self.server)
        self.server.verify_string_in_logfile("Loading RDB produced by Valkey")
        self.server.verify_string_in_logfile("Done loading RDB, keys loaded: 1")

    def test_flash_and_native_keys_coexist_after_restart(self):
        self.client.execute_command("FLASH.SET", "flash:k", "fv")
        self.client.execute_command("SET", "native:k", "nv")
        _bgsave_and_restart(self.server)
        assert self.client.execute_command("FLASH.GET", "flash:k") == b"fv"
        assert self.client.execute_command("GET", "native:k") == b"nv"

    def test_ttl_set_key_survives_restart(self):
        # Key with a long TTL (10 minutes) should still be present after restart.
        self.client.execute_command("FLASH.SET", "ttlkey", "ttlval", "EX", "600")
        _bgsave_and_restart(self.server)
        result = self.client.execute_command("FLASH.GET", "ttlkey")
        assert result == b"ttlval"

    def test_flash_get_after_restart_re_promotes_to_cache(self):
        # After restart the RAM cache is empty. First GET reads from the keyspace
        # object (Tier::Hot restored from RDB), re-promotes to cache, and returns
        # the value. A second GET must also succeed (now served from cache).
        self.client.execute_command("FLASH.SET", "promote", "pval")
        _bgsave_and_restart(self.server)
        assert self.client.execute_command("FLASH.GET", "promote") == b"pval"
        assert self.client.execute_command("FLASH.GET", "promote") == b"pval"
