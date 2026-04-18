import os
import time
from valkeytestframework.util.waiters import wait_for_equal
from valkeytestframework.valkey_test_case import ValkeyAction
from valkey_flash_test_case import ValkeyFlashTestCase


# ── Helpers ───────────────────────────────────────────────────────────────────

def _enable_aof(client):
    client.config_set("appendonly", "yes")
    wait_for_equal(
        lambda: client.info("persistence")["aof_rewrite_in_progress"], 0, timeout=30
    )


def _bgrewriteaof_and_restart(server):
    server.client.bgrewriteaof()
    server.wait_for_action_done(ValkeyAction.AOF_REWRITE)
    server.args["appendonly"] = "yes"
    server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
    assert server.is_alive()


# ── AOF rewrite tests ─────────────────────────────────────────────────────────

class TestFlashAofRewrite(ValkeyFlashTestCase):

    def test_keys_survive_aof_rewrite_and_restart(self):
        _enable_aof(self.client)
        for i in range(3):
            self.client.execute_command("FLASH.SET", f"aof:{i}", f"val{i}")
        _bgrewriteaof_and_restart(self.server)
        for i in range(3):
            assert self.client.execute_command("FLASH.GET", f"aof:{i}") == f"val{i}".encode()

    def test_key_count_preserved_after_aof_restart(self):
        _enable_aof(self.client)
        for i in range(3):
            self.client.execute_command("FLASH.SET", f"cnt:{i}", "v")
        before = self.server.num_keys()
        _bgrewriteaof_and_restart(self.server)
        assert self.server.num_keys() == before

    def test_key_without_ttl_has_no_expiry_after_restart(self):
        _enable_aof(self.client)
        self.client.execute_command("FLASH.SET", "nottl", "noexp")
        _bgrewriteaof_and_restart(self.server)
        # TTL command returns -1 (no expiry) for a key that has no TTL set.
        ttl = self.client.execute_command("TTL", "nottl")
        assert ttl == -1

    def test_key_with_ttl_survives_with_expiry_intact(self):
        _enable_aof(self.client)
        # 10-minute TTL — should survive restart without expiring.
        self.client.execute_command("FLASH.SET", "withttl", "tv", "EX", "600")
        _bgrewriteaof_and_restart(self.server)
        result = self.client.execute_command("FLASH.GET", "withttl")
        assert result == b"tv"
        ttl = self.client.execute_command("TTL", "withttl")
        assert ttl > 0

    def test_binary_value_survives_aof_rewrite(self):
        _enable_aof(self.client)
        binary = bytes(range(256))
        self.client.execute_command("FLASH.SET", "binkey", binary)
        _bgrewriteaof_and_restart(self.server)
        assert self.client.execute_command("FLASH.GET", "binkey") == binary

    def test_empty_value_survives_aof_rewrite(self):
        _enable_aof(self.client)
        self.client.execute_command("FLASH.SET", "empty", "")
        _bgrewriteaof_and_restart(self.server)
        assert self.client.execute_command("FLASH.GET", "empty") == b""

    def test_three_keys_with_different_ttls(self):
        _enable_aof(self.client)
        self.client.execute_command("FLASH.SET", "t1", "v1", "EX", "600")
        self.client.execute_command("FLASH.SET", "t2", "v2", "EX", "1200")
        self.client.execute_command("FLASH.SET", "t3", "v3")
        _bgrewriteaof_and_restart(self.server)
        assert self.client.execute_command("FLASH.GET", "t1") == b"v1"
        assert self.client.execute_command("FLASH.GET", "t2") == b"v2"
        assert self.client.execute_command("FLASH.GET", "t3") == b"v3"
        assert self.client.execute_command("TTL", "t1") > 0
        assert self.client.execute_command("TTL", "t2") > 0
        assert self.client.execute_command("TTL", "t3") == -1

    def test_debug_reload_preserves_keys(self):
        _enable_aof(self.client)
        for i in range(3):
            self.client.execute_command("FLASH.SET", f"reload:{i}", f"rv{i}")
        self.client.bgrewriteaof()
        self.server.wait_for_action_done(ValkeyAction.AOF_REWRITE)
        self.client.execute_command("DEBUG", "RELOAD")
        for i in range(3):
            assert self.client.execute_command("FLASH.GET", f"reload:{i}") == f"rv{i}".encode()

    def test_overwritten_key_reflects_last_value(self):
        _enable_aof(self.client)
        self.client.execute_command("FLASH.SET", "ow", "first")
        self.client.execute_command("FLASH.SET", "ow", "second")
        _bgrewriteaof_and_restart(self.server)
        assert self.client.execute_command("FLASH.GET", "ow") == b"second"

    def test_deleted_key_absent_after_aof_restart(self):
        _enable_aof(self.client)
        self.client.execute_command("FLASH.SET", "gone", "v")
        self.client.execute_command("FLASH.DEL", "gone")
        _bgrewriteaof_and_restart(self.server)
        assert self.client.execute_command("FLASH.GET", "gone") is None
