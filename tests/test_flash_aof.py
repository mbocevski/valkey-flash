from valkey_flash_test_case import ValkeyFlashTestCase
from valkeytestframework.util.waiters import wait_for_equal
from valkeytestframework.valkey_test_case import ValkeyAction

# ── Helpers ───────────────────────────────────────────────────────────────────


def _enable_aof(client):
    client.config_set("appendonly", "yes")
    wait_for_equal(lambda: client.info("persistence")["aof_rewrite_in_progress"], 0, timeout=30)


def _bgrewriteaof_and_restart(server):
    server.client.bgrewriteaof()
    server.wait_for_action_done(ValkeyAction.AOF_REWRITE)
    server.args["appendonly"] = "yes"
    server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
    assert server.is_alive()
    wait_for_equal(server.is_rdb_done_loading, True)


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


# ── Cold-tier AOF rewrite round-trip ──────────────────────────────────────────
#
# Before the pread_at_offset + aof_rewrite fix, Cold-tier keys were silently
# dropped from the rewritten AOF — the `free` + `aof_rewrite` callbacks would
# short-circuit on Tier::Cold with a warning and emit nothing. Any demoted key
# was permanently lost on AOF-only restart. These tests cover the four types.


class TestFlashAofRewriteColdTier(ValkeyFlashTestCase):
    def test_cold_string_survives_aof_rewrite(self):
        _enable_aof(self.client)
        self.client.execute_command("FLASH.SET", "cold_str", "cold_value")
        self.client.execute_command("FLASH.DEBUG.DEMOTE", "cold_str")
        _bgrewriteaof_and_restart(self.server)
        assert self.client.execute_command("FLASH.GET", "cold_str") == b"cold_value"

    def test_cold_string_with_ttl_survives_aof_rewrite(self):
        _enable_aof(self.client)
        self.client.execute_command("FLASH.SET", "cold_str_ttl", "cold_tv", "EX", "600")
        self.client.execute_command("FLASH.DEBUG.DEMOTE", "cold_str_ttl")
        _bgrewriteaof_and_restart(self.server)
        assert self.client.execute_command("FLASH.GET", "cold_str_ttl") == b"cold_tv"
        assert self.client.execute_command("TTL", "cold_str_ttl") > 0

    def test_cold_hash_survives_aof_rewrite(self):
        _enable_aof(self.client)
        self.client.execute_command("FLASH.HSET", "cold_h", "f1", "v1", "f2", "v2", "f3", "v3")
        self.client.execute_command("FLASH.DEBUG.DEMOTE", "cold_h")
        _bgrewriteaof_and_restart(self.server)
        assert self.client.execute_command("FLASH.HGET", "cold_h", "f1") == b"v1"
        assert self.client.execute_command("FLASH.HGET", "cold_h", "f2") == b"v2"
        assert self.client.execute_command("FLASH.HGET", "cold_h", "f3") == b"v3"

    def test_cold_list_survives_aof_rewrite(self):
        _enable_aof(self.client)
        self.client.execute_command("FLASH.RPUSH", "cold_l", "a", "b", "c")
        self.client.execute_command("FLASH.DEBUG.DEMOTE", "cold_l")
        _bgrewriteaof_and_restart(self.server)
        assert self.client.execute_command("FLASH.LRANGE", "cold_l", "0", "-1") == [
            b"a",
            b"b",
            b"c",
        ]

    def test_cold_zset_survives_aof_rewrite(self):
        _enable_aof(self.client)
        self.client.execute_command("FLASH.ZADD", "cold_z", "1.0", "a", "2.0", "b", "3.0", "c")
        self.client.execute_command("FLASH.DEBUG.DEMOTE", "cold_z")
        _bgrewriteaof_and_restart(self.server)
        assert self.client.execute_command("FLASH.ZCARD", "cold_z") == 3
        assert float(self.client.execute_command("FLASH.ZSCORE", "cold_z", "a")) == 1.0
        assert float(self.client.execute_command("FLASH.ZSCORE", "cold_z", "b")) == 2.0
        assert float(self.client.execute_command("FLASH.ZSCORE", "cold_z", "c")) == 3.0

    def test_mixed_hot_and_cold_survive_aof_rewrite(self):
        # Half hot, half cold — both must survive in the same rewrite.
        _enable_aof(self.client)
        self.client.execute_command("FLASH.SET", "mixed_hot", "hot_value")
        self.client.execute_command("FLASH.SET", "mixed_cold", "cold_value")
        self.client.execute_command("FLASH.DEBUG.DEMOTE", "mixed_cold")
        _bgrewriteaof_and_restart(self.server)
        assert self.client.execute_command("FLASH.GET", "mixed_hot") == b"hot_value"
        assert self.client.execute_command("FLASH.GET", "mixed_cold") == b"cold_value"
