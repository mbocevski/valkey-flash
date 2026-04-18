import os
from valkeytestframework.util.waiters import wait_for_equal
from valkeytestframework.valkey_test_case import ValkeyAction
from valkey_flash_test_case import ValkeyFlashTestCase


# ── Helpers ───────────────────────────────────────────────────────────────────

def _bgsave_and_restart(server):
    server.client.execute_command("BGSAVE")
    server.wait_for_save_done()
    server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
    assert server.is_alive()
    wait_for_equal(lambda: server.is_rdb_done_loading(), True)


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
    wait_for_equal(lambda: server.is_rdb_done_loading(), True)


def _hgetall_as_dict(client, key):
    flat = client.execute_command("FLASH.HGETALL", key)
    return dict(zip(flat[::2], flat[1::2]))


# ── RDB round-trip tests ──────────────────────────────────────────────────────

class TestFlashHashRdb(ValkeyFlashTestCase):

    def test_single_field_hash_survives_restart(self):
        self.client.execute_command("FLASH.HSET", "rh1", "name", "alice")
        _bgsave_and_restart(self.server)
        assert self.client.execute_command("FLASH.HGET", "rh1", "name") == b"alice"

    def test_multi_field_hash_survives_restart(self):
        self.client.execute_command("FLASH.HSET", "rh2", "a", "1", "b", "2", "c", "3")
        _bgsave_and_restart(self.server)
        d = _hgetall_as_dict(self.client, "rh2")
        assert d == {b"a": b"1", b"b": b"2", b"c": b"3"}

    def test_hash_ttl_survives_restart(self):
        self.client.execute_command("FLASH.HSET", "rh3", "f", "v")
        self.client.execute_command("EXPIRE", "rh3", 600)
        _bgsave_and_restart(self.server)
        assert self.client.execute_command("FLASH.HGET", "rh3", "f") == b"v"
        assert self.client.execute_command("TTL", "rh3") > 0

    def test_hash_without_ttl_has_no_expiry_after_restart(self):
        self.client.execute_command("FLASH.HSET", "rh4", "f", "v")
        _bgsave_and_restart(self.server)
        assert self.client.execute_command("TTL", "rh4") == -1

    def test_cold_hash_survives_restart(self):
        self.client.execute_command("FLASH.HSET", "rh5", "x", "cold_val")
        self.client.execute_command("FLASH.DEBUG.DEMOTE", "rh5")
        _bgsave_and_restart(self.server)
        assert self.client.execute_command("FLASH.HGET", "rh5", "x") == b"cold_val"

    def test_cold_hash_multi_field_survives_restart(self):
        self.client.execute_command("FLASH.HSET", "rh6", "k1", "v1", "k2", "v2")
        self.client.execute_command("FLASH.DEBUG.DEMOTE", "rh6")
        _bgsave_and_restart(self.server)
        d = _hgetall_as_dict(self.client, "rh6")
        assert d == {b"k1": b"v1", b"k2": b"v2"}

    def test_mixed_string_and_hash_keys_survive_restart(self):
        self.client.execute_command("FLASH.SET", "rstr", "sval")
        self.client.execute_command("FLASH.HSET", "rhash", "field", "hval")
        _bgsave_and_restart(self.server)
        assert self.client.execute_command("FLASH.GET", "rstr") == b"sval"
        assert self.client.execute_command("FLASH.HGET", "rhash", "field") == b"hval"

    def test_key_count_preserved_after_restart(self):
        for i in range(5):
            self.client.execute_command("FLASH.HSET", f"cnt:{i}", "f", "v")
        before = self.server.num_keys()
        _bgsave_and_restart(self.server)
        assert self.server.num_keys() == before

    def test_overwritten_field_reflects_last_write(self):
        self.client.execute_command("FLASH.HSET", "rh7", "f", "first")
        self.client.execute_command("FLASH.HSET", "rh7", "f", "second")
        _bgsave_and_restart(self.server)
        assert self.client.execute_command("FLASH.HGET", "rh7", "f") == b"second"

    def test_binary_field_and_value_survive_restart(self):
        field = bytes(range(16))
        value = bytes(range(255, 239, -1))
        self.client.execute_command("FLASH.HSET", "rh8", field, value)
        _bgsave_and_restart(self.server)
        assert self.client.execute_command("FLASH.HGET", "rh8", field) == value

    def test_hash_restored_to_hot_tier_after_restart(self):
        # After RDB load all keys come back as Tier::Hot. Reads should work
        # without NVMe I/O and re-populate the cache correctly.
        self.client.execute_command("FLASH.HSET", "rh9", "g", "hi")
        _bgsave_and_restart(self.server)
        assert self.client.execute_command("FLASH.HGET", "rh9", "g") == b"hi"
        assert self.client.execute_command("FLASH.HGET", "rh9", "g") == b"hi"

    def test_many_fields_hash_survives_restart(self):
        pairs = [item for i in range(50) for item in (f"f{i}", f"v{i}")]
        self.client.execute_command("FLASH.HSET", "rh10", *pairs)
        _bgsave_and_restart(self.server)
        for i in range(50):
            assert self.client.execute_command("FLASH.HGET", "rh10", f"f{i}") == f"v{i}".encode()


# ── AOF round-trip tests ──────────────────────────────────────────────────────

class TestFlashHashAof(ValkeyFlashTestCase):

    def test_hash_survives_aof_rewrite_and_restart(self):
        _enable_aof(self.client)
        self.client.execute_command("FLASH.HSET", "ah1", "color", "red", "size", "large")
        _bgrewriteaof_and_restart(self.server)
        d = _hgetall_as_dict(self.client, "ah1")
        assert d == {b"color": b"red", b"size": b"large"}

    def test_hash_ttl_survives_aof_rewrite(self):
        _enable_aof(self.client)
        self.client.execute_command("FLASH.HSET", "ah2", "f", "v")
        self.client.execute_command("EXPIRE", "ah2", 600)
        _bgrewriteaof_and_restart(self.server)
        assert self.client.execute_command("FLASH.HGET", "ah2", "f") == b"v"
        assert self.client.execute_command("TTL", "ah2") > 0

    def test_hash_without_ttl_no_expiry_after_aof_restart(self):
        _enable_aof(self.client)
        self.client.execute_command("FLASH.HSET", "ah3", "f", "v")
        _bgrewriteaof_and_restart(self.server)
        assert self.client.execute_command("TTL", "ah3") == -1

    def test_multiple_hash_keys_survive_aof(self):
        _enable_aof(self.client)
        for i in range(4):
            self.client.execute_command("FLASH.HSET", f"ah_multi:{i}", "n", str(i))
        _bgrewriteaof_and_restart(self.server)
        for i in range(4):
            assert self.client.execute_command("FLASH.HGET", f"ah_multi:{i}", "n") == str(i).encode()

    def test_overwritten_hash_field_reflects_last_write_after_aof(self):
        _enable_aof(self.client)
        self.client.execute_command("FLASH.HSET", "ah4", "f", "old")
        self.client.execute_command("FLASH.HSET", "ah4", "f", "new")
        _bgrewriteaof_and_restart(self.server)
        assert self.client.execute_command("FLASH.HGET", "ah4", "f") == b"new"

    def test_deleted_hash_absent_after_aof_restart(self):
        _enable_aof(self.client)
        self.client.execute_command("FLASH.HSET", "ah5", "f", "v")
        self.client.execute_command("DEL", "ah5")
        _bgrewriteaof_and_restart(self.server)
        assert self.client.execute_command("FLASH.HGETALL", "ah5") == []

    def test_mixed_string_and_hash_survive_aof(self):
        _enable_aof(self.client)
        self.client.execute_command("FLASH.SET", "astr", "sv")
        self.client.execute_command("FLASH.HSET", "ahash", "fld", "hv")
        _bgrewriteaof_and_restart(self.server)
        assert self.client.execute_command("FLASH.GET", "astr") == b"sv"
        assert self.client.execute_command("FLASH.HGET", "ahash", "fld") == b"hv"

    def test_many_fields_aof_rewrite(self):
        _enable_aof(self.client)
        pairs = [item for i in range(100) for item in (f"field{i}", f"value{i}")]
        self.client.execute_command("FLASH.HSET", "ah6", *pairs)
        _bgrewriteaof_and_restart(self.server)
        for i in range(100):
            assert self.client.execute_command("FLASH.HGET", "ah6", f"field{i}") == f"value{i}".encode()

    def test_debug_reload_preserves_hash_fields(self):
        _enable_aof(self.client)
        self.client.execute_command("FLASH.HSET", "ah7", "a", "1", "b", "2")
        self.client.bgrewriteaof()
        self.server.wait_for_action_done(ValkeyAction.AOF_REWRITE)
        self.client.execute_command("DEBUG", "RELOAD")
        assert self.client.execute_command("FLASH.HGET", "ah7", "a") == b"1"
        assert self.client.execute_command("FLASH.HGET", "ah7", "b") == b"2"

    def test_key_count_preserved_after_aof_restart(self):
        _enable_aof(self.client)
        for i in range(3):
            self.client.execute_command("FLASH.HSET", f"acnt:{i}", "f", "v")
        before = self.server.num_keys()
        _bgrewriteaof_and_restart(self.server)
        assert self.server.num_keys() == before
