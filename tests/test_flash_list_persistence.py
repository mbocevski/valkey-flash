"""Integration tests for FlashList RDB and AOF persistence."""

from valkey_flash_test_case import ValkeyFlashTestCase
from valkeytestframework.util.waiters import wait_for_equal
from valkeytestframework.valkey_test_case import ValkeyAction

# ── Helpers ───────────────────────────────────────────────────────────────────


def _bgsave_and_restart(server):
    server.client.execute_command("BGSAVE")
    server.wait_for_save_done()
    server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
    assert server.is_alive()
    wait_for_equal(server.is_rdb_done_loading, True)


def _bgrewriteaof_and_restart(server):
    server.client.bgrewriteaof()
    server.wait_for_action_done(ValkeyAction.AOF_REWRITE)
    server.args["appendonly"] = "yes"
    server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
    assert server.is_alive()
    wait_for_equal(server.is_rdb_done_loading, True)


def _enable_aof(client):
    client.config_set("appendonly", "yes")
    wait_for_equal(lambda: client.info("persistence")["aof_rewrite_in_progress"], 0, timeout=30)


# ── RDB round-trip tests ──────────────────────────────────────────────────────


class TestFlashListRdb(ValkeyFlashTestCase):
    def test_single_element_list_survives_restart(self):
        self.client.execute_command("FLASH.RPUSH", "rl1", "alpha")
        _bgsave_and_restart(self.server)
        result = self.client.execute_command("FLASH.LRANGE", "rl1", "0", "-1")
        assert result == [b"alpha"]

    def test_multi_element_list_preserves_order_after_restart(self):
        self.client.execute_command("FLASH.RPUSH", "rl2", "a", "b", "c", "d")
        _bgsave_and_restart(self.server)
        result = self.client.execute_command("FLASH.LRANGE", "rl2", "0", "-1")
        assert result == [b"a", b"b", b"c", b"d"]

    def test_list_ttl_survives_restart(self):
        self.client.execute_command("FLASH.RPUSH", "rl3", "x")
        self.client.execute_command("PEXPIRE", "rl3", 600_000)
        _bgsave_and_restart(self.server)
        assert self.client.execute_command("FLASH.LRANGE", "rl3", "0", "-1") == [b"x"]
        assert self.client.execute_command("TTL", "rl3") > 0

    def test_list_without_ttl_has_no_expiry_after_restart(self):
        self.client.execute_command("FLASH.RPUSH", "rl4", "y")
        _bgsave_and_restart(self.server)
        assert self.client.execute_command("TTL", "rl4") == -1

    def test_binary_elements_survive_restart(self):
        elem = bytes(range(256))
        self.client.execute_command("FLASH.RPUSH", "rl5", elem)
        _bgsave_and_restart(self.server)
        result = self.client.execute_command("FLASH.LRANGE", "rl5", "0", "-1")
        assert result == [elem]

    def test_cold_list_survives_restart(self):
        self.client.execute_command("FLASH.RPUSH", "rl6", "cold1", "cold2")
        self.client.execute_command("FLASH.DEBUG.DEMOTE", "rl6")
        _bgsave_and_restart(self.server)
        result = self.client.execute_command("FLASH.LRANGE", "rl6", "0", "-1")
        assert result == [b"cold1", b"cold2"]

    def test_list_length_preserved_after_restart(self):
        elems = [str(i) for i in range(50)]
        self.client.execute_command("FLASH.RPUSH", "rl7", *elems)
        _bgsave_and_restart(self.server)
        assert self.client.execute_command("FLASH.LLEN", "rl7") == 50

    def test_list_restored_to_hot_tier_after_restart(self):
        self.client.execute_command("FLASH.RPUSH", "rl8", "elem")
        _bgsave_and_restart(self.server)
        assert self.client.execute_command("FLASH.LPOP", "rl8") == b"elem"

    def test_many_elements_survive_restart(self):
        # 200 elements — spans more than one AOF chunk boundary (128).
        elems = [str(i) for i in range(200)]
        self.client.execute_command("FLASH.RPUSH", "rl9", *elems)
        _bgsave_and_restart(self.server)
        result = self.client.execute_command("FLASH.LRANGE", "rl9", "0", "-1")
        assert result == [e.encode() for e in elems]

    def test_key_count_preserved_after_restart(self):
        for i in range(5):
            self.client.execute_command("FLASH.RPUSH", f"rl_cnt:{i}", "v")
        before = self.server.num_keys()
        _bgsave_and_restart(self.server)
        assert self.server.num_keys() == before

    def test_mixed_list_and_string_keys_survive_restart(self):
        self.client.execute_command("FLASH.SET", "rlstr", "strval")
        self.client.execute_command("FLASH.RPUSH", "rllist", "listval")
        _bgsave_and_restart(self.server)
        assert self.client.execute_command("FLASH.GET", "rlstr") == b"strval"
        assert self.client.execute_command("FLASH.LPOP", "rllist") == b"listval"


# ── AOF round-trip tests ──────────────────────────────────────────────────────


class TestFlashListAof(ValkeyFlashTestCase):
    def test_single_element_list_survives_aof(self):
        _enable_aof(self.client)
        self.client.execute_command("FLASH.RPUSH", "al1", "hello")
        _bgrewriteaof_and_restart(self.server)
        assert self.client.execute_command("FLASH.LRANGE", "al1", "0", "-1") == [b"hello"]

    def test_multi_element_list_order_preserved_after_aof(self):
        _enable_aof(self.client)
        self.client.execute_command("FLASH.RPUSH", "al2", "x", "y", "z")
        _bgrewriteaof_and_restart(self.server)
        assert self.client.execute_command("FLASH.LRANGE", "al2", "0", "-1") == [b"x", b"y", b"z"]

    def test_list_ttl_survives_aof(self):
        _enable_aof(self.client)
        self.client.execute_command("FLASH.RPUSH", "al3", "t")
        self.client.execute_command("PEXPIRE", "al3", 600_000)
        _bgrewriteaof_and_restart(self.server)
        assert self.client.execute_command("FLASH.LRANGE", "al3", "0", "-1") == [b"t"]
        assert self.client.execute_command("TTL", "al3") > 0

    def test_list_without_ttl_no_expiry_after_aof(self):
        _enable_aof(self.client)
        self.client.execute_command("FLASH.RPUSH", "al4", "u")
        _bgrewriteaof_and_restart(self.server)
        assert self.client.execute_command("TTL", "al4") == -1

    def test_aof_chunks_128_elements_correctly(self):
        # 129 elements forces two AOF RPUSH commands (128 + 1).
        _enable_aof(self.client)
        elems = [str(i) for i in range(129)]
        self.client.execute_command("FLASH.RPUSH", "al5", *elems)
        _bgrewriteaof_and_restart(self.server)
        result = self.client.execute_command("FLASH.LRANGE", "al5", "0", "-1")
        assert result == [e.encode() for e in elems]

    def test_aof_exact_128_elements(self):
        _enable_aof(self.client)
        elems = [str(i) for i in range(128)]
        self.client.execute_command("FLASH.RPUSH", "al6", *elems)
        _bgrewriteaof_and_restart(self.server)
        result = self.client.execute_command("FLASH.LRANGE", "al6", "0", "-1")
        assert result == [e.encode() for e in elems]

    def test_aof_256_elements_three_chunks(self):
        # 256 elements → two full chunks of 128.
        _enable_aof(self.client)
        elems = [str(i) for i in range(256)]
        self.client.execute_command("FLASH.RPUSH", "al7", *elems)
        _bgrewriteaof_and_restart(self.server)
        result = self.client.execute_command("FLASH.LRANGE", "al7", "0", "-1")
        assert result == [e.encode() for e in elems]

    def test_deleted_list_absent_after_aof_restart(self):
        _enable_aof(self.client)
        self.client.execute_command("FLASH.RPUSH", "al8", "v")
        self.client.execute_command("DEL", "al8")
        _bgrewriteaof_and_restart(self.server)
        assert self.client.execute_command("FLASH.LLEN", "al8") == 0

    def test_mixed_string_and_list_survive_aof(self):
        _enable_aof(self.client)
        self.client.execute_command("FLASH.SET", "alstr", "sv")
        self.client.execute_command("FLASH.RPUSH", "allist", "lv")
        _bgrewriteaof_and_restart(self.server)
        assert self.client.execute_command("FLASH.GET", "alstr") == b"sv"
        assert self.client.execute_command("FLASH.LPOP", "allist") == b"lv"

    def test_debug_reload_preserves_list(self):
        _enable_aof(self.client)
        self.client.execute_command("FLASH.RPUSH", "al9", "p", "q", "r")
        self.client.bgrewriteaof()
        self.server.wait_for_action_done(ValkeyAction.AOF_REWRITE)
        self.client.execute_command("DEBUG", "RELOAD")
        assert self.client.execute_command("FLASH.LRANGE", "al9", "0", "-1") == [b"p", b"q", b"r"]

    def test_cold_list_survives_aof(self):
        _enable_aof(self.client)
        self.client.execute_command("FLASH.RPUSH", "al_cold", "x", "y", "z")
        self.client.execute_command("FLASH.DEBUG.DEMOTE", "al_cold")
        _bgrewriteaof_and_restart(self.server)
        result = self.client.execute_command("FLASH.LRANGE", "al_cold", "0", "-1")
        assert result == [b"x", b"y", b"z"]
