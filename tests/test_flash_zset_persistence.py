"""Integration tests for FlashZSet RDB and AOF persistence."""

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


class TestFlashZSetRdb(ValkeyFlashTestCase):
    def test_zset_bgsave_round_trip_small(self):
        self.client.execute_command("FLASH.ZADD", "rz1", "1.0", "a", "2.0", "b", "3.0", "c")
        _bgsave_and_restart(self.server)
        assert float(self.client.execute_command("FLASH.ZSCORE", "rz1", "a")) == 1.0
        assert float(self.client.execute_command("FLASH.ZSCORE", "rz1", "b")) == 2.0
        assert float(self.client.execute_command("FLASH.ZSCORE", "rz1", "c")) == 3.0
        assert self.client.execute_command("FLASH.ZCARD", "rz1") == 3

    def test_zset_bgsave_round_trip_large(self):
        # >64 score-member pairs to span more than one aof_rewrite chunk boundary.
        pairs = []
        for i in range(100):
            pairs += [str(float(i)), f"m{i}"]
        self.client.execute_command("FLASH.ZADD", "rz2", *pairs)
        _bgsave_and_restart(self.server)
        assert self.client.execute_command("FLASH.ZCARD", "rz2") == 100
        assert float(self.client.execute_command("FLASH.ZSCORE", "rz2", "m0")) == 0.0
        assert float(self.client.execute_command("FLASH.ZSCORE", "rz2", "m99")) == 99.0

    def test_zset_bgsave_round_trip_ttl(self):
        self.client.execute_command("FLASH.ZADD", "rz3", "5.0", "x")
        self.client.execute_command("PEXPIRE", "rz3", 600_000)
        _bgsave_and_restart(self.server)
        assert self.client.execute_command("FLASH.ZCARD", "rz3") == 1
        assert self.client.execute_command("TTL", "rz3") > 0

    def test_zset_bgsave_no_ttl_stays_persistent(self):
        self.client.execute_command("FLASH.ZADD", "rz4", "1.0", "m")
        _bgsave_and_restart(self.server)
        assert self.client.execute_command("TTL", "rz4") == -1

    def test_zset_bgsave_preserves_scores_exactly(self):
        # Test that f64 scores survive the RDB round-trip without precision loss.
        self.client.execute_command(
            "FLASH.ZADD", "rz5", "3.14", "pi", "-inf", "neginf", "inf", "posinf"
        )
        _bgsave_and_restart(self.server)
        assert self.client.execute_command("FLASH.ZSCORE", "rz5", "neginf") == b"-inf"
        assert self.client.execute_command("FLASH.ZSCORE", "rz5", "posinf") == b"+inf"
        score_pi = float(self.client.execute_command("FLASH.ZSCORE", "rz5", "pi"))
        assert abs(score_pi - 3.14) < 1e-10

    def test_zset_bgsave_binary_member(self):
        member = bytes(range(256))
        self.client.execute_command("FLASH.ZADD", "rz6", "1.0", member)
        _bgsave_and_restart(self.server)
        assert self.client.execute_command("FLASH.ZCARD", "rz6") == 1

    def test_cold_zset_survives_rdb(self):
        self.client.execute_command("FLASH.ZADD", "rz_cold", "1.0", "a", "2.0", "b", "3.0", "c")
        self.client.execute_command("FLASH.DEBUG.DEMOTE", "rz_cold")
        _bgsave_and_restart(self.server)
        assert self.client.execute_command("FLASH.ZCARD", "rz_cold") == 3
        assert float(self.client.execute_command("FLASH.ZSCORE", "rz_cold", "b")) == 2.0

    def test_cold_zset_ttl_survives_rdb(self):
        self.client.execute_command("FLASH.ZADD", "rz_cold_ttl", "1.0", "x")
        self.client.execute_command("PEXPIRE", "rz_cold_ttl", 600_000)
        self.client.execute_command("FLASH.DEBUG.DEMOTE", "rz_cold_ttl")
        _bgsave_and_restart(self.server)
        assert self.client.execute_command("FLASH.ZCARD", "rz_cold_ttl") == 1
        assert self.client.execute_command("TTL", "rz_cold_ttl") > 0

    def test_zset_bgsave_order_preserved(self):
        # BTreeMap order (score asc, then member-lex) must be preserved after restart.
        self.client.execute_command("FLASH.ZADD", "rz_ord", "3", "c", "1", "a", "2", "b")
        _bgsave_and_restart(self.server)
        result = self.client.execute_command("FLASH.ZRANGE", "rz_ord", "0", "-1", "WITHSCORES")
        assert result == [b"a", b"1", b"b", b"2", b"c", b"3"]

    def test_mixed_zset_and_string_survive_rdb(self):
        self.client.execute_command("FLASH.SET", "rz_str", "hello")
        self.client.execute_command("FLASH.ZADD", "rz_zset", "1.0", "m")
        _bgsave_and_restart(self.server)
        assert self.client.execute_command("FLASH.GET", "rz_str") == b"hello"
        assert self.client.execute_command("FLASH.ZCARD", "rz_zset") == 1


# ── AOF round-trip tests ──────────────────────────────────────────────────────


class TestFlashZSetAofPersistence(ValkeyFlashTestCase):
    def test_zset_bgrewriteaof_small(self):
        _enable_aof(self.client)
        self.client.execute_command("FLASH.ZADD", "az1", "1.0", "a", "2.0", "b")
        _bgrewriteaof_and_restart(self.server)
        assert float(self.client.execute_command("FLASH.ZSCORE", "az1", "a")) == 1.0
        assert float(self.client.execute_command("FLASH.ZSCORE", "az1", "b")) == 2.0

    def test_zset_bgrewriteaof_large(self):
        # >64 pairs — verifies chunked ZADD emission (64 pairs per command).
        _enable_aof(self.client)
        pairs = []
        for i in range(130):
            pairs += [str(float(i)), f"m{i}"]
        self.client.execute_command("FLASH.ZADD", "az2", *pairs)
        _bgrewriteaof_and_restart(self.server)
        assert self.client.execute_command("FLASH.ZCARD", "az2") == 130
        assert float(self.client.execute_command("FLASH.ZSCORE", "az2", "m0")) == 0.0
        assert float(self.client.execute_command("FLASH.ZSCORE", "az2", "m129")) == 129.0

    def test_zset_bgrewriteaof_ttl(self):
        _enable_aof(self.client)
        self.client.execute_command("FLASH.ZADD", "az3", "7.0", "x")
        self.client.execute_command("PEXPIRE", "az3", 600_000)
        _bgrewriteaof_and_restart(self.server)
        assert self.client.execute_command("FLASH.ZCARD", "az3") == 1
        assert self.client.execute_command("TTL", "az3") > 0

    def test_zset_bgrewriteaof_no_ttl_stays_persistent(self):
        _enable_aof(self.client)
        self.client.execute_command("FLASH.ZADD", "az4", "1.0", "m")
        _bgrewriteaof_and_restart(self.server)
        assert self.client.execute_command("TTL", "az4") == -1

    def test_zset_bgrewriteaof_deterministic_order(self):
        # Order after AOF replay must match pre-save BTreeMap order.
        _enable_aof(self.client)
        self.client.execute_command("FLASH.ZADD", "az5", "3", "c", "1", "a", "2", "b")
        _bgrewriteaof_and_restart(self.server)
        result = self.client.execute_command("FLASH.ZRANGE", "az5", "0", "-1", "WITHSCORES")
        assert result == [b"a", b"1", b"b", b"2", b"c", b"3"]

    def test_zset_bgrewriteaof_exact_64_pairs(self):
        # Exactly 64 pairs — a single full chunk.
        _enable_aof(self.client)
        pairs = []
        for i in range(64):
            pairs += [str(float(i)), f"m{i}"]
        self.client.execute_command("FLASH.ZADD", "az6", *pairs)
        _bgrewriteaof_and_restart(self.server)
        assert self.client.execute_command("FLASH.ZCARD", "az6") == 64

    def test_zset_bgrewriteaof_cold_tier(self):
        _enable_aof(self.client)
        self.client.execute_command("FLASH.ZADD", "az_cold", "1.0", "a", "2.0", "b")
        self.client.execute_command("FLASH.DEBUG.DEMOTE", "az_cold")
        _bgrewriteaof_and_restart(self.server)
        assert self.client.execute_command("FLASH.ZCARD", "az_cold") == 2
        assert float(self.client.execute_command("FLASH.ZSCORE", "az_cold", "a")) == 1.0

    def test_zset_bgrewriteaof_infinity_scores(self):
        _enable_aof(self.client)
        self.client.execute_command("FLASH.ZADD", "az_inf", "-inf", "lo", "inf", "hi")
        _bgrewriteaof_and_restart(self.server)
        assert self.client.execute_command("FLASH.ZSCORE", "az_inf", "lo") == b"-inf"
        assert self.client.execute_command("FLASH.ZSCORE", "az_inf", "hi") == b"+inf"
