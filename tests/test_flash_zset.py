import math
import os as _os

import pytest
from valkey import ResponseError
from valkey_flash_test_case import ValkeyFlashTestCase
from valkeytestframework.util.waiters import wait_for_equal
from valkeytestframework.valkey_test_case import ValkeyAction


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


class TestFlashZAdd(ValkeyFlashTestCase):
    def test_zadd_creates_key_returns_added_count(self):
        result = self.client.execute_command("FLASH.ZADD", "z1", "1.0", "a")
        assert result == 1

    def test_zadd_multiple_members(self):
        result = self.client.execute_command("FLASH.ZADD", "z2", "1.0", "a", "2.0", "b", "3.0", "c")
        assert result == 3

    def test_zadd_update_existing_returns_zero(self):
        self.client.execute_command("FLASH.ZADD", "z3", "1.0", "a")
        result = self.client.execute_command("FLASH.ZADD", "z3", "5.0", "a")
        assert result == 0

    def test_zadd_nx_skips_existing_member(self):
        self.client.execute_command("FLASH.ZADD", "z4", "1.0", "a")
        result = self.client.execute_command("FLASH.ZADD", "z4", "NX", "9.0", "a")
        assert result == 0
        score = self.client.execute_command("FLASH.ZSCORE", "z4", "a")
        assert float(score) == 1.0

    def test_zadd_nx_adds_new_member(self):
        self.client.execute_command("FLASH.ZADD", "z5", "1.0", "a")
        result = self.client.execute_command("FLASH.ZADD", "z5", "NX", "2.0", "b")
        assert result == 1

    def test_zadd_xx_updates_existing(self):
        self.client.execute_command("FLASH.ZADD", "z6", "1.0", "a")
        result = self.client.execute_command("FLASH.ZADD", "z6", "XX", "9.0", "a")
        assert result == 0
        score = self.client.execute_command("FLASH.ZSCORE", "z6", "a")
        assert float(score) == 9.0

    def test_zadd_xx_skips_new_member(self):
        self.client.execute_command("FLASH.ZADD", "z7", "1.0", "a")
        result = self.client.execute_command("FLASH.ZADD", "z7", "XX", "2.0", "new")
        assert result == 0
        assert self.client.execute_command("FLASH.ZSCORE", "z7", "new") is None

    def test_zadd_ch_counts_changed(self):
        self.client.execute_command("FLASH.ZADD", "z8", "1.0", "a", "2.0", "b")
        result = self.client.execute_command("FLASH.ZADD", "z8", "CH", "9.0", "a", "3.0", "c")
        assert result == 2  # "a" changed score + "c" new

    def test_zadd_gt_updates_only_higher(self):
        self.client.execute_command("FLASH.ZADD", "z9", "5.0", "a")
        self.client.execute_command("FLASH.ZADD", "z9", "GT", "3.0", "a")
        assert float(self.client.execute_command("FLASH.ZSCORE", "z9", "a")) == 5.0
        self.client.execute_command("FLASH.ZADD", "z9", "GT", "7.0", "a")
        assert float(self.client.execute_command("FLASH.ZSCORE", "z9", "a")) == 7.0

    def test_zadd_lt_updates_only_lower(self):
        self.client.execute_command("FLASH.ZADD", "z10", "5.0", "a")
        self.client.execute_command("FLASH.ZADD", "z10", "LT", "7.0", "a")
        assert float(self.client.execute_command("FLASH.ZSCORE", "z10", "a")) == 5.0
        self.client.execute_command("FLASH.ZADD", "z10", "LT", "3.0", "a")
        assert float(self.client.execute_command("FLASH.ZSCORE", "z10", "a")) == 3.0

    def test_zadd_incr_returns_new_score(self):
        self.client.execute_command("FLASH.ZADD", "z11", "5.0", "a")
        result = self.client.execute_command("FLASH.ZADD", "z11", "INCR", "2.0", "a")
        assert float(result) == 7.0

    def test_zadd_nx_xx_incompatible(self):
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.ZADD", "z12", "NX", "XX", "1.0", "a")

    def test_zadd_gt_lt_incompatible(self):
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.ZADD", "z13", "GT", "LT", "1.0", "a")

    def test_zadd_wrong_arity(self):
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.ZADD", "z14")

    def test_zadd_wrong_type_error(self):
        self.client.execute_command("FLASH.SET", "strkey", "v")
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.ZADD", "strkey", "1.0", "a")


class TestFlashZRem(ValkeyFlashTestCase):
    def test_zrem_existing_member_returns_1(self):
        self.client.execute_command("FLASH.ZADD", "zr1", "1.0", "a")
        result = self.client.execute_command("FLASH.ZREM", "zr1", "a")
        assert result == 1

    def test_zrem_missing_member_returns_0(self):
        self.client.execute_command("FLASH.ZADD", "zr2", "1.0", "a")
        result = self.client.execute_command("FLASH.ZREM", "zr2", "ghost")
        assert result == 0

    def test_zrem_multiple_members(self):
        self.client.execute_command("FLASH.ZADD", "zr3", "1.0", "a", "2.0", "b", "3.0", "c")
        result = self.client.execute_command("FLASH.ZREM", "zr3", "a", "c", "missing")
        assert result == 2

    def test_zrem_last_member_deletes_key(self):
        self.client.execute_command("FLASH.ZADD", "zr4", "1.0", "only")
        self.client.execute_command("FLASH.ZREM", "zr4", "only")
        assert self.client.execute_command("FLASH.ZCARD", "zr4") == 0

    def test_zrem_missing_key_returns_0(self):
        result = self.client.execute_command("FLASH.ZREM", "nokey", "a")
        assert result == 0


class TestFlashZIncrBy(ValkeyFlashTestCase):
    def test_zincrby_increments_score(self):
        self.client.execute_command("FLASH.ZADD", "zi1", "3.0", "a")
        result = self.client.execute_command("FLASH.ZINCRBY", "zi1", "2.0", "a")
        assert float(result) == 5.0

    def test_zincrby_creates_member_if_absent(self):
        result = self.client.execute_command("FLASH.ZINCRBY", "zi2", "1.5", "new")
        assert float(result) == 1.5

    def test_zincrby_decrement_with_negative(self):
        self.client.execute_command("FLASH.ZADD", "zi3", "10.0", "a")
        result = self.client.execute_command("FLASH.ZINCRBY", "zi3", "-4.0", "a")
        assert float(result) == 6.0


class TestFlashZPopMinMax(ValkeyFlashTestCase):
    def test_zpopmin_returns_min_element(self):
        self.client.execute_command("FLASH.ZADD", "zp1", "1.0", "a", "2.0", "b", "3.0", "c")
        result = self.client.execute_command("FLASH.ZPOPMIN", "zp1")
        assert result == [b"a", b"1"]

    def test_zpopmax_returns_max_element(self):
        self.client.execute_command("FLASH.ZADD", "zp2", "1.0", "a", "2.0", "b", "3.0", "c")
        result = self.client.execute_command("FLASH.ZPOPMAX", "zp2")
        assert result == [b"c", b"3"]

    def test_zpopmin_count_returns_multiple(self):
        self.client.execute_command("FLASH.ZADD", "zp3", "1.0", "a", "2.0", "b", "3.0", "c")
        result = self.client.execute_command("FLASH.ZPOPMIN", "zp3", "2")
        assert len(result) == 4  # 2 pairs: member + score each
        assert result[0] == b"a"
        assert result[2] == b"b"

    def test_zpopmin_empty_key_returns_empty(self):
        result = self.client.execute_command("FLASH.ZPOPMIN", "nokey")
        assert result == []

    def test_zpopmax_removes_element_from_set(self):
        self.client.execute_command("FLASH.ZADD", "zp4", "1.0", "a", "2.0", "b")
        self.client.execute_command("FLASH.ZPOPMAX", "zp4")
        assert self.client.execute_command("FLASH.ZCARD", "zp4") == 1
        assert self.client.execute_command("FLASH.ZSCORE", "zp4", "b") is None


class TestFlashZScore(ValkeyFlashTestCase):
    def test_zscore_returns_score(self):
        self.client.execute_command("FLASH.ZADD", "zs1", "3.14", "pi")
        result = self.client.execute_command("FLASH.ZSCORE", "zs1", "pi")
        assert abs(float(result) - 3.14) < 1e-9

    def test_zscore_missing_member_returns_nil(self):
        self.client.execute_command("FLASH.ZADD", "zs2", "1.0", "a")
        assert self.client.execute_command("FLASH.ZSCORE", "zs2", "ghost") is None

    def test_zscore_missing_key_returns_nil(self):
        assert self.client.execute_command("FLASH.ZSCORE", "nokey", "a") is None

    def test_zscore_infinity(self):
        self.client.execute_command("FLASH.ZADD", "zs3", "+inf", "top")
        result = self.client.execute_command("FLASH.ZSCORE", "zs3", "top")
        assert result == b"+inf"


class TestFlashZRankRevRank(ValkeyFlashTestCase):
    def test_zrank_ascending(self):
        self.client.execute_command("FLASH.ZADD", "zrank1", "1.0", "a", "2.0", "b", "3.0", "c")
        assert self.client.execute_command("FLASH.ZRANK", "zrank1", "a") == 0
        assert self.client.execute_command("FLASH.ZRANK", "zrank1", "b") == 1
        assert self.client.execute_command("FLASH.ZRANK", "zrank1", "c") == 2

    def test_zrevrank_descending(self):
        self.client.execute_command("FLASH.ZADD", "zrank2", "1.0", "a", "2.0", "b", "3.0", "c")
        assert self.client.execute_command("FLASH.ZREVRANK", "zrank2", "c") == 0
        assert self.client.execute_command("FLASH.ZREVRANK", "zrank2", "b") == 1
        assert self.client.execute_command("FLASH.ZREVRANK", "zrank2", "a") == 2

    def test_zrank_missing_member_returns_nil(self):
        self.client.execute_command("FLASH.ZADD", "zrank3", "1.0", "a")
        assert self.client.execute_command("FLASH.ZRANK", "zrank3", "ghost") is None

    def test_zrank_withscore(self):
        self.client.execute_command("FLASH.ZADD", "zrank4", "5.0", "a")
        result = self.client.execute_command("FLASH.ZRANK", "zrank4", "a", "WITHSCORE")
        assert result[0] == 0
        assert float(result[1]) == 5.0

    def test_zrank_missing_key_returns_nil(self):
        assert self.client.execute_command("FLASH.ZRANK", "nokey", "a") is None


class TestFlashZCard(ValkeyFlashTestCase):
    def test_zcard_returns_count(self):
        self.client.execute_command("FLASH.ZADD", "zc1", "1.0", "a", "2.0", "b")
        assert self.client.execute_command("FLASH.ZCARD", "zc1") == 2

    def test_zcard_missing_key_returns_0(self):
        assert self.client.execute_command("FLASH.ZCARD", "nokey") == 0


class TestFlashZCount(ValkeyFlashTestCase):
    def test_zcount_all_members(self):
        self.client.execute_command("FLASH.ZADD", "zct1", "1.0", "a", "2.0", "b", "3.0", "c")
        assert self.client.execute_command("FLASH.ZCOUNT", "zct1", "-inf", "+inf") == 3

    def test_zcount_range(self):
        self.client.execute_command("FLASH.ZADD", "zct2", "1.0", "a", "2.0", "b", "3.0", "c")
        assert self.client.execute_command("FLASH.ZCOUNT", "zct2", "1.0", "2.0") == 2

    def test_zcount_exclusive_bound(self):
        self.client.execute_command("FLASH.ZADD", "zct3", "1.0", "a", "2.0", "b", "3.0", "c")
        assert self.client.execute_command("FLASH.ZCOUNT", "zct3", "(1.0", "3.0") == 2

    def test_zcount_missing_key_returns_0(self):
        assert self.client.execute_command("FLASH.ZCOUNT", "nokey", "-inf", "+inf") == 0


class TestFlashZLexCount(ValkeyFlashTestCase):
    def test_zlexcount_range(self):
        self.client.execute_command("FLASH.ZADD", "zlc1", "0", "a", "0", "b", "0", "c", "0", "d")
        assert self.client.execute_command("FLASH.ZLEXCOUNT", "zlc1", "[b", "[c") == 2

    def test_zlexcount_all(self):
        self.client.execute_command("FLASH.ZADD", "zlc2", "0", "a", "0", "b", "0", "c")
        assert self.client.execute_command("FLASH.ZLEXCOUNT", "zlc2", "-", "+") == 3

    def test_zlexcount_exclusive(self):
        self.client.execute_command("FLASH.ZADD", "zlc3", "0", "a", "0", "b", "0", "c")
        assert self.client.execute_command("FLASH.ZLEXCOUNT", "zlc3", "(a", "(c") == 1


class TestFlashZRange(ValkeyFlashTestCase):
    def test_zrange_by_index(self):
        self.client.execute_command("FLASH.ZADD", "zrng1", "1.0", "a", "2.0", "b", "3.0", "c")
        result = self.client.execute_command("FLASH.ZRANGE", "zrng1", "0", "-1")
        assert result == [b"a", b"b", b"c"]

    def test_zrange_with_scores(self):
        self.client.execute_command("FLASH.ZADD", "zrng2", "1.0", "a", "2.0", "b")
        result = self.client.execute_command("FLASH.ZRANGE", "zrng2", "0", "-1", "WITHSCORES")
        assert result[0] == b"a"
        assert float(result[1]) == 1.0

    def test_zrange_rev(self):
        self.client.execute_command("FLASH.ZADD", "zrng3", "1.0", "a", "2.0", "b", "3.0", "c")
        result = self.client.execute_command("FLASH.ZRANGE", "zrng3", "0", "-1", "REV")
        assert result == [b"c", b"b", b"a"]

    def test_zrange_byscore(self):
        self.client.execute_command("FLASH.ZADD", "zrng4", "1.0", "a", "2.0", "b", "3.0", "c")
        result = self.client.execute_command("FLASH.ZRANGE", "zrng4", "1.0", "2.0", "BYSCORE")
        assert result == [b"a", b"b"]

    def test_zrange_bylex(self):
        self.client.execute_command("FLASH.ZADD", "zrng5", "0", "a", "0", "b", "0", "c", "0", "d")
        result = self.client.execute_command("FLASH.ZRANGE", "zrng5", "[b", "[c", "BYLEX")
        assert result == [b"b", b"c"]

    def test_zrange_limit(self):
        self.client.execute_command(
            "FLASH.ZADD", "zrng6", "1.0", "a", "2.0", "b", "3.0", "c", "4.0", "d"
        )
        result = self.client.execute_command(
            "FLASH.ZRANGE", "zrng6", "-inf", "+inf", "BYSCORE", "LIMIT", "1", "2"
        )
        assert result == [b"b", b"c"]

    def test_zrange_missing_key_returns_empty(self):
        result = self.client.execute_command("FLASH.ZRANGE", "nokey", "0", "-1")
        assert result == []


class TestFlashZRangeByScore(ValkeyFlashTestCase):
    def test_zrangebyscore_basic(self):
        self.client.execute_command("FLASH.ZADD", "zrbs1", "1.0", "a", "2.0", "b", "3.0", "c")
        result = self.client.execute_command("FLASH.ZRANGEBYSCORE", "zrbs1", "1.0", "2.0")
        assert result == [b"a", b"b"]

    def test_zrangebyscore_withscores(self):
        self.client.execute_command("FLASH.ZADD", "zrbs2", "1.0", "a", "2.0", "b")
        result = self.client.execute_command(
            "FLASH.ZRANGEBYSCORE", "zrbs2", "-inf", "+inf", "WITHSCORES"
        )
        assert result == [b"a", b"1", b"b", b"2"]

    def test_zrangebyscore_limit(self):
        self.client.execute_command("FLASH.ZADD", "zrbs3", "1.0", "a", "2.0", "b", "3.0", "c")
        result = self.client.execute_command(
            "FLASH.ZRANGEBYSCORE", "zrbs3", "-inf", "+inf", "LIMIT", "1", "1"
        )
        assert result == [b"b"]

    def test_zrevrangebyscore_basic(self):
        self.client.execute_command("FLASH.ZADD", "zrbs4", "1.0", "a", "2.0", "b", "3.0", "c")
        result = self.client.execute_command("FLASH.ZREVRANGEBYSCORE", "zrbs4", "3.0", "2.0")
        assert result == [b"c", b"b"]


class TestFlashZRangeByLex(ValkeyFlashTestCase):
    def test_zrangebylex_basic(self):
        self.client.execute_command("FLASH.ZADD", "zrbl1", "0", "a", "0", "b", "0", "c", "0", "d")
        result = self.client.execute_command("FLASH.ZRANGEBYLEX", "zrbl1", "[b", "[c")
        assert result == [b"b", b"c"]

    def test_zrangebylex_all(self):
        self.client.execute_command("FLASH.ZADD", "zrbl2", "0", "a", "0", "b", "0", "c")
        result = self.client.execute_command("FLASH.ZRANGEBYLEX", "zrbl2", "-", "+")
        assert result == [b"a", b"b", b"c"]

    def test_zrevrangebylex_basic(self):
        self.client.execute_command("FLASH.ZADD", "zrbl3", "0", "a", "0", "b", "0", "c", "0", "d")
        result = self.client.execute_command("FLASH.ZREVRANGEBYLEX", "zrbl3", "[c", "[b")
        assert result == [b"c", b"b"]


class TestFlashZScan(ValkeyFlashTestCase):
    def test_zscan_returns_all_members(self):
        self.client.execute_command("FLASH.ZADD", "zsc1", "1.0", "a", "2.0", "b", "3.0", "c")
        result = self.client.execute_command("FLASH.ZSCAN", "zsc1", "0")
        cursor, items = result[0], result[1]
        assert cursor == b"0"
        assert len(items) == 6  # 3 member+score pairs

    def test_zscan_match_filter(self):
        self.client.execute_command(
            "FLASH.ZADD", "zsc2", "1.0", "apple", "2.0", "banana", "3.0", "avocado"
        )
        result = self.client.execute_command("FLASH.ZSCAN", "zsc2", "0", "MATCH", "a*")
        _, items = result
        members = items[::2]
        assert b"apple" in members
        assert b"avocado" in members
        assert b"banana" not in members

    def test_zscan_pagination(self):
        for i in range(20):
            self.client.execute_command("FLASH.ZADD", "zsc3", str(float(i)), f"m{i:02d}")
        all_members = []
        cursor = b"0"
        while True:
            result = self.client.execute_command("FLASH.ZSCAN", "zsc3", cursor, "COUNT", "5")
            cursor, items = result[0], result[1]
            all_members.extend(items[::2])
            if cursor == b"0":
                break
        assert len(all_members) == 20

    def test_zscan_empty_key_returns_empty(self):
        result = self.client.execute_command("FLASH.ZSCAN", "nokey", "0")
        cursor, items = result[0], result[1]
        assert cursor == b"0"
        assert items == []


# ── NaN guard tests ───────────────────────────────────────────────────────────


class TestFlashZSetNaN(ValkeyFlashTestCase):
    def test_zincrby_nan_returns_error(self):
        self.client.execute_command("FLASH.ZADD", "znan1", "+inf", "a")
        with pytest.raises(ResponseError, match="NaN"):
            self.client.execute_command("FLASH.ZINCRBY", "znan1", "-inf", "a")

    def test_zadd_incr_nan_returns_error(self):
        self.client.execute_command("FLASH.ZADD", "znan2", "+inf", "a")
        with pytest.raises(ResponseError, match="NaN"):
            self.client.execute_command("FLASH.ZADD", "znan2", "INCR", "-inf", "a")


# ── Cold-tier AOF persistence ─────────────────────────────────────────────────


class TestFlashZSetAof(ValkeyFlashTestCase):
    def test_cold_zset_survives_aof(self):
        _enable_aof(self.client)
        self.client.execute_command("FLASH.ZADD", "zaof_cold", "1.0", "a", "2.0", "b", "3.0", "c")
        self.client.execute_command("FLASH.DEBUG.DEMOTE", "zaof_cold")
        _bgrewriteaof_and_restart(self.server)
        result = self.client.execute_command("FLASH.ZRANGE", "zaof_cold", "0", "-1", "WITHSCORES")
        assert result == [b"a", b"1", b"b", b"2", b"c", b"3"]


# ── Cold-tier demotion + promotion ────────────────────────────────────────────


class TestFlashZSetColdTier(ValkeyFlashTestCase):
    def test_zscore_after_demote(self):
        self.client.execute_command("FLASH.ZADD", "cold_zs", "3.14", "pi")
        self.client.execute_command("FLASH.DEBUG.DEMOTE", "cold_zs")
        result = self.client.execute_command("FLASH.ZSCORE", "cold_zs", "pi")
        assert abs(float(result) - 3.14) < 1e-9

    def test_zrank_after_demote(self):
        self.client.execute_command("FLASH.ZADD", "cold_zrank", "1.0", "a", "2.0", "b", "3.0", "c")
        self.client.execute_command("FLASH.DEBUG.DEMOTE", "cold_zrank")
        assert self.client.execute_command("FLASH.ZRANK", "cold_zrank", "b") == 1

    def test_zcard_after_demote(self):
        self.client.execute_command("FLASH.ZADD", "cold_zcard", "1.0", "a", "2.0", "b")
        self.client.execute_command("FLASH.DEBUG.DEMOTE", "cold_zcard")
        assert self.client.execute_command("FLASH.ZCARD", "cold_zcard") == 2

    def test_zrange_after_demote(self):
        self.client.execute_command("FLASH.ZADD", "cold_zrng", "1.0", "a", "2.0", "b", "3.0", "c")
        self.client.execute_command("FLASH.DEBUG.DEMOTE", "cold_zrng")
        result = self.client.execute_command("FLASH.ZRANGE", "cold_zrng", "0", "-1")
        assert result == [b"a", b"b", b"c"]

    def test_zrangebyscore_after_demote(self):
        self.client.execute_command("FLASH.ZADD", "cold_zrbs", "1.0", "a", "2.0", "b", "3.0", "c")
        self.client.execute_command("FLASH.DEBUG.DEMOTE", "cold_zrbs")
        result = self.client.execute_command("FLASH.ZRANGEBYSCORE", "cold_zrbs", "1.0", "2.0")
        assert result == [b"a", b"b"]

    def test_zpopmin_after_demote_promotes_and_pops(self):
        self.client.execute_command("FLASH.ZADD", "cold_zpm", "1.0", "a", "2.0", "b", "3.0", "c")
        self.client.execute_command("FLASH.DEBUG.DEMOTE", "cold_zpm")
        result = self.client.execute_command("FLASH.ZPOPMIN", "cold_zpm")
        assert result == [b"a", b"1"]
        assert self.client.execute_command("FLASH.ZCARD", "cold_zpm") == 2

    def test_zadd_after_demote_promotes_and_inserts(self):
        self.client.execute_command("FLASH.ZADD", "cold_zadd", "1.0", "a", "2.0", "b")
        self.client.execute_command("FLASH.DEBUG.DEMOTE", "cold_zadd")
        self.client.execute_command("FLASH.ZADD", "cold_zadd", "3.0", "c")
        assert self.client.execute_command("FLASH.ZCARD", "cold_zadd") == 3
        assert float(self.client.execute_command("FLASH.ZSCORE", "cold_zadd", "c")) == 3.0

    def test_zincrby_after_demote(self):
        self.client.execute_command("FLASH.ZADD", "cold_zinc", "5.0", "m")
        self.client.execute_command("FLASH.DEBUG.DEMOTE", "cold_zinc")
        result = self.client.execute_command("FLASH.ZINCRBY", "cold_zinc", "3.0", "m")
        assert float(result) == 8.0

    def test_zrem_after_demote(self):
        self.client.execute_command("FLASH.ZADD", "cold_zrem", "1.0", "a", "2.0", "b", "3.0", "c")
        self.client.execute_command("FLASH.DEBUG.DEMOTE", "cold_zrem")
        result = self.client.execute_command("FLASH.ZREM", "cold_zrem", "b")
        assert result == 1
        assert self.client.execute_command("FLASH.ZCARD", "cold_zrem") == 2


# ── Replication ───────────────────────────────────────────────────────────────


import shutil as _shutil
import tempfile as _tempfile

from valkeytestframework.valkey_test_case import ReplicationTestCase as _ReplTC


def _flash_loadmodule_arg(flash_path):
    return f"{_os.getenv('MODULE_PATH')} path {flash_path} capacity-bytes 16777216"


class TestFlashZSetReplication(_ReplTC):
    @pytest.fixture(autouse=True)
    def setup_test(self, setup):
        binaries_dir = (
            f"{_os.path.dirname(_os.path.realpath(__file__))}"
            f"/build/binaries/{_os.environ['SERVER_VERSION']}"
        )
        server_path = _os.path.join(binaries_dir, "valkey-server")
        existing = _os.environ.get("LD_LIBRARY_PATH", "")
        _os.environ["LD_LIBRARY_PATH"] = f"{binaries_dir}:{existing}" if existing else binaries_dir
        self._flash_dir = _os.path.abspath(
            _tempfile.mkdtemp(prefix="flash_repl_zset_", dir=self.testdir)
        )
        primary_path = _os.path.join(self._flash_dir, "primary.bin")
        self.args = {
            "enable-debug-command": "yes",
            "loadmodule": _flash_loadmodule_arg(primary_path),
        }
        self.server, self.client = self.create_server(
            testdir=self.testdir, server_path=server_path, args=self.args
        )
        yield
        _shutil.rmtree(self._flash_dir, ignore_errors=True)

    def setup_replication(self, num_replicas=1):
        """Override so each replica gets its own flash.bin path."""
        self.num_replicas = num_replicas
        self.replicas = []
        self.skip_teardown = False
        self.create_replicas(num_replicas)
        for i, replica in enumerate(self.replicas):
            replica_path = _os.path.join(self._flash_dir, f"replica{i}.bin")
            replica.args["loadmodule"] = _flash_loadmodule_arg(replica_path)
        self.start_replicas()
        self.wait_for_replicas(self.num_replicas)
        self.wait_for_primary_link_up_all_replicas()
        self.wait_for_all_replicas_online(self.num_replicas)
        for i in range(len(self.replicas)):
            self.waitForReplicaToSyncUp(self.replicas[i])
        return self.replicas

    def test_zadd_replicates_to_replica(self):
        self.setup_replication(num_replicas=1)
        r = self.replicas[0]
        self.client.execute_command("FLASH.ZADD", "repl_z1", "1.0", "a", "2.0", "b")
        self.waitForReplicaToSyncUp(r)
        result = r.client.execute_command("FLASH.ZRANGE", "repl_z1", "0", "-1", "WITHSCORES")
        assert result == [b"a", b"1", b"b", b"2"]

    def test_zrem_replicates_to_replica(self):
        self.setup_replication(num_replicas=1)
        r = self.replicas[0]
        self.client.execute_command("FLASH.ZADD", "repl_z2", "1.0", "a", "2.0", "b")
        self.waitForReplicaToSyncUp(r)
        self.client.execute_command("FLASH.ZREM", "repl_z2", "a")
        self.waitForReplicaToSyncUp(r)
        assert r.client.execute_command("FLASH.ZCARD", "repl_z2") == 1
        assert r.client.execute_command("FLASH.ZSCORE", "repl_z2", "a") is None

    def test_zincrby_replicates_to_replica(self):
        self.setup_replication(num_replicas=1)
        r = self.replicas[0]
        self.client.execute_command("FLASH.ZADD", "repl_z3", "5.0", "m")
        self.waitForReplicaToSyncUp(r)
        self.client.execute_command("FLASH.ZINCRBY", "repl_z3", "3.0", "m")
        self.waitForReplicaToSyncUp(r)
        score = r.client.execute_command("FLASH.ZSCORE", "repl_z3", "m")
        assert float(score) == 8.0


# ── TTL ───────────────────────────────────────────────────────────────────────


class TestFlashZSetTTL(ValkeyFlashTestCase):
    def test_zadd_pexpire_sets_ttl(self):
        self.client.execute_command("FLASH.ZADD", "zttl1", "1.0", "a")
        self.client.execute_command("PEXPIRE", "zttl1", 30_000)
        assert 0 < self.client.execute_command("PTTL", "zttl1") <= 30_000

    def test_zscore_does_not_clear_ttl(self):
        self.client.execute_command("FLASH.ZADD", "zttl2", "1.0", "a")
        self.client.execute_command("PEXPIRE", "zttl2", 30_000)
        self.client.execute_command("FLASH.ZSCORE", "zttl2", "a")
        assert self.client.execute_command("PTTL", "zttl2") > 0

    def test_zadd_does_not_clear_ttl_on_update(self):
        self.client.execute_command("FLASH.ZADD", "zttl3", "1.0", "a")
        self.client.execute_command("PEXPIRE", "zttl3", 30_000)
        self.client.execute_command("FLASH.ZADD", "zttl3", "2.0", "b")
        assert self.client.execute_command("PTTL", "zttl3") > 0

    def test_zrange_does_not_clear_ttl(self):
        self.client.execute_command("FLASH.ZADD", "zttl4", "1.0", "a", "2.0", "b")
        self.client.execute_command("PEXPIRE", "zttl4", 30_000)
        self.client.execute_command("FLASH.ZRANGE", "zttl4", "0", "-1")
        assert self.client.execute_command("PTTL", "zttl4") > 0

    def test_no_ttl_by_default(self):
        self.client.execute_command("FLASH.ZADD", "zttl5", "1.0", "a")
        assert self.client.execute_command("TTL", "zttl5") == -1


# ── Score edge cases ──────────────────────────────────────────────────────────


class TestFlashZSetScoreEdges(ValkeyFlashTestCase):
    def test_zscore_negative_infinity(self):
        self.client.execute_command("FLASH.ZADD", "zedge1", "-inf", "lo")
        result = self.client.execute_command("FLASH.ZSCORE", "zedge1", "lo")
        assert result == b"-inf"

    def test_zscore_exact_zero(self):
        self.client.execute_command("FLASH.ZADD", "zedge2", "0", "zero")
        result = self.client.execute_command("FLASH.ZSCORE", "zedge2", "zero")
        assert float(result) == 0.0

    def test_zadd_very_large_positive_score(self):
        large = "1.7976931348623157e+308"
        self.client.execute_command("FLASH.ZADD", "zedge3", large, "m")
        result = self.client.execute_command("FLASH.ZSCORE", "zedge3", "m")
        assert math.isfinite(float(result))
        assert float(result) > 1e308

    def test_zadd_very_large_negative_score(self):
        self.client.execute_command("FLASH.ZADD", "zedge4", "-1e300", "m")
        result = self.client.execute_command("FLASH.ZSCORE", "zedge4", "m")
        assert float(result) < -1e299

    def test_integer_score_formatted_without_decimal(self):
        self.client.execute_command("FLASH.ZADD", "zedge5", "42", "m")
        result = self.client.execute_command("FLASH.ZSCORE", "zedge5", "m")
        assert result == b"42"

    def test_zpopmin_on_inf_scores(self):
        self.client.execute_command("FLASH.ZADD", "zedge6", "-inf", "lo", "inf", "hi", "0", "mid")
        result = self.client.execute_command("FLASH.ZPOPMIN", "zedge6")
        assert result == [b"lo", b"-inf"]

    def test_zpopmax_on_inf_scores(self):
        self.client.execute_command("FLASH.ZADD", "zedge7", "-inf", "lo", "inf", "hi", "0", "mid")
        result = self.client.execute_command("FLASH.ZPOPMAX", "zedge7")
        assert result == [b"hi", b"+inf"]

    def test_zrangebyscore_exclusive_both_ends(self):
        self.client.execute_command("FLASH.ZADD", "zedge8", "1.0", "a", "2.0", "b", "3.0", "c")
        result = self.client.execute_command("FLASH.ZRANGEBYSCORE", "zedge8", "(1.0", "(3.0")
        assert result == [b"b"]

    def test_zrangebyscore_with_limit(self):
        self.client.execute_command(
            "FLASH.ZADD", "zedge9", "1.0", "a", "2.0", "b", "3.0", "c", "4.0", "d"
        )
        result = self.client.execute_command(
            "FLASH.ZRANGEBYSCORE", "zedge9", "-inf", "+inf", "LIMIT", "1", "2"
        )
        assert result == [b"b", b"c"]


# ── Advanced ZRANGE combos ────────────────────────────────────────────────────


class TestFlashZRangeCombos(ValkeyFlashTestCase):
    def test_zrange_rev_byscore(self):
        self.client.execute_command("FLASH.ZADD", "zrc1", "1.0", "a", "2.0", "b", "3.0", "c")
        result = self.client.execute_command("FLASH.ZRANGE", "zrc1", "3.0", "1.0", "BYSCORE", "REV")
        assert result == [b"c", b"b", b"a"]

    def test_zrange_bylex_rev(self):
        self.client.execute_command("FLASH.ZADD", "zrc2", "0", "a", "0", "b", "0", "c", "0", "d")
        result = self.client.execute_command("FLASH.ZRANGE", "zrc2", "[c", "[a", "BYLEX", "REV")
        assert result == [b"c", b"b", b"a"]

    def test_zrange_byscore_withscores(self):
        self.client.execute_command("FLASH.ZADD", "zrc3", "1.0", "a", "2.0", "b")
        result = self.client.execute_command(
            "FLASH.ZRANGE", "zrc3", "1.0", "2.0", "BYSCORE", "WITHSCORES"
        )
        assert result == [b"a", b"1", b"b", b"2"]

    def test_zrevrangebyscore_withscores_limit(self):
        self.client.execute_command(
            "FLASH.ZADD", "zrc4", "1.0", "a", "2.0", "b", "3.0", "c", "4.0", "d"
        )
        result = self.client.execute_command(
            "FLASH.ZREVRANGEBYSCORE", "zrc4", "+inf", "-inf", "WITHSCORES", "LIMIT", "1", "2"
        )
        assert result == [b"c", b"3", b"b", b"2"]

    def test_zrevrangebylex_limit(self):
        self.client.execute_command(
            "FLASH.ZADD", "zrc5", "0", "a", "0", "b", "0", "c", "0", "d", "0", "e"
        )
        result = self.client.execute_command(
            "FLASH.ZREVRANGEBYLEX", "zrc5", "+", "-", "LIMIT", "1", "2"
        )
        assert result == [b"d", b"c"]

    def test_zrank_withscore_on_existing(self):
        self.client.execute_command("FLASH.ZADD", "zrc6", "7.5", "m")
        result = self.client.execute_command("FLASH.ZRANK", "zrc6", "m", "WITHSCORE")
        assert result[0] == 0
        assert float(result[1]) == 7.5

    def test_zrevrank_withscore(self):
        self.client.execute_command("FLASH.ZADD", "zrc7", "1.0", "a", "2.0", "b", "3.0", "c")
        result = self.client.execute_command("FLASH.ZREVRANK", "zrc7", "b", "WITHSCORE")
        assert result[0] == 1
        assert float(result[1]) == 2.0


# ── Dual-index consistency ────────────────────────────────────────────────────


class TestFlashZSetDualIndexConsistency(ValkeyFlashTestCase):
    def test_zadd_update_changes_score_position(self):
        """After updating a score, ZRANK must reflect the new sorted position."""
        self.client.execute_command("FLASH.ZADD", "dic1", "1.0", "a", "2.0", "b", "3.0", "c")
        self.client.execute_command("FLASH.ZADD", "dic1", "10.0", "a")
        assert self.client.execute_command("FLASH.ZRANK", "dic1", "a") == 2
        assert self.client.execute_command("FLASH.ZRANK", "dic1", "b") == 0

    def test_zrangebyscore_consistent_with_zrangebylex_same_score(self):
        """Members at equal scores must be lex-ordered in both BYSCORE and BYLEX."""
        self.client.execute_command(
            "FLASH.ZADD", "dic2", "0", "apple", "0", "banana", "0", "cherry"
        )
        score_result = self.client.execute_command("FLASH.ZRANGEBYSCORE", "dic2", "0", "0")
        lex_result = self.client.execute_command("FLASH.ZRANGEBYLEX", "dic2", "[apple", "[cherry")
        assert score_result == lex_result == [b"apple", b"banana", b"cherry"]

    def test_zincrby_maintains_dual_index_integrity(self):
        """After ZINCRBY, both ZSCORE and ZRANGEBYSCORE must agree on the new score."""
        self.client.execute_command("FLASH.ZADD", "dic3", "5.0", "x", "10.0", "y")
        self.client.execute_command("FLASH.ZINCRBY", "dic3", "8.0", "x")
        assert float(self.client.execute_command("FLASH.ZSCORE", "dic3", "x")) == 13.0
        result = self.client.execute_command("FLASH.ZRANGEBYSCORE", "dic3", "12.0", "14.0")
        assert result == [b"x"]
        assert self.client.execute_command("FLASH.ZRANK", "dic3", "x") == 1

    def test_zrem_removes_from_both_indices(self):
        """After ZREM, the member must not appear in ZRANGEBYSCORE or ZSCORE."""
        self.client.execute_command("FLASH.ZADD", "dic4", "1.0", "a", "2.0", "b", "3.0", "c")
        self.client.execute_command("FLASH.ZREM", "dic4", "b")
        assert self.client.execute_command("FLASH.ZSCORE", "dic4", "b") is None
        result = self.client.execute_command("FLASH.ZRANGEBYSCORE", "dic4", "1.0", "3.0")
        assert b"b" not in result
        assert self.client.execute_command("FLASH.ZCARD", "dic4") == 2


# ── ZPOPMIN / ZPOPMAX extended ────────────────────────────────────────────────


class TestFlashZPopExtended(ValkeyFlashTestCase):
    def test_zpopmin_with_count(self):
        self.client.execute_command("FLASH.ZADD", "zpext1", "1.0", "a", "2.0", "b", "3.0", "c")
        result = self.client.execute_command("FLASH.ZPOPMIN", "zpext1", "2")
        assert len(result) == 4  # 2 pairs
        assert result[0] == b"a"
        assert result[2] == b"b"
        assert self.client.execute_command("FLASH.ZCARD", "zpext1") == 1

    def test_zpopmax_with_count(self):
        self.client.execute_command("FLASH.ZADD", "zpext2", "1.0", "a", "2.0", "b", "3.0", "c")
        result = self.client.execute_command("FLASH.ZPOPMAX", "zpext2", "2")
        assert result[0] == b"c"
        assert result[2] == b"b"

    def test_zpopmin_count_exceeds_size_returns_all(self):
        self.client.execute_command("FLASH.ZADD", "zpext3", "1.0", "a", "2.0", "b")
        result = self.client.execute_command("FLASH.ZPOPMIN", "zpext3", "100")
        assert len(result) == 4  # both pairs
        assert self.client.execute_command("EXISTS", "zpext3") == 0

    def test_zpopmin_on_single_element_deletes_key(self):
        self.client.execute_command("FLASH.ZADD", "zpext4", "1.0", "only")
        self.client.execute_command("FLASH.ZPOPMIN", "zpext4")
        assert self.client.execute_command("EXISTS", "zpext4") == 0

    def test_zpopmax_on_empty_key_returns_empty(self):
        result = self.client.execute_command("FLASH.ZPOPMAX", "zpext_nokey")
        assert result == []

    def test_zpopmin_count_zero_returns_empty(self):
        self.client.execute_command("FLASH.ZADD", "zpext5", "1.0", "a")
        result = self.client.execute_command("FLASH.ZPOPMIN", "zpext5", "0")
        assert result == []
