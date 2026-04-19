import pytest
from valkey import ResponseError
from valkeytestframework.valkey_test_case import ValkeyAction
from valkey_flash_test_case import ValkeyFlashTestCase


def _enable_aof(client):
    client.config_set("appendonly", "yes")


def _bgrewriteaof_and_restart(server):
    server.client.bgrewriteaof()
    server.wait_for_action_done(ValkeyAction.AOF_REWRITE)
    server.restart()


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
        self.client.execute_command("FLASH.ZADD", "zrng6", "1.0", "a", "2.0", "b", "3.0", "c", "4.0", "d")
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
        result = self.client.execute_command("FLASH.ZRANGEBYSCORE", "zrbs2", "-inf", "+inf", "WITHSCORES")
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
        self.client.execute_command("FLASH.ZADD", "zsc2", "1.0", "apple", "2.0", "banana", "3.0", "avocado")
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
        self.client.execute_command(
            "FLASH.ZADD", "zaof_cold", "1.0", "a", "2.0", "b", "3.0", "c"
        )
        self.client.execute_command("FLASH.DEBUG.DEMOTE", "zaof_cold")
        _bgrewriteaof_and_restart(self.server)
        result = self.client.execute_command(
            "FLASH.ZRANGE", "zaof_cold", "0", "-1", "WITHSCORES"
        )
        assert result == [b"a", b"1", b"b", b"2", b"c", b"3"]
