import pytest
from valkey import ResponseError
from valkey_flash_test_case import ValkeyFlashTestCase


class TestFlashZUnionStore(ValkeyFlashTestCase):

    def test_zunionstore_basic(self):
        self.client.execute_command("FLASH.ZADD", "zu1a", "1.0", "a", "2.0", "b")
        self.client.execute_command("FLASH.ZADD", "zu1b", "3.0", "b", "4.0", "c")
        result = self.client.execute_command("FLASH.ZUNIONSTORE", "zu1dst", "2", "zu1a", "zu1b")
        assert result == 3
        assert float(self.client.execute_command("FLASH.ZSCORE", "zu1dst", "a")) == 1.0
        assert float(self.client.execute_command("FLASH.ZSCORE", "zu1dst", "b")) == 5.0  # SUM
        assert float(self.client.execute_command("FLASH.ZSCORE", "zu1dst", "c")) == 4.0

    def test_zunionstore_aggregate_min(self):
        self.client.execute_command("FLASH.ZADD", "zu2a", "5.0", "m")
        self.client.execute_command("FLASH.ZADD", "zu2b", "3.0", "m")
        result = self.client.execute_command(
            "FLASH.ZUNIONSTORE", "zu2dst", "2", "zu2a", "zu2b", "AGGREGATE", "MIN"
        )
        assert result == 1
        assert float(self.client.execute_command("FLASH.ZSCORE", "zu2dst", "m")) == 3.0

    def test_zunionstore_aggregate_max(self):
        self.client.execute_command("FLASH.ZADD", "zu3a", "5.0", "m")
        self.client.execute_command("FLASH.ZADD", "zu3b", "3.0", "m")
        result = self.client.execute_command(
            "FLASH.ZUNIONSTORE", "zu3dst", "2", "zu3a", "zu3b", "AGGREGATE", "MAX"
        )
        assert result == 1
        assert float(self.client.execute_command("FLASH.ZSCORE", "zu3dst", "m")) == 5.0

    def test_zunionstore_weights(self):
        self.client.execute_command("FLASH.ZADD", "zu4a", "2.0", "x")
        self.client.execute_command("FLASH.ZADD", "zu4b", "3.0", "x")
        self.client.execute_command(
            "FLASH.ZUNIONSTORE", "zu4dst", "2", "zu4a", "zu4b", "WEIGHTS", "2", "1"
        )
        # 2*2 + 1*3 = 7
        assert float(self.client.execute_command("FLASH.ZSCORE", "zu4dst", "x")) == 7.0

    def test_zunionstore_single_source(self):
        self.client.execute_command("FLASH.ZADD", "zu5a", "1.0", "a", "2.0", "b")
        result = self.client.execute_command("FLASH.ZUNIONSTORE", "zu5dst", "1", "zu5a")
        assert result == 2

    def test_zunionstore_missing_source_treated_as_empty(self):
        self.client.execute_command("FLASH.ZADD", "zu6a", "1.0", "a")
        result = self.client.execute_command(
            "FLASH.ZUNIONSTORE", "zu6dst", "2", "zu6a", "nokey"
        )
        assert result == 1

    def test_zunionstore_overwrites_existing_dst(self):
        self.client.execute_command("FLASH.ZADD", "zu7dst", "99.0", "old")
        self.client.execute_command("FLASH.ZADD", "zu7a", "1.0", "new")
        self.client.execute_command("FLASH.ZUNIONSTORE", "zu7dst", "1", "zu7a")
        assert self.client.execute_command("FLASH.ZSCORE", "zu7dst", "old") is None
        assert float(self.client.execute_command("FLASH.ZSCORE", "zu7dst", "new")) == 1.0

    def test_zunionstore_all_empty_deletes_dst(self):
        self.client.execute_command("FLASH.ZADD", "zu8dst", "1.0", "x")
        result = self.client.execute_command(
            "FLASH.ZUNIONSTORE", "zu8dst", "1", "nokey"
        )
        assert result == 0
        assert self.client.execute_command("FLASH.ZCARD", "zu8dst") == 0

    def test_zunionstore_wrongtype_source_raises_error(self):
        self.client.execute_command("FLASH.SET", "zu9str", "v")
        self.client.execute_command("FLASH.ZADD", "zu9z", "1.0", "a")
        with pytest.raises(ResponseError):
            self.client.execute_command(
                "FLASH.ZUNIONSTORE", "zu9dst", "2", "zu9z", "zu9str"
            )


class TestFlashZInterStore(ValkeyFlashTestCase):

    def test_zinterstore_basic(self):
        self.client.execute_command("FLASH.ZADD", "zi1a", "1.0", "a", "2.0", "b")
        self.client.execute_command("FLASH.ZADD", "zi1b", "3.0", "b", "4.0", "c")
        result = self.client.execute_command("FLASH.ZINTERSTORE", "zi1dst", "2", "zi1a", "zi1b")
        assert result == 1
        assert float(self.client.execute_command("FLASH.ZSCORE", "zi1dst", "b")) == 5.0
        assert self.client.execute_command("FLASH.ZSCORE", "zi1dst", "a") is None
        assert self.client.execute_command("FLASH.ZSCORE", "zi1dst", "c") is None

    def test_zinterstore_aggregate_min(self):
        self.client.execute_command("FLASH.ZADD", "zi2a", "5.0", "m")
        self.client.execute_command("FLASH.ZADD", "zi2b", "3.0", "m")
        self.client.execute_command(
            "FLASH.ZINTERSTORE", "zi2dst", "2", "zi2a", "zi2b", "AGGREGATE", "MIN"
        )
        assert float(self.client.execute_command("FLASH.ZSCORE", "zi2dst", "m")) == 3.0

    def test_zinterstore_aggregate_max(self):
        self.client.execute_command("FLASH.ZADD", "zi3a", "5.0", "m")
        self.client.execute_command("FLASH.ZADD", "zi3b", "3.0", "m")
        self.client.execute_command(
            "FLASH.ZINTERSTORE", "zi3dst", "2", "zi3a", "zi3b", "AGGREGATE", "MAX"
        )
        assert float(self.client.execute_command("FLASH.ZSCORE", "zi3dst", "m")) == 5.0

    def test_zinterstore_weights(self):
        self.client.execute_command("FLASH.ZADD", "zi4a", "2.0", "x")
        self.client.execute_command("FLASH.ZADD", "zi4b", "3.0", "x")
        self.client.execute_command(
            "FLASH.ZINTERSTORE", "zi4dst", "2", "zi4a", "zi4b", "WEIGHTS", "3", "1"
        )
        # 3*2 + 1*3 = 9
        assert float(self.client.execute_command("FLASH.ZSCORE", "zi4dst", "x")) == 9.0

    def test_zinterstore_missing_source_returns_empty(self):
        self.client.execute_command("FLASH.ZADD", "zi5a", "1.0", "a")
        result = self.client.execute_command(
            "FLASH.ZINTERSTORE", "zi5dst", "2", "zi5a", "nokey"
        )
        assert result == 0

    def test_zinterstore_three_sources(self):
        self.client.execute_command("FLASH.ZADD", "zi6a", "1.0", "x", "2.0", "y")
        self.client.execute_command("FLASH.ZADD", "zi6b", "1.0", "x", "3.0", "z")
        self.client.execute_command("FLASH.ZADD", "zi6c", "1.0", "x", "4.0", "w")
        result = self.client.execute_command(
            "FLASH.ZINTERSTORE", "zi6dst", "3", "zi6a", "zi6b", "zi6c"
        )
        assert result == 1
        assert float(self.client.execute_command("FLASH.ZSCORE", "zi6dst", "x")) == 3.0

    def test_zinterstore_wrongtype_source_raises_error(self):
        self.client.execute_command("FLASH.RPUSH", "zi7list", "v")
        self.client.execute_command("FLASH.ZADD", "zi7z", "1.0", "a")
        with pytest.raises(ResponseError):
            self.client.execute_command(
                "FLASH.ZINTERSTORE", "zi7dst", "2", "zi7z", "zi7list"
            )


class TestFlashZDiffStore(ValkeyFlashTestCase):

    def test_zdiffstore_basic(self):
        self.client.execute_command("FLASH.ZADD", "zd1a", "1.0", "a", "2.0", "b", "3.0", "c")
        self.client.execute_command("FLASH.ZADD", "zd1b", "99.0", "b")
        result = self.client.execute_command("FLASH.ZDIFFSTORE", "zd1dst", "2", "zd1a", "zd1b")
        assert result == 2
        assert float(self.client.execute_command("FLASH.ZSCORE", "zd1dst", "a")) == 1.0
        assert float(self.client.execute_command("FLASH.ZSCORE", "zd1dst", "c")) == 3.0
        assert self.client.execute_command("FLASH.ZSCORE", "zd1dst", "b") is None

    def test_zdiffstore_empty_diff(self):
        self.client.execute_command("FLASH.ZADD", "zd2a", "1.0", "a")
        self.client.execute_command("FLASH.ZADD", "zd2b", "2.0", "a")
        result = self.client.execute_command("FLASH.ZDIFFSTORE", "zd2dst", "2", "zd2a", "zd2b")
        assert result == 0

    def test_zdiffstore_single_source(self):
        self.client.execute_command("FLASH.ZADD", "zd3a", "1.0", "x", "2.0", "y")
        result = self.client.execute_command("FLASH.ZDIFFSTORE", "zd3dst", "1", "zd3a")
        assert result == 2

    def test_zdiffstore_three_sources(self):
        self.client.execute_command("FLASH.ZADD", "zd4a", "1.0", "a", "2.0", "b", "3.0", "c")
        self.client.execute_command("FLASH.ZADD", "zd4b", "1.0", "a")
        self.client.execute_command("FLASH.ZADD", "zd4c", "1.0", "b")
        result = self.client.execute_command(
            "FLASH.ZDIFFSTORE", "zd4dst", "3", "zd4a", "zd4b", "zd4c"
        )
        assert result == 1
        assert float(self.client.execute_command("FLASH.ZSCORE", "zd4dst", "c")) == 3.0

    def test_zdiffstore_wrongtype_source_raises_error(self):
        self.client.execute_command("FLASH.SET", "zd5str", "v")
        self.client.execute_command("FLASH.ZADD", "zd5z", "1.0", "a")
        with pytest.raises(ResponseError):
            self.client.execute_command(
                "FLASH.ZDIFFSTORE", "zd5dst", "2", "zd5z", "zd5str"
            )


class TestFlashZRangeStore(ValkeyFlashTestCase):

    def test_zrangestore_index_range(self):
        self.client.execute_command(
            "FLASH.ZADD", "zrs1", "1.0", "a", "2.0", "b", "3.0", "c", "4.0", "d"
        )
        result = self.client.execute_command("FLASH.ZRANGESTORE", "zrs1dst", "zrs1", "1", "2")
        assert result == 2
        assert float(self.client.execute_command("FLASH.ZSCORE", "zrs1dst", "b")) == 2.0
        assert float(self.client.execute_command("FLASH.ZSCORE", "zrs1dst", "c")) == 3.0

    def test_zrangestore_byscore(self):
        self.client.execute_command(
            "FLASH.ZADD", "zrs2", "1.0", "a", "2.0", "b", "3.0", "c"
        )
        result = self.client.execute_command(
            "FLASH.ZRANGESTORE", "zrs2dst", "zrs2", "1.5", "3.0", "BYSCORE"
        )
        assert result == 2
        assert self.client.execute_command("FLASH.ZSCORE", "zrs2dst", "a") is None
        assert float(self.client.execute_command("FLASH.ZSCORE", "zrs2dst", "b")) == 2.0

    def test_zrangestore_bylex(self):
        self.client.execute_command(
            "FLASH.ZADD", "zrs3", "0", "a", "0", "b", "0", "c", "0", "d"
        )
        result = self.client.execute_command(
            "FLASH.ZRANGESTORE", "zrs3dst", "zrs3", "[b", "[c", "BYLEX"
        )
        assert result == 2

    def test_zrangestore_rev(self):
        self.client.execute_command(
            "FLASH.ZADD", "zrs4", "1.0", "a", "2.0", "b", "3.0", "c"
        )
        result = self.client.execute_command(
            "FLASH.ZRANGESTORE", "zrs4dst", "zrs4", "0", "1", "REV"
        )
        assert result == 2
        # REV 0..1 takes top 2: c(3), b(2)
        assert float(self.client.execute_command("FLASH.ZSCORE", "zrs4dst", "c")) == 3.0
        assert float(self.client.execute_command("FLASH.ZSCORE", "zrs4dst", "b")) == 2.0

    def test_zrangestore_limit(self):
        self.client.execute_command(
            "FLASH.ZADD", "zrs5", "1.0", "a", "2.0", "b", "3.0", "c", "4.0", "d"
        )
        result = self.client.execute_command(
            "FLASH.ZRANGESTORE", "zrs5dst", "zrs5", "-inf", "+inf", "BYSCORE", "LIMIT", "1", "2"
        )
        assert result == 2
        assert self.client.execute_command("FLASH.ZSCORE", "zrs5dst", "a") is None
        assert float(self.client.execute_command("FLASH.ZSCORE", "zrs5dst", "b")) == 2.0
        assert float(self.client.execute_command("FLASH.ZSCORE", "zrs5dst", "c")) == 3.0

    def test_zrangestore_missing_src_creates_empty_dst(self):
        result = self.client.execute_command(
            "FLASH.ZRANGESTORE", "zrs6dst", "nosrc", "0", "-1"
        )
        assert result == 0

    def test_zrangestore_overwrites_existing_dst(self):
        self.client.execute_command("FLASH.ZADD", "zrs7dst", "99.0", "old")
        self.client.execute_command("FLASH.ZADD", "zrs7src", "1.0", "new")
        self.client.execute_command("FLASH.ZRANGESTORE", "zrs7dst", "zrs7src", "0", "-1")
        assert self.client.execute_command("FLASH.ZSCORE", "zrs7dst", "old") is None
        assert float(self.client.execute_command("FLASH.ZSCORE", "zrs7dst", "new")) == 1.0

    def test_zrangestore_wrongtype_src_raises_error(self):
        self.client.execute_command("FLASH.SET", "zrs8str", "v")
        with pytest.raises(ResponseError):
            self.client.execute_command(
                "FLASH.ZRANGESTORE", "zrs8dst", "zrs8str", "0", "-1"
            )
