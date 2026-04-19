import pytest
from valkey import ResponseError
from valkey_flash_test_case import ValkeyFlashTestCase


class TestFlashHSet(ValkeyFlashTestCase):
    def test_hset_creates_new_key_returns_field_count(self):
        result = self.client.execute_command("FLASH.HSET", "h1", "f1", "v1", "f2", "v2")
        assert result == 2

    def test_hset_overwrite_existing_fields_returns_zero(self):
        self.client.execute_command("FLASH.HSET", "h2", "f", "old")
        result = self.client.execute_command("FLASH.HSET", "h2", "f", "new")
        assert result == 0

    def test_hset_mix_new_and_overwrite_counts_only_new(self):
        self.client.execute_command("FLASH.HSET", "h3", "existing", "v")
        result = self.client.execute_command(
            "FLASH.HSET", "h3", "existing", "v2", "new_field", "vn"
        )
        assert result == 1

    def test_hset_wrong_arity_raises_error(self):
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.HSET", "h")

    def test_hset_odd_field_count_raises_error(self):
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.HSET", "h", "f1", "v1", "f2")

    def test_hset_persists_across_reads(self):
        self.client.execute_command("FLASH.HSET", "h4", "name", "alice", "age", "30")
        assert self.client.execute_command("FLASH.HGET", "h4", "name") == b"alice"
        assert self.client.execute_command("FLASH.HGET", "h4", "age") == b"30"


class TestFlashHGet(ValkeyFlashTestCase):
    def test_hget_existing_field_returns_value(self):
        self.client.execute_command("FLASH.HSET", "hg1", "color", "blue")
        result = self.client.execute_command("FLASH.HGET", "hg1", "color")
        assert result == b"blue"

    def test_hget_missing_field_returns_nil(self):
        self.client.execute_command("FLASH.HSET", "hg2", "x", "1")
        result = self.client.execute_command("FLASH.HGET", "hg2", "ghost")
        assert result is None

    def test_hget_missing_key_returns_nil(self):
        result = self.client.execute_command("FLASH.HGET", "nokey", "f")
        assert result is None

    def test_hget_wrong_arity_raises_error(self):
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.HGET", "k")

    def test_hget_cache_hit_returns_correct_value(self):
        self.client.execute_command("FLASH.HSET", "hg3", "k", "cached_val")
        # Second HGET should be served from cache.
        self.client.execute_command("FLASH.HGET", "hg3", "k")
        result = self.client.execute_command("FLASH.HGET", "hg3", "k")
        assert result == b"cached_val"


class TestFlashHGetAll(ValkeyFlashTestCase):
    def test_hgetall_returns_all_field_value_pairs(self):
        self.client.execute_command("FLASH.HSET", "ha1", "a", "1", "b", "2")
        result = self.client.execute_command("FLASH.HGETALL", "ha1")
        # Sorted by key: a, 1, b, 2
        assert result == [b"a", b"1", b"b", b"2"]

    def test_hgetall_missing_key_returns_empty_array(self):
        result = self.client.execute_command("FLASH.HGETALL", "nokey")
        assert result == []

    def test_hgetall_single_field(self):
        self.client.execute_command("FLASH.HSET", "ha2", "only", "one")
        result = self.client.execute_command("FLASH.HGETALL", "ha2")
        assert result == [b"only", b"one"]

    def test_hgetall_wrong_arity_raises_error(self):
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.HGETALL")

    def test_hgetall_result_is_sorted_by_field(self):
        self.client.execute_command("FLASH.HSET", "ha3", "z", "last", "a", "first", "m", "mid")
        result = self.client.execute_command("FLASH.HGETALL", "ha3")
        fields = result[::2]
        assert fields == sorted(fields)


class TestFlashHDel(ValkeyFlashTestCase):
    def test_hdel_existing_field_returns_one(self):
        self.client.execute_command("FLASH.HSET", "hd1", "f1", "v1")
        result = self.client.execute_command("FLASH.HDEL", "hd1", "f1")
        assert result == 1

    def test_hdel_removes_field(self):
        self.client.execute_command("FLASH.HSET", "hd2", "keep", "v", "drop", "v")
        self.client.execute_command("FLASH.HDEL", "hd2", "drop")
        assert self.client.execute_command("FLASH.HEXISTS", "hd2", "drop") == 0
        assert self.client.execute_command("FLASH.HEXISTS", "hd2", "keep") == 1

    def test_hdel_missing_field_returns_zero(self):
        self.client.execute_command("FLASH.HSET", "hd3", "f", "v")
        result = self.client.execute_command("FLASH.HDEL", "hd3", "ghost")
        assert result == 0

    def test_hdel_missing_key_returns_zero(self):
        result = self.client.execute_command("FLASH.HDEL", "nokey", "f")
        assert result == 0

    def test_hdel_all_fields_removes_key(self):
        self.client.execute_command("FLASH.HSET", "hd4", "only", "v")
        self.client.execute_command("FLASH.HDEL", "hd4", "only")
        assert self.client.execute_command("EXISTS", "hd4") == 0

    def test_hdel_multiple_fields_returns_count(self):
        self.client.execute_command("FLASH.HSET", "hd5", "a", "1", "b", "2", "c", "3")
        result = self.client.execute_command("FLASH.HDEL", "hd5", "a", "b", "ghost")
        assert result == 2

    def test_hdel_wrong_arity_raises_error(self):
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.HDEL", "k")


class TestFlashHExists(ValkeyFlashTestCase):
    def test_hexists_present_field_returns_one(self):
        self.client.execute_command("FLASH.HSET", "he1", "name", "bob")
        result = self.client.execute_command("FLASH.HEXISTS", "he1", "name")
        assert result == 1

    def test_hexists_absent_field_returns_zero(self):
        self.client.execute_command("FLASH.HSET", "he2", "x", "1")
        result = self.client.execute_command("FLASH.HEXISTS", "he2", "ghost")
        assert result == 0

    def test_hexists_missing_key_returns_zero(self):
        result = self.client.execute_command("FLASH.HEXISTS", "nokey", "f")
        assert result == 0

    def test_hexists_wrong_arity_raises_error(self):
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.HEXISTS", "k")


class TestFlashHLen(ValkeyFlashTestCase):
    def test_hlen_returns_field_count(self):
        self.client.execute_command("FLASH.HSET", "hl1", "a", "1", "b", "2", "c", "3")
        result = self.client.execute_command("FLASH.HLEN", "hl1")
        assert result == 3

    def test_hlen_missing_key_returns_zero(self):
        result = self.client.execute_command("FLASH.HLEN", "nokey")
        assert result == 0

    def test_hlen_decrements_after_hdel(self):
        self.client.execute_command("FLASH.HSET", "hl2", "x", "1", "y", "2")
        self.client.execute_command("FLASH.HDEL", "hl2", "x")
        result = self.client.execute_command("FLASH.HLEN", "hl2")
        assert result == 1

    def test_hlen_wrong_arity_raises_error(self):
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.HLEN")


class TestFlashHashColdTier(ValkeyFlashTestCase):
    def test_hget_after_demote_reads_from_nvme(self):
        self.client.execute_command("FLASH.HSET", "cold1", "field", "value")
        self.client.execute_command("FLASH.DEBUG.DEMOTE", "cold1")
        result = self.client.execute_command("FLASH.HGET", "cold1", "field")
        assert result == b"value"

    def test_hgetall_after_demote_returns_all_pairs(self):
        self.client.execute_command("FLASH.HSET", "cold2", "a", "1", "b", "2")
        self.client.execute_command("FLASH.DEBUG.DEMOTE", "cold2")
        result = self.client.execute_command("FLASH.HGETALL", "cold2")
        assert result == [b"a", b"1", b"b", b"2"]

    def test_hlen_after_demote_reads_field_count(self):
        self.client.execute_command("FLASH.HSET", "cold3", "x", "1", "y", "2")
        self.client.execute_command("FLASH.DEBUG.DEMOTE", "cold3")
        result = self.client.execute_command("FLASH.HLEN", "cold3")
        assert result == 2

    def test_hexists_after_demote_field_present(self):
        self.client.execute_command("FLASH.HSET", "cold4", "present", "v")
        self.client.execute_command("FLASH.DEBUG.DEMOTE", "cold4")
        result = self.client.execute_command("FLASH.HEXISTS", "cold4", "present")
        assert result == 1

    def test_hset_on_cold_key_promotes_and_updates(self):
        self.client.execute_command("FLASH.HSET", "cold5", "old_field", "old_val")
        self.client.execute_command("FLASH.DEBUG.DEMOTE", "cold5")
        self.client.execute_command("FLASH.HSET", "cold5", "new_field", "new_val")
        assert self.client.execute_command("FLASH.HGET", "cold5", "old_field") == b"old_val"
        assert self.client.execute_command("FLASH.HGET", "cold5", "new_field") == b"new_val"

    def test_hdel_on_cold_key_removes_field(self):
        self.client.execute_command("FLASH.HSET", "cold6", "keep", "k", "drop", "d")
        self.client.execute_command("FLASH.DEBUG.DEMOTE", "cold6")
        result = self.client.execute_command("FLASH.HDEL", "cold6", "drop")
        assert result == 1
        assert self.client.execute_command("FLASH.HEXISTS", "cold6", "drop") == 0
        assert self.client.execute_command("FLASH.HEXISTS", "cold6", "keep") == 1
