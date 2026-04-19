import pytest
from valkey import ResponseError
from valkey_flash_test_case import ValkeyFlashTestCase
from valkeytestframework.conftest import resource_port_tracker


class TestFlashCopyString(ValkeyFlashTestCase):

    def test_copy_hot_string_dst_has_correct_value(self):
        client = self.client
        client.execute_command("FLASH.SET", "src_str", "hello_world")
        result = client.execute_command("COPY", "src_str", "dst_str")
        assert result == 1
        assert client.execute_command("FLASH.GET", "dst_str") == b"hello_world"

    def test_copy_hot_string_src_unchanged(self):
        client = self.client
        client.execute_command("FLASH.SET", "src_str2", "original")
        client.execute_command("COPY", "src_str2", "dst_str2")
        assert client.execute_command("FLASH.GET", "src_str2") == b"original"

    def test_copy_hot_string_dst_is_independent(self):
        """Mutating dst must not affect src and vice versa."""
        client = self.client
        client.execute_command("FLASH.SET", "src_ind", "shared_value")
        client.execute_command("COPY", "src_ind", "dst_ind")
        client.execute_command("FLASH.SET", "dst_ind", "modified")
        assert client.execute_command("FLASH.GET", "src_ind") == b"shared_value"
        assert client.execute_command("FLASH.GET", "dst_ind") == b"modified"

    def test_copy_missing_src_returns_zero(self):
        client = self.client
        result = client.execute_command("COPY", "nonexistent_src", "any_dst")
        assert result == 0

    def test_copy_to_existing_dst_without_replace_returns_zero(self):
        client = self.client
        client.execute_command("FLASH.SET", "src_exist", "a")
        client.execute_command("FLASH.SET", "dst_exist", "b")
        result = client.execute_command("COPY", "src_exist", "dst_exist")
        assert result == 0
        # Original dst value must be preserved.
        assert client.execute_command("FLASH.GET", "dst_exist") == b"b"

    def test_copy_to_existing_dst_with_replace_overwrites(self):
        client = self.client
        client.execute_command("FLASH.SET", "src_repl", "new_val")
        client.execute_command("FLASH.SET", "dst_repl", "old_val")
        result = client.execute_command("COPY", "src_repl", "dst_repl", "REPLACE")
        assert result == 1
        assert client.execute_command("FLASH.GET", "dst_repl") == b"new_val"

    def test_copy_flash_string_over_native_string_fails(self):
        """COPY from FLASH.STRING to a key holding a native STRING type is rejected."""
        client = self.client
        client.execute_command("FLASH.SET", "flash_src", "v")
        client.execute_command("SET", "native_dst", "n")
        # Without REPLACE, must return 0 (key exists, no overwrite).
        result = client.execute_command("COPY", "flash_src", "native_dst")
        assert result == 0

    def test_copy_cross_db(self):
        """COPY src dst DB 1 must create the key in db 1."""
        client = self.client
        client.execute_command("FLASH.SET", "src_xdb", "cross_db_val")
        result = client.execute_command("COPY", "src_xdb", "dst_xdb", "DB", "1")
        assert result == 1
        # Verify in db 1.
        client.execute_command("SELECT", "1")
        assert client.execute_command("FLASH.GET", "dst_xdb") == b"cross_db_val"
        # Restore to db 0.
        client.execute_command("SELECT", "0")


class TestFlashCopyHash(ValkeyFlashTestCase):

    def test_copy_hot_hash_dst_has_correct_fields(self):
        client = self.client
        client.execute_command("FLASH.HSET", "src_hash", "f1", "v1", "f2", "v2")
        result = client.execute_command("COPY", "src_hash", "dst_hash")
        assert result == 1
        assert client.execute_command("FLASH.HGET", "dst_hash", "f1") == b"v1"
        assert client.execute_command("FLASH.HGET", "dst_hash", "f2") == b"v2"

    def test_copy_hot_hash_src_unchanged(self):
        client = self.client
        client.execute_command("FLASH.HSET", "src_hash2", "key", "val")
        client.execute_command("COPY", "src_hash2", "dst_hash2")
        assert client.execute_command("FLASH.HGET", "src_hash2", "key") == b"val"

    def test_copy_hot_hash_dst_is_independent(self):
        """Modifying dst hash must not change src hash."""
        client = self.client
        client.execute_command("FLASH.HSET", "src_hind", "field", "original")
        client.execute_command("COPY", "src_hind", "dst_hind")
        client.execute_command("FLASH.HSET", "dst_hind", "field", "changed")
        assert client.execute_command("FLASH.HGET", "src_hind", "field") == b"original"
        assert client.execute_command("FLASH.HGET", "dst_hind", "field") == b"changed"

    def test_copy_hash_missing_src_returns_zero(self):
        result = self.client.execute_command("COPY", "no_src_hash", "no_dst_hash")
        assert result == 0

    def test_copy_hash_to_existing_without_replace_returns_zero(self):
        client = self.client
        client.execute_command("FLASH.HSET", "src_hex", "f", "v")
        client.execute_command("FLASH.HSET", "dst_hex", "f", "old")
        result = client.execute_command("COPY", "src_hex", "dst_hex")
        assert result == 0
        assert client.execute_command("FLASH.HGET", "dst_hex", "f") == b"old"

    def test_copy_hash_with_replace(self):
        client = self.client
        client.execute_command("FLASH.HSET", "src_hrep", "field", "new_val")
        client.execute_command("FLASH.HSET", "dst_hrep", "field", "old_val")
        result = client.execute_command("COPY", "src_hrep", "dst_hrep", "REPLACE")
        assert result == 1
        assert client.execute_command("FLASH.HGET", "dst_hrep", "field") == b"new_val"


class TestFlashCopyList(ValkeyFlashTestCase):

    def test_copy_hot_list_dst_has_correct_elements(self):
        client = self.client
        client.execute_command("FLASH.RPUSH", "src_list", "a", "b", "c")
        result = client.execute_command("COPY", "src_list", "dst_list")
        assert result == 1
        assert client.execute_command("FLASH.LLEN", "dst_list") == 3
        assert client.execute_command("FLASH.LINDEX", "dst_list", 0) == b"a"
        assert client.execute_command("FLASH.LINDEX", "dst_list", 2) == b"c"

    def test_copy_hot_list_src_unchanged(self):
        client = self.client
        client.execute_command("FLASH.RPUSH", "src_list2", "x", "y")
        client.execute_command("COPY", "src_list2", "dst_list2")
        assert client.execute_command("FLASH.LLEN", "src_list2") == 2

    def test_copy_hot_list_dst_is_independent(self):
        """Pushing to dst must not affect src."""
        client = self.client
        client.execute_command("FLASH.RPUSH", "src_lind", "original")
        client.execute_command("COPY", "src_lind", "dst_lind")
        client.execute_command("FLASH.RPUSH", "dst_lind", "extra")
        assert client.execute_command("FLASH.LLEN", "src_lind") == 1
        assert client.execute_command("FLASH.LLEN", "dst_lind") == 2

    def test_copy_list_to_existing_without_replace_returns_zero(self):
        client = self.client
        client.execute_command("FLASH.RPUSH", "src_lex", "v")
        client.execute_command("FLASH.RPUSH", "dst_lex", "old")
        result = client.execute_command("COPY", "src_lex", "dst_lex")
        assert result == 0
        assert client.execute_command("FLASH.LLEN", "dst_lex") == 1

    def test_copy_list_with_replace(self):
        client = self.client
        client.execute_command("FLASH.RPUSH", "src_lrep", "new")
        client.execute_command("FLASH.RPUSH", "dst_lrep", "old1", "old2")
        result = client.execute_command("COPY", "src_lrep", "dst_lrep", "REPLACE")
        assert result == 1
        assert client.execute_command("FLASH.LLEN", "dst_lrep") == 1
        assert client.execute_command("FLASH.LINDEX", "dst_lrep", 0) == b"new"


class TestFlashCopyZSet(ValkeyFlashTestCase):

    def test_copy_hot_zset_dst_has_correct_members(self):
        client = self.client
        client.execute_command("FLASH.ZADD", "src_zset", "1.0", "a", "2.0", "b")
        result = client.execute_command("COPY", "src_zset", "dst_zset")
        assert result == 1
        assert client.execute_command("FLASH.ZCARD", "dst_zset") == 2
        assert float(client.execute_command("FLASH.ZSCORE", "dst_zset", "a")) == 1.0
        assert float(client.execute_command("FLASH.ZSCORE", "dst_zset", "b")) == 2.0

    def test_copy_hot_zset_src_unchanged(self):
        client = self.client
        client.execute_command("FLASH.ZADD", "src_zset2", "5.0", "m")
        client.execute_command("COPY", "src_zset2", "dst_zset2")
        assert client.execute_command("FLASH.ZCARD", "src_zset2") == 1

    def test_copy_hot_zset_dst_is_independent(self):
        """Adding to dst must not affect src."""
        client = self.client
        client.execute_command("FLASH.ZADD", "src_zind", "1.0", "x")
        client.execute_command("COPY", "src_zind", "dst_zind")
        client.execute_command("FLASH.ZADD", "dst_zind", "99.0", "extra")
        assert client.execute_command("FLASH.ZCARD", "src_zind") == 1
        assert client.execute_command("FLASH.ZCARD", "dst_zind") == 2

    def test_copy_zset_to_existing_without_replace_returns_zero(self):
        client = self.client
        client.execute_command("FLASH.ZADD", "src_zex", "1.0", "a")
        client.execute_command("FLASH.ZADD", "dst_zex", "2.0", "b")
        result = client.execute_command("COPY", "src_zex", "dst_zex")
        assert result == 0
        assert client.execute_command("FLASH.ZCARD", "dst_zex") == 1

    def test_copy_zset_with_replace(self):
        client = self.client
        client.execute_command("FLASH.ZADD", "src_zrep", "3.0", "new_m")
        client.execute_command("FLASH.ZADD", "dst_zrep", "1.0", "old_m")
        result = client.execute_command("COPY", "src_zrep", "dst_zrep", "REPLACE")
        assert result == 1
        assert client.execute_command("FLASH.ZCARD", "dst_zrep") == 1
        assert client.execute_command("FLASH.ZSCORE", "dst_zrep", "old_m") is None
        assert float(client.execute_command("FLASH.ZSCORE", "dst_zrep", "new_m")) == 3.0
