"""Integration tests for FLASH.EXISTS.

`FLASH.EXISTS key [key ...]` returns the count of keys that exist as one of
the four flash-tier types (FlashString, FlashHash, FlashList, FlashZSet).
Mirrors native `EXISTS` semantics: counts argument occurrences (not unique
keys), returns 0 for keys that don't exist or hold non-flash types, never
raises WRONGTYPE.
"""

import pytest
from valkey import ResponseError
from valkey_flash_test_case import ValkeyFlashTestCase


class TestFlashExists(ValkeyFlashTestCase):
    # ── Per data type ─────────────────────────────────────────────────────────

    def test_exists_flash_string_returns_one(self):
        self.client.execute_command("FLASH.SET", "ex:str", "v")
        assert self.client.execute_command("FLASH.EXISTS", "ex:str") == 1

    def test_exists_flash_hash_returns_one(self):
        self.client.execute_command("FLASH.HSET", "ex:hash", "f", "v")
        assert self.client.execute_command("FLASH.EXISTS", "ex:hash") == 1

    def test_exists_flash_list_returns_one(self):
        self.client.execute_command("FLASH.RPUSH", "ex:list", "v")
        assert self.client.execute_command("FLASH.EXISTS", "ex:list") == 1

    def test_exists_flash_zset_returns_one(self):
        self.client.execute_command("FLASH.ZADD", "ex:zset", "1", "m")
        assert self.client.execute_command("FLASH.EXISTS", "ex:zset") == 1

    # ── Absence ───────────────────────────────────────────────────────────────

    def test_exists_missing_key_returns_zero(self):
        assert self.client.execute_command("FLASH.EXISTS", "ex:never_set") == 0

    def test_exists_after_del_returns_zero(self):
        self.client.execute_command("FLASH.SET", "ex:gone", "v")
        self.client.execute_command("FLASH.DEL", "ex:gone")
        assert self.client.execute_command("FLASH.EXISTS", "ex:gone") == 0

    # ── Non-flash types contribute 0 (no WRONGTYPE raised) ────────────────────

    def test_exists_native_string_contributes_zero(self):
        # A native (non-flash) string set via plain SET should not count.
        self.client.execute_command("SET", "ex:native", "v")
        assert self.client.execute_command("FLASH.EXISTS", "ex:native") == 0

    def test_exists_mixes_flash_and_native_keys(self):
        self.client.execute_command("FLASH.SET", "ex:flash", "v")
        self.client.execute_command("SET", "ex:native", "v")
        # 1 (flash) + 0 (native) + 0 (missing) = 1
        result = self.client.execute_command("FLASH.EXISTS", "ex:flash", "ex:native", "ex:nope")
        assert result == 1

    def test_exists_native_string_does_not_raise_wrongtype(self):
        self.client.execute_command("SET", "ex:not_flash", "v")
        # Critically: returns 0, does NOT raise — distinct from FLASH.DEL
        # which raises WRONGTYPE on native keys.
        assert self.client.execute_command("FLASH.EXISTS", "ex:not_flash") == 0

    # ── Multi-key + duplicate semantics ───────────────────────────────────────

    def test_exists_counts_arguments_not_unique_keys(self):
        self.client.execute_command("FLASH.SET", "ex:dup", "v")
        # Same key passed three times → count is 3.
        assert self.client.execute_command("FLASH.EXISTS", "ex:dup", "ex:dup", "ex:dup") == 3

    def test_exists_partial_presence_returns_partial_count(self):
        self.client.execute_command("FLASH.SET", "ex:p1", "v")
        self.client.execute_command("FLASH.HSET", "ex:p2", "f", "v")
        # 2 of 4 exist as flash types.
        result = self.client.execute_command(
            "FLASH.EXISTS", "ex:p1", "ex:missing", "ex:p2", "ex:also_missing"
        )
        assert result == 2

    def test_exists_all_four_data_types_in_single_call(self):
        self.client.execute_command("FLASH.SET", "ex:s", "v")
        self.client.execute_command("FLASH.HSET", "ex:h", "f", "v")
        self.client.execute_command("FLASH.RPUSH", "ex:l", "x")
        self.client.execute_command("FLASH.ZADD", "ex:z", "1", "m")
        result = self.client.execute_command("FLASH.EXISTS", "ex:s", "ex:h", "ex:l", "ex:z")
        assert result == 4

    # ── Arity ─────────────────────────────────────────────────────────────────

    def test_exists_zero_args_raises_error(self):
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.EXISTS")
