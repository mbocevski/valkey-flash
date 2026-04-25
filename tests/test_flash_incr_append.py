"""Integration tests for FLASH.INCR / INCRBY / DECR / DECRBY / APPEND.

These five commands close the v1.1.x production-readiness gap that prevented
counter / rate-limiter / append-log workloads from running on the flash tier
at all. Wrappers' Bucket B previously returned WrapperError::FlashArithmeticUnsupported
for these; with the module commands shipped, wrappers can route real
flash-tier counter ops without falling back to native.

Coverage:
- Initialisation on missing key (INCR → "1", APPEND → length of provided value).
- Strict integer parsing (whitespace, decimals, alpha all rejected).
- i64 boundary handling: MAX + 1 → ERR; MIN - 1 → ERR.
- DECRBY with i64::MIN delta is rejected up front.
- Type guards: WRONGTYPE on FlashHash/FlashList/FlashZSet/native types.
- TTL preservation across the read-modify-write.
- APPEND on a missing key sets the value and returns its length.
"""

import pytest
from valkey import ResponseError
from valkey_flash_test_case import ValkeyFlashTestCase


class TestFlashIncr(ValkeyFlashTestCase):
    def test_incr_initialises_missing_key_to_one(self):
        result = self.client.execute_command("FLASH.INCR", "ic:new")
        assert result == 1
        assert self.client.execute_command("FLASH.GET", "ic:new") == b"1"

    def test_incr_increments_existing_value(self):
        self.client.execute_command("FLASH.SET", "ic:five", "5")
        assert self.client.execute_command("FLASH.INCR", "ic:five") == 6
        assert self.client.execute_command("FLASH.GET", "ic:five") == b"6"

    def test_incr_at_max_overflows(self):
        self.client.execute_command("FLASH.SET", "ic:max", str(2**63 - 1))
        with pytest.raises(ResponseError, match="overflow"):
            self.client.execute_command("FLASH.INCR", "ic:max")
        # Value unchanged.
        assert self.client.execute_command("FLASH.GET", "ic:max") == str(
            2**63 - 1
        ).encode()

    def test_incr_on_non_integer_value_errors(self):
        self.client.execute_command("FLASH.SET", "ic:bad", "abc")
        with pytest.raises(ResponseError, match="not an integer"):
            self.client.execute_command("FLASH.INCR", "ic:bad")

    def test_incr_arity(self):
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.INCR")
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.INCR", "k", "extra")


class TestFlashIncrby(ValkeyFlashTestCase):
    def test_incrby_positive_delta(self):
        self.client.execute_command("FLASH.SET", "ib:c", "10")
        assert self.client.execute_command("FLASH.INCRBY", "ib:c", 5) == 15

    def test_incrby_negative_delta(self):
        self.client.execute_command("FLASH.SET", "ib:c2", "10")
        assert self.client.execute_command("FLASH.INCRBY", "ib:c2", -3) == 7

    def test_incrby_zero_delta(self):
        self.client.execute_command("FLASH.SET", "ib:c3", "42")
        assert self.client.execute_command("FLASH.INCRBY", "ib:c3", 0) == 42

    def test_incrby_initialises_missing_to_zero(self):
        assert self.client.execute_command("FLASH.INCRBY", "ib:fresh", 100) == 100

    def test_incrby_overflow_errors(self):
        self.client.execute_command("FLASH.SET", "ib:near", str(2**63 - 10))
        with pytest.raises(ResponseError, match="overflow"):
            self.client.execute_command("FLASH.INCRBY", "ib:near", 100)

    def test_incrby_non_integer_delta_rejected(self):
        with pytest.raises(ResponseError, match="not an integer"):
            self.client.execute_command("FLASH.INCRBY", "ib:k", "1.5")


class TestFlashDecr(ValkeyFlashTestCase):
    def test_decr_initialises_missing_to_minus_one(self):
        # Native DECR on missing key sets value to "-1" and returns -1.
        assert self.client.execute_command("FLASH.DECR", "dc:new") == -1
        assert self.client.execute_command("FLASH.GET", "dc:new") == b"-1"

    def test_decr_decrements_existing_value(self):
        self.client.execute_command("FLASH.SET", "dc:ten", "10")
        assert self.client.execute_command("FLASH.DECR", "dc:ten") == 9

    def test_decr_at_min_overflows(self):
        self.client.execute_command("FLASH.SET", "dc:min", str(-(2**63)))
        with pytest.raises(ResponseError, match="overflow"):
            self.client.execute_command("FLASH.DECR", "dc:min")


class TestFlashDecrby(ValkeyFlashTestCase):
    def test_decrby_positive_delta_subtracts(self):
        self.client.execute_command("FLASH.SET", "db:c", "20")
        assert self.client.execute_command("FLASH.DECRBY", "db:c", 5) == 15

    def test_decrby_negative_delta_adds(self):
        # DECRBY -3 means add 3 (matches native).
        self.client.execute_command("FLASH.SET", "db:c2", "20")
        assert self.client.execute_command("FLASH.DECRBY", "db:c2", -3) == 23

    def test_decrby_i64_min_rejected(self):
        # i64::MIN can't be negated — our handler rejects it up front.
        self.client.execute_command("FLASH.SET", "db:c3", "0")
        with pytest.raises(ResponseError, match="not an integer"):
            self.client.execute_command("FLASH.DECRBY", "db:c3", str(-(2**63)))


class TestFlashIncrAppendTypeGuards(ValkeyFlashTestCase):
    def test_incr_on_flash_hash_wrongtype(self):
        self.client.execute_command("FLASH.HSET", "ic:hash", "f", "v")
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.INCR", "ic:hash")

    def test_incr_on_flash_list_wrongtype(self):
        self.client.execute_command("FLASH.RPUSH", "ic:list", "x")
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.INCR", "ic:list")

    def test_incr_on_flash_zset_wrongtype(self):
        self.client.execute_command("FLASH.ZADD", "ic:zset", 1, "m")
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.INCR", "ic:zset")

    def test_incr_on_native_string_wrongtype(self):
        # Native string is NOT a flash type — refuse rather than mutate it.
        self.client.execute_command("SET", "ic:native", "5")
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.INCR", "ic:native")

    def test_append_on_flash_hash_wrongtype(self):
        self.client.execute_command("FLASH.HSET", "ap:hash", "f", "v")
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.APPEND", "ap:hash", "more")


class TestFlashIncrTtlPreservation(ValkeyFlashTestCase):
    def test_incr_preserves_existing_ttl(self):
        self.client.execute_command("FLASH.SET", "ttl:c", "10", "EX", 600)
        old_ttl = self.client.execute_command("TTL", "ttl:c")
        self.client.execute_command("FLASH.INCR", "ttl:c")
        new_ttl = self.client.execute_command("TTL", "ttl:c")
        # TTL within 2 seconds of original (allowing for tick lag).
        assert abs(new_ttl - old_ttl) <= 2

    def test_decr_preserves_existing_ttl(self):
        self.client.execute_command("FLASH.SET", "ttl:d", "10", "EX", 600)
        old_ttl = self.client.execute_command("TTL", "ttl:d")
        self.client.execute_command("FLASH.DECR", "ttl:d")
        new_ttl = self.client.execute_command("TTL", "ttl:d")
        assert abs(new_ttl - old_ttl) <= 2

    def test_append_preserves_existing_ttl(self):
        self.client.execute_command("FLASH.SET", "ttl:a", "hello", "EX", 600)
        old_ttl = self.client.execute_command("TTL", "ttl:a")
        self.client.execute_command("FLASH.APPEND", "ttl:a", " world")
        new_ttl = self.client.execute_command("TTL", "ttl:a")
        assert abs(new_ttl - old_ttl) <= 2

    def test_incr_initialised_key_has_no_ttl(self):
        # Native INCR on missing key creates a fresh key without TTL.
        self.client.execute_command("FLASH.INCR", "ttl:fresh")
        assert self.client.execute_command("TTL", "ttl:fresh") == -1


class TestFlashAppend(ValkeyFlashTestCase):
    def test_append_to_missing_key_initialises(self):
        result = self.client.execute_command("FLASH.APPEND", "ap:new", "hello")
        # Returns new length.
        assert result == 5
        assert self.client.execute_command("FLASH.GET", "ap:new") == b"hello"

    def test_append_to_existing_extends(self):
        self.client.execute_command("FLASH.SET", "ap:exists", "hello")
        result = self.client.execute_command("FLASH.APPEND", "ap:exists", " world")
        assert result == 11
        assert self.client.execute_command("FLASH.GET", "ap:exists") == b"hello world"

    def test_append_empty_string_returns_existing_length(self):
        self.client.execute_command("FLASH.SET", "ap:e", "abc")
        assert self.client.execute_command("FLASH.APPEND", "ap:e", "") == 3
        assert self.client.execute_command("FLASH.GET", "ap:e") == b"abc"

    def test_append_repeated_extends_each_time(self):
        self.client.execute_command("FLASH.SET", "ap:r", "")
        self.client.execute_command("FLASH.APPEND", "ap:r", "a")
        self.client.execute_command("FLASH.APPEND", "ap:r", "b")
        result = self.client.execute_command("FLASH.APPEND", "ap:r", "c")
        assert result == 3
        assert self.client.execute_command("FLASH.GET", "ap:r") == b"abc"

    def test_append_arity(self):
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.APPEND", "k")
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.APPEND")
