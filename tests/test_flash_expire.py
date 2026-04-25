"""Integration tests for the FLASH.EXPIRE family.

Nine commands cover the production-readiness gap that prevented session-cache
TTL-bump-on-activity workloads from running on flash:

- Mutators: FLASH.EXPIRE / FLASH.EXPIREAT / FLASH.PEXPIRE / FLASH.PEXPIREAT
- PERSIST: FLASH.PERSIST
- Readers: FLASH.TTL / FLASH.PTTL / FLASH.EXPIRETIME / FLASH.PEXPIRETIME

Tests verify each command across all four flash-tier types (FlashString,
FlashHash, FlashList, FlashZSet), the NX/XX/GT/LT flag predicates with their
"no current TTL = +∞" semantics, and the read-back via TTL/PTTL.
"""

import time

import pytest
from valkey import ResponseError
from valkey_flash_test_case import ValkeyFlashTestCase


def _now_ms() -> int:
    return int(time.time() * 1000)


class TestFlashExpireBasic(ValkeyFlashTestCase):
    # ── Per data type: setting a TTL that wasn't there ────────────────────────

    def test_expire_on_flash_string_returns_one_and_sets_ttl(self):
        self.client.execute_command("FLASH.SET", "ex:str", "v")
        assert self.client.execute_command("FLASH.EXPIRE", "ex:str", 60) == 1
        ttl = self.client.execute_command("FLASH.TTL", "ex:str")
        assert 55 <= ttl <= 60

    def test_expire_on_flash_hash_returns_one_and_sets_ttl(self):
        self.client.execute_command("FLASH.HSET", "ex:hash", "f", "v")
        assert self.client.execute_command("FLASH.EXPIRE", "ex:hash", 60) == 1
        assert 55 <= self.client.execute_command("FLASH.TTL", "ex:hash") <= 60

    def test_expire_on_flash_list_returns_one_and_sets_ttl(self):
        self.client.execute_command("FLASH.RPUSH", "ex:list", "x")
        assert self.client.execute_command("FLASH.EXPIRE", "ex:list", 60) == 1
        assert 55 <= self.client.execute_command("FLASH.TTL", "ex:list") <= 60

    def test_expire_on_flash_zset_returns_one_and_sets_ttl(self):
        self.client.execute_command("FLASH.ZADD", "ex:zset", "1", "m")
        assert self.client.execute_command("FLASH.EXPIRE", "ex:zset", 60) == 1
        assert 55 <= self.client.execute_command("FLASH.TTL", "ex:zset") <= 60

    # ── Missing / non-flash keys return 0, no error ───────────────────────────

    def test_expire_on_missing_key_returns_zero(self):
        assert self.client.execute_command("FLASH.EXPIRE", "ex:never_set", 60) == 0

    def test_expire_on_native_key_returns_zero_no_error(self):
        # Native EXPIRE on a native key works; FLASH.EXPIRE on a native key
        # should NOT raise, just return 0 — the flash module does not own
        # this key.
        self.client.execute_command("SET", "ex:native", "v")
        assert self.client.execute_command("FLASH.EXPIRE", "ex:native", 60) == 0
        # And the native key's lack of TTL is unchanged.
        assert self.client.execute_command("TTL", "ex:native") == -1


class TestFlashExpireUnits(ValkeyFlashTestCase):
    def test_expire_seconds(self):
        self.client.execute_command("FLASH.SET", "ux:1", "v")
        self.client.execute_command("FLASH.EXPIRE", "ux:1", 60)
        ttl_s = self.client.execute_command("FLASH.TTL", "ux:1")
        assert 55 <= ttl_s <= 60

    def test_pexpire_milliseconds(self):
        self.client.execute_command("FLASH.SET", "ux:2", "v")
        self.client.execute_command("FLASH.PEXPIRE", "ux:2", 60_000)
        ttl_ms = self.client.execute_command("FLASH.PTTL", "ux:2")
        assert 55_000 <= ttl_ms <= 60_000

    def test_expireat_absolute_seconds(self):
        self.client.execute_command("FLASH.SET", "ux:3", "v")
        target_secs = (_now_ms() // 1000) + 60
        self.client.execute_command("FLASH.EXPIREAT", "ux:3", target_secs)
        # ExpireTime returns absolute seconds.
        assert self.client.execute_command("FLASH.EXPIRETIME", "ux:3") == target_secs

    def test_pexpireat_absolute_milliseconds(self):
        self.client.execute_command("FLASH.SET", "ux:4", "v")
        target_ms = _now_ms() + 60_000
        self.client.execute_command("FLASH.PEXPIREAT", "ux:4", target_ms)
        assert self.client.execute_command("FLASH.PEXPIRETIME", "ux:4") == target_ms


class TestFlashTtlReaders(ValkeyFlashTestCase):
    def test_ttl_returns_negative_two_for_missing_key(self):
        assert self.client.execute_command("FLASH.TTL", "tr:never") == -2
        assert self.client.execute_command("FLASH.PTTL", "tr:never") == -2
        assert self.client.execute_command("FLASH.EXPIRETIME", "tr:never") == -2
        assert self.client.execute_command("FLASH.PEXPIRETIME", "tr:never") == -2

    def test_ttl_returns_negative_one_for_no_ttl(self):
        self.client.execute_command("FLASH.SET", "tr:no_ttl", "v")
        assert self.client.execute_command("FLASH.TTL", "tr:no_ttl") == -1
        assert self.client.execute_command("FLASH.PTTL", "tr:no_ttl") == -1
        assert self.client.execute_command("FLASH.EXPIRETIME", "tr:no_ttl") == -1
        assert self.client.execute_command("FLASH.PEXPIRETIME", "tr:no_ttl") == -1

    def test_ttl_pttl_units_consistent(self):
        self.client.execute_command("FLASH.SET", "tr:both", "v")
        self.client.execute_command("FLASH.EXPIRE", "tr:both", 60)
        ttl_s = self.client.execute_command("FLASH.TTL", "tr:both")
        ttl_ms = self.client.execute_command("FLASH.PTTL", "tr:both")
        # ttl_ms should be roughly ttl_s * 1000, with sub-second slop.
        assert ttl_s * 1000 <= ttl_ms + 1000
        assert ttl_ms <= (ttl_s + 1) * 1000


class TestFlashPersist(ValkeyFlashTestCase):
    def test_persist_clears_existing_ttl(self):
        self.client.execute_command("FLASH.SET", "p:1", "v", "EX", 60)
        # Confirm there's a TTL.
        assert self.client.execute_command("FLASH.TTL", "p:1") > 0
        # Clear it.
        assert self.client.execute_command("FLASH.PERSIST", "p:1") == 1
        # Now no TTL.
        assert self.client.execute_command("FLASH.TTL", "p:1") == -1

    def test_persist_returns_zero_when_no_ttl(self):
        self.client.execute_command("FLASH.SET", "p:2", "v")
        # No TTL set.
        assert self.client.execute_command("FLASH.PERSIST", "p:2") == 0

    def test_persist_returns_zero_when_missing_key(self):
        assert self.client.execute_command("FLASH.PERSIST", "p:never") == 0

    def test_persist_returns_zero_on_native_key(self):
        self.client.execute_command("SET", "p:native", "v", "EX", 60)
        assert self.client.execute_command("FLASH.PERSIST", "p:native") == 0


class TestFlashExpireFlags(ValkeyFlashTestCase):
    """NX / XX / GT / LT predicate semantics matching native Valkey 7.0+."""

    # ── NX: only set if no current TTL ────────────────────────────────────────

    def test_nx_sets_when_no_current_ttl(self):
        self.client.execute_command("FLASH.SET", "fl:nx1", "v")
        assert self.client.execute_command("FLASH.EXPIRE", "fl:nx1", 60, "NX") == 1
        assert self.client.execute_command("FLASH.TTL", "fl:nx1") > 0

    def test_nx_does_not_set_when_ttl_already_exists(self):
        self.client.execute_command("FLASH.SET", "fl:nx2", "v", "EX", 30)
        old_ttl = self.client.execute_command("FLASH.TTL", "fl:nx2")
        assert self.client.execute_command("FLASH.EXPIRE", "fl:nx2", 600, "NX") == 0
        new_ttl = self.client.execute_command("FLASH.TTL", "fl:nx2")
        # TTL essentially unchanged (allowing for 1-2 second tick).
        assert abs(new_ttl - old_ttl) <= 2

    # ── XX: only set if current TTL exists ────────────────────────────────────

    def test_xx_does_not_set_when_no_current_ttl(self):
        self.client.execute_command("FLASH.SET", "fl:xx1", "v")
        assert self.client.execute_command("FLASH.EXPIRE", "fl:xx1", 60, "XX") == 0
        # Still no TTL.
        assert self.client.execute_command("FLASH.TTL", "fl:xx1") == -1

    def test_xx_sets_when_ttl_already_exists(self):
        self.client.execute_command("FLASH.SET", "fl:xx2", "v", "EX", 30)
        assert self.client.execute_command("FLASH.EXPIRE", "fl:xx2", 600, "XX") == 1
        new_ttl = self.client.execute_command("FLASH.TTL", "fl:xx2")
        assert 590 <= new_ttl <= 600

    # ── GT: only set if new > current; no-current = +∞, so never sets ─────────

    def test_gt_does_not_set_when_no_current_ttl(self):
        # Native Valkey treats no-TTL as +∞: nothing > +∞, so GT must reject.
        self.client.execute_command("FLASH.SET", "fl:gt1", "v")
        assert self.client.execute_command("FLASH.EXPIRE", "fl:gt1", 60, "GT") == 0
        assert self.client.execute_command("FLASH.TTL", "fl:gt1") == -1

    def test_gt_sets_when_new_is_greater(self):
        self.client.execute_command("FLASH.SET", "fl:gt2", "v", "EX", 30)
        assert self.client.execute_command("FLASH.EXPIRE", "fl:gt2", 600, "GT") == 1
        assert 590 <= self.client.execute_command("FLASH.TTL", "fl:gt2") <= 600

    def test_gt_does_not_set_when_new_is_smaller(self):
        self.client.execute_command("FLASH.SET", "fl:gt3", "v", "EX", 600)
        assert self.client.execute_command("FLASH.EXPIRE", "fl:gt3", 30, "GT") == 0
        # TTL unchanged near 600.
        assert self.client.execute_command("FLASH.TTL", "fl:gt3") > 100

    # ── LT: only set if new < current; no-current = +∞, so always sets ────────

    def test_lt_sets_when_no_current_ttl(self):
        # Native Valkey: anything < +∞, so LT always succeeds.
        self.client.execute_command("FLASH.SET", "fl:lt1", "v")
        assert self.client.execute_command("FLASH.EXPIRE", "fl:lt1", 60, "LT") == 1
        assert self.client.execute_command("FLASH.TTL", "fl:lt1") > 0

    def test_lt_sets_when_new_is_smaller(self):
        self.client.execute_command("FLASH.SET", "fl:lt2", "v", "EX", 600)
        assert self.client.execute_command("FLASH.EXPIRE", "fl:lt2", 30, "LT") == 1
        assert self.client.execute_command("FLASH.TTL", "fl:lt2") <= 30

    def test_lt_does_not_set_when_new_is_larger(self):
        self.client.execute_command("FLASH.SET", "fl:lt3", "v", "EX", 30)
        assert self.client.execute_command("FLASH.EXPIRE", "fl:lt3", 600, "LT") == 0
        # TTL unchanged near 30.
        assert self.client.execute_command("FLASH.TTL", "fl:lt3") <= 30

    # ── Invalid flag rejected ─────────────────────────────────────────────────

    def test_unknown_flag_raises_error(self):
        self.client.execute_command("FLASH.SET", "fl:bad", "v")
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.EXPIRE", "fl:bad", 60, "WAT")


class TestFlashExpireBumpPattern(ValkeyFlashTestCase):
    """The session-cache bump-on-activity pattern that motivated this work."""

    def test_repeated_expire_extends_lifetime(self):
        # Set with short TTL.
        self.client.execute_command("FLASH.SET", "session:abc", "payload", "EX", 30)
        ttl_initial = self.client.execute_command("FLASH.TTL", "session:abc")
        assert ttl_initial <= 30

        # Bump TTL on activity — extend by a fresh 1800 seconds.
        assert self.client.execute_command("FLASH.EXPIRE", "session:abc", 1800) == 1
        ttl_bumped = self.client.execute_command("FLASH.TTL", "session:abc")
        assert ttl_bumped > 1700

        # The value is unchanged — bump did NOT rewrite the payload.
        assert self.client.execute_command("FLASH.GET", "session:abc") == b"payload"


class TestFlashExpireArity(ValkeyFlashTestCase):
    def test_expire_too_few_args(self):
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.EXPIRE", "k")

    def test_expire_too_many_args(self):
        with pytest.raises(ResponseError):
            # FLASH.EXPIRE accepts at most 4 args (key, value, optional flag).
            self.client.execute_command("FLASH.EXPIRE", "k", 60, "NX", "extra")

    def test_persist_too_many_args(self):
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.PERSIST", "k", "extra")

    def test_ttl_zero_args(self):
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.TTL")
