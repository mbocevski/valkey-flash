"""Integration tests for FLASH.COPY / FLASH.RENAME / FLASH.RENAMENX.

These commands close the v1.1.x atomicity gap that forced wrappers' Bucket C
into non-atomic GET+SET+DEL fallbacks for cross-tier moves. Module-side
primitives are atomic from the client's viewpoint by virtue of single-tick
command execution.

Coverage:

- All 4 flash-tier types (FlashString / FlashHash / FlashList / FlashZSet).
- Same-type and cross-type rename (e.g. RENAME hashlikedst-replacing-string).
- TTL preservation from src to dst.
- Cluster-cross-slot detection (delegated to Valkey core; we don't test
  here — ARCH:200 covers the underlying mechanism).
- Arity errors and unknown-keyword rejection.
- COPY semantics: no-REPLACE returns 0 when dst exists; REPLACE overwrites.
- RENAMENX semantics: returns 0 when dst exists.
"""

import pytest
from valkey import ResponseError
from valkey_flash_test_case import ValkeyFlashTestCase


class TestFlashRenameByType(ValkeyFlashTestCase):
    """RENAME exercise across the four flash types."""

    def test_rename_flash_string(self):
        self.client.execute_command("FLASH.SET", "rn:s_src", "value")
        result = self.client.execute_command("FLASH.RENAME", "rn:s_src", "rn:s_dst")
        assert result == b"OK"
        assert self.client.execute_command("FLASH.GET", "rn:s_dst") == b"value"
        assert self.client.execute_command("FLASH.GET", "rn:s_src") is None

    def test_rename_flash_hash(self):
        self.client.execute_command("FLASH.HSET", "rn:h_src", "f1", "v1", "f2", "v2")
        result = self.client.execute_command("FLASH.RENAME", "rn:h_src", "rn:h_dst")
        assert result == b"OK"
        # Verify hash content moved.
        assert self.client.execute_command("FLASH.HGET", "rn:h_dst", "f1") == b"v1"
        assert self.client.execute_command("FLASH.HGET", "rn:h_dst", "f2") == b"v2"
        assert self.client.execute_command("FLASH.HEXISTS", "rn:h_src", "f1") == 0

    def test_rename_flash_list(self):
        self.client.execute_command("FLASH.RPUSH", "rn:l_src", "a", "b", "c")
        result = self.client.execute_command("FLASH.RENAME", "rn:l_src", "rn:l_dst")
        assert result == b"OK"
        assert self.client.execute_command("FLASH.LRANGE", "rn:l_dst", 0, -1) == [
            b"a",
            b"b",
            b"c",
        ]
        assert self.client.execute_command("FLASH.LLEN", "rn:l_src") == 0

    def test_rename_flash_zset(self):
        self.client.execute_command("FLASH.ZADD", "rn:z_src", 1, "alpha", 2, "beta")
        result = self.client.execute_command("FLASH.RENAME", "rn:z_src", "rn:z_dst")
        assert result == b"OK"
        assert self.client.execute_command("FLASH.ZSCORE", "rn:z_dst", "alpha") == b"1"
        assert self.client.execute_command("FLASH.ZSCORE", "rn:z_dst", "beta") == b"2"
        assert self.client.execute_command("FLASH.ZSCORE", "rn:z_src", "alpha") is None


class TestFlashRenameTtlAndOverwrite(ValkeyFlashTestCase):
    def test_rename_preserves_ttl(self):
        self.client.execute_command("FLASH.SET", "rn:ttl_src", "v", "EX", 600)
        old_ttl = self.client.execute_command("FLASH.TTL", "rn:ttl_src")
        self.client.execute_command("FLASH.RENAME", "rn:ttl_src", "rn:ttl_dst")
        new_ttl = self.client.execute_command("FLASH.TTL", "rn:ttl_dst")
        # New TTL is within 2s of the old one (allow for tick lag during rename).
        assert abs(new_ttl - old_ttl) <= 2

    def test_rename_overwrites_existing_dst(self):
        self.client.execute_command("FLASH.SET", "rn:ovw_src", "new")
        self.client.execute_command("FLASH.SET", "rn:ovw_dst", "old")
        self.client.execute_command("FLASH.RENAME", "rn:ovw_src", "rn:ovw_dst")
        assert self.client.execute_command("FLASH.GET", "rn:ovw_dst") == b"new"
        assert self.client.execute_command("FLASH.GET", "rn:ovw_src") is None

    def test_rename_cross_type_replaces_dst(self):
        # src is FlashString; dst is FlashHash; after RENAME dst is FlashString.
        self.client.execute_command("FLASH.SET", "rn:xt_src", "scalar")
        self.client.execute_command("FLASH.HSET", "rn:xt_dst", "f", "v")
        self.client.execute_command("FLASH.RENAME", "rn:xt_src", "rn:xt_dst")
        # dst is now a string (HGET should reflect a wrong-type-ish situation,
        # but FLASH.HGET on a non-hash key returns nil per existing semantics).
        assert self.client.execute_command("FLASH.GET", "rn:xt_dst") == b"scalar"


class TestFlashRenameErrors(ValkeyFlashTestCase):
    def test_rename_missing_src_raises(self):
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.RENAME", "rn:never", "rn:any")

    def test_rename_native_src_raises(self):
        # Native (non-flash) src — same as missing from this module's view.
        self.client.execute_command("SET", "rn:native_src", "v")
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.RENAME", "rn:native_src", "rn:any")

    def test_rename_arity_too_few(self):
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.RENAME", "k")

    def test_rename_arity_too_many(self):
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.RENAME", "k", "k2", "extra")


class TestFlashRenameNX(ValkeyFlashTestCase):
    def test_renamenx_succeeds_when_dst_absent(self):
        self.client.execute_command("FLASH.SET", "nx:src", "v")
        assert self.client.execute_command("FLASH.RENAMENX", "nx:src", "nx:dst") == 1
        assert self.client.execute_command("FLASH.GET", "nx:dst") == b"v"
        assert self.client.execute_command("FLASH.GET", "nx:src") is None

    def test_renamenx_returns_zero_when_dst_exists(self):
        self.client.execute_command("FLASH.SET", "nx:src2", "src_val")
        self.client.execute_command("FLASH.SET", "nx:dst2", "dst_val")
        assert self.client.execute_command("FLASH.RENAMENX", "nx:src2", "nx:dst2") == 0
        # No side effect.
        assert self.client.execute_command("FLASH.GET", "nx:src2") == b"src_val"
        assert self.client.execute_command("FLASH.GET", "nx:dst2") == b"dst_val"

    def test_renamenx_dst_exists_as_native_blocks(self):
        # Native dst should also block the rename.
        self.client.execute_command("FLASH.SET", "nx:src3", "v")
        self.client.execute_command("SET", "nx:dst3_native", "blocker")
        assert self.client.execute_command("FLASH.RENAMENX", "nx:src3", "nx:dst3_native") == 0
        assert self.client.execute_command("FLASH.GET", "nx:src3") == b"v"

    def test_renamenx_missing_src_raises(self):
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.RENAMENX", "nx:never", "nx:any")


class TestFlashCopyByType(ValkeyFlashTestCase):
    """COPY across the four types — value duplicated, src preserved."""

    def test_copy_flash_string(self):
        self.client.execute_command("FLASH.SET", "cp:s_src", "value")
        assert self.client.execute_command("FLASH.COPY", "cp:s_src", "cp:s_dst") == 1
        # src AND dst both have the value.
        assert self.client.execute_command("FLASH.GET", "cp:s_src") == b"value"
        assert self.client.execute_command("FLASH.GET", "cp:s_dst") == b"value"

    def test_copy_flash_hash(self):
        self.client.execute_command("FLASH.HSET", "cp:h_src", "f", "v")
        assert self.client.execute_command("FLASH.COPY", "cp:h_src", "cp:h_dst") == 1
        assert self.client.execute_command("FLASH.HGET", "cp:h_src", "f") == b"v"
        assert self.client.execute_command("FLASH.HGET", "cp:h_dst", "f") == b"v"

    def test_copy_flash_list(self):
        self.client.execute_command("FLASH.RPUSH", "cp:l_src", "x", "y", "z")
        assert self.client.execute_command("FLASH.COPY", "cp:l_src", "cp:l_dst") == 1
        assert self.client.execute_command("FLASH.LRANGE", "cp:l_src", 0, -1) == [
            b"x",
            b"y",
            b"z",
        ]
        assert self.client.execute_command("FLASH.LRANGE", "cp:l_dst", 0, -1) == [
            b"x",
            b"y",
            b"z",
        ]

    def test_copy_flash_zset(self):
        self.client.execute_command("FLASH.ZADD", "cp:z_src", 1.5, "m")
        assert self.client.execute_command("FLASH.COPY", "cp:z_src", "cp:z_dst") == 1
        assert self.client.execute_command("FLASH.ZSCORE", "cp:z_src", "m") == b"1.5"
        assert self.client.execute_command("FLASH.ZSCORE", "cp:z_dst", "m") == b"1.5"


class TestFlashCopySemantics(ValkeyFlashTestCase):
    def test_copy_returns_zero_when_dst_exists_no_replace(self):
        self.client.execute_command("FLASH.SET", "cp:nr_src", "src_val")
        self.client.execute_command("FLASH.SET", "cp:nr_dst", "dst_val")
        assert self.client.execute_command("FLASH.COPY", "cp:nr_src", "cp:nr_dst") == 0
        # No side effect.
        assert self.client.execute_command("FLASH.GET", "cp:nr_dst") == b"dst_val"

    def test_copy_with_replace_overwrites(self):
        self.client.execute_command("FLASH.SET", "cp:rep_src", "src_val")
        self.client.execute_command("FLASH.SET", "cp:rep_dst", "dst_val")
        assert self.client.execute_command("FLASH.COPY", "cp:rep_src", "cp:rep_dst", "REPLACE") == 1
        assert self.client.execute_command("FLASH.GET", "cp:rep_dst") == b"src_val"
        # src still present.
        assert self.client.execute_command("FLASH.GET", "cp:rep_src") == b"src_val"

    def test_copy_replace_case_insensitive(self):
        self.client.execute_command("FLASH.SET", "cp:ci_src", "v")
        self.client.execute_command("FLASH.SET", "cp:ci_dst", "old")
        assert self.client.execute_command("FLASH.COPY", "cp:ci_src", "cp:ci_dst", "replace") == 1
        assert self.client.execute_command("FLASH.GET", "cp:ci_dst") == b"v"

    def test_copy_returns_zero_when_src_missing(self):
        # Match native COPY: missing src → 0, no error.
        assert self.client.execute_command("FLASH.COPY", "cp:never", "cp:any") == 0

    def test_copy_returns_zero_when_src_native(self):
        # Native (non-flash) src — return 0 (same as missing).
        self.client.execute_command("SET", "cp:native_src", "v")
        assert self.client.execute_command("FLASH.COPY", "cp:native_src", "cp:any") == 0

    def test_copy_unknown_keyword_raises(self):
        self.client.execute_command("FLASH.SET", "cp:bad_src", "v")
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.COPY", "cp:bad_src", "cp:bad_dst", "REPLACE_TYPO")

    def test_copy_preserves_ttl(self):
        self.client.execute_command("FLASH.SET", "cp:ttl_src", "v", "EX", 600)
        self.client.execute_command("FLASH.COPY", "cp:ttl_src", "cp:ttl_dst")
        ttl_src = self.client.execute_command("FLASH.TTL", "cp:ttl_src")
        ttl_dst = self.client.execute_command("FLASH.TTL", "cp:ttl_dst")
        assert ttl_src > 0 and ttl_dst > 0
        assert abs(ttl_src - ttl_dst) <= 2

    def test_copy_arity_too_few(self):
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.COPY", "k")
