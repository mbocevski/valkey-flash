import time

import pytest
from valkey import ResponseError
from valkey_flash_test_case import ValkeyFlashTestCase


class TestFlashHSetTTL(ValkeyFlashTestCase):
    # ── EX / PX / EXAT / PXAT ────────────────────────────────────────────────

    def test_hset_ex_key_expires(self):
        client = self.client
        client.execute_command("FLASH.HSET", "ex_key", "f", "v", "EX", "1")
        assert client.execute_command("FLASH.HGET", "ex_key", "f") == b"v"
        self.wait_for_key_expiry(client, "ex_key", timeout_s=4)
        assert client.execute_command("FLASH.HGET", "ex_key", "f") is None

    def test_hset_px_key_expires(self):
        client = self.client
        client.execute_command("FLASH.HSET", "px_key", "f", "v", "PX", "500")
        assert client.execute_command("FLASH.HGET", "px_key", "f") == b"v"
        self.wait_for_key_expiry(client, "px_key", timeout_s=3)
        assert client.execute_command("FLASH.HGET", "px_key", "f") is None

    def test_hset_exat_sets_ttl(self):
        client = self.client
        future_s = int(time.time()) + 60
        client.execute_command("FLASH.HSET", "exat_key", "f", "v", "EXAT", str(future_s))
        ttl = client.execute_command("PTTL", "exat_key")
        assert 0 < ttl <= 60_000 + 1000, f"expected TTL ~60s, got {ttl}ms"

    def test_hset_pxat_sets_ttl(self):
        client = self.client
        future_ms = int(time.time() * 1000) + 60_000
        client.execute_command("FLASH.HSET", "pxat_key", "f", "v", "PXAT", str(future_ms))
        ttl = client.execute_command("PTTL", "pxat_key")
        assert 0 < ttl <= 60_000 + 1000, f"expected TTL ~60s, got {ttl}ms"

    # ── KEEPTTL ───────────────────────────────────────────────────────────────

    def test_hset_keepttl_preserves_existing_ttl(self):
        client = self.client
        client.execute_command("FLASH.HSET", "kttl_key", "f1", "v1", "PX", "30000")
        ttl_before = client.execute_command("PTTL", "kttl_key")
        assert ttl_before > 0

        client.execute_command("FLASH.HSET", "kttl_key", "f2", "v2", "KEEPTTL")
        ttl_after = client.execute_command("PTTL", "kttl_key")
        # TTL should still be positive and roughly the same (within 1s drift).
        assert ttl_after > 0
        assert abs(ttl_before - ttl_after) < 1500, (
            f"KEEPTTL changed TTL unexpectedly: before={ttl_before} after={ttl_after}"
        )
        assert client.execute_command("FLASH.HGET", "kttl_key", "f1") == b"v1"
        assert client.execute_command("FLASH.HGET", "kttl_key", "f2") == b"v2"

    def test_hset_no_ttl_flag_preserves_existing_ttl(self):
        """Default HSET semantics: omitting TTL option must not clear existing TTL."""
        client = self.client
        client.execute_command("FLASH.HSET", "nottl_key", "f1", "v1", "PX", "30000")
        ttl_before = client.execute_command("PTTL", "nottl_key")
        assert ttl_before > 0

        # Re-HSET with no TTL flag — TTL must be preserved.
        client.execute_command("FLASH.HSET", "nottl_key", "f2", "v2")
        ttl_after = client.execute_command("PTTL", "nottl_key")
        assert ttl_after > 0
        assert abs(ttl_before - ttl_after) < 1500, (
            f"missing TTL flag cleared existing TTL: before={ttl_before} after={ttl_after}"
        )

    def test_hset_ex_replaces_existing_ttl(self):
        """EX option must replace a previously set TTL."""
        client = self.client
        client.execute_command("FLASH.HSET", "repl_ttl", "f", "v", "PX", "100000")
        client.execute_command("FLASH.HSET", "repl_ttl", "f", "v2", "EX", "5")
        ttl = client.execute_command("PTTL", "repl_ttl")
        assert 0 < ttl <= 5500, f"expected TTL ≤5s after EX override, got {ttl}ms"

    # ── BGREWRITEAOF confirms ttl_ms is set ───────────────────────────────────

    def test_hset_ttl_survives_aof_rewrite(self):
        """AOF rewrite of a FLASH.HSET key with EX must preserve the TTL."""
        client = self.client
        client.execute_command("FLASH.HSET", "aof_ttl_key", "f", "v", "EX", "60")
        ttl_before = client.execute_command("PTTL", "aof_ttl_key")
        assert ttl_before > 0

        client.execute_command("BGREWRITEAOF")
        # Allow rewrite to start; value should still be readable with TTL.
        time.sleep(0.1)
        assert client.execute_command("FLASH.HGET", "aof_ttl_key", "f") == b"v"
        ttl_after = client.execute_command("PTTL", "aof_ttl_key")
        assert ttl_after > 0, "TTL was lost after BGREWRITEAOF"

    # ── Malformed args ────────────────────────────────────────────────────────

    def test_hset_ex_zero_is_error(self):
        with pytest.raises(ResponseError, match="invalid expire time"):
            self.client.execute_command("FLASH.HSET", "err_key", "f", "v", "EX", "0")

    def test_hset_ex_negative_is_error(self):
        with pytest.raises(ResponseError, match="invalid expire time"):
            self.client.execute_command("FLASH.HSET", "err_key", "f", "v", "EX", "-1")

    def test_hset_ex_missing_value_is_error(self):
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.HSET", "err_key", "f", "v", "EX")

    def test_hset_keepttl_and_ex_is_error(self):
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.HSET", "err_key", "f", "v", "KEEPTTL", "EX", "10")

    def test_hset_odd_fv_with_ttl_is_error(self):
        with pytest.raises((ResponseError, Exception)):
            self.client.execute_command("FLASH.HSET", "err_key", "f", "EX", "10")

    # ── WRONGTYPE ─────────────────────────────────────────────────────────────

    def test_hset_ttl_on_native_hash_is_wrongtype(self):
        client = self.client
        client.execute_command("HSET", "native_h", "f", "v")
        with pytest.raises(ResponseError, match="WRONGTYPE"):
            client.execute_command("FLASH.HSET", "native_h", "f", "v2", "EX", "10")
