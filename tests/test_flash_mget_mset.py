"""Integration tests for FLASH.MGET and FLASH.MSET.

The two batch commands close the v1.1.x performance gap that forced wrappers
to do per-key dispatch (N round-trips for an N-key bulk-fetch / bulk-write).

Semantics summary:

- FLASH.MGET key [key ...]
    Returns array of N entries. Each entry is the value bytes if the key
    exists as a FlashString, else nil. Non-string flash types, native keys,
    and missing keys all map to nil. Never raises WRONGTYPE.

- FLASH.MSET key value [key value ...]
    Atomic batched write. Hot-tier-only — values land in RAM immediately;
    auto-demotion handles eventual NVMe propagation. TTL cleared per key
    (matching native MSET). Phase 1 type-checks ALL keys; if any holds a
    non-flash-string type, the whole batch fails with WRONGTYPE — no
    partial writes. Batch size capped at 1024 keys per call.
"""

import pytest
from valkey import ResponseError
from valkey_flash_test_case import ValkeyFlashTestCase


class TestFlashMSet(ValkeyFlashTestCase):
    # ── Basic semantics ────────────────────────────────────────────────────────

    def test_mset_single_pair(self):
        result = self.client.execute_command("FLASH.MSET", "ms:k1", "v1")
        assert result == b"OK"
        assert self.client.execute_command("FLASH.GET", "ms:k1") == b"v1"

    def test_mset_three_pairs(self):
        result = self.client.execute_command(
            "FLASH.MSET", "ms:a", "1", "ms:b", "2", "ms:c", "3"
        )
        assert result == b"OK"
        assert self.client.execute_command("FLASH.GET", "ms:a") == b"1"
        assert self.client.execute_command("FLASH.GET", "ms:b") == b"2"
        assert self.client.execute_command("FLASH.GET", "ms:c") == b"3"

    def test_mset_overwrites_existing_flash_string(self):
        self.client.execute_command("FLASH.SET", "ms:over", "old")
        self.client.execute_command("FLASH.MSET", "ms:over", "new")
        assert self.client.execute_command("FLASH.GET", "ms:over") == b"new"

    def test_mset_clears_existing_ttl(self):
        # Native MSET clears TTL; FLASH.MSET matches.
        self.client.execute_command("FLASH.SET", "ms:ttl", "v", "EX", 60)
        assert self.client.execute_command("FLASH.TTL", "ms:ttl") > 0
        self.client.execute_command("FLASH.MSET", "ms:ttl", "v2")
        assert self.client.execute_command("FLASH.TTL", "ms:ttl") == -1

    # ── Type-check (atomicity under WRONGTYPE) ─────────────────────────────────

    def test_mset_wrongtype_against_flash_hash_aborts_whole_batch(self):
        self.client.execute_command("FLASH.HSET", "ms:hash_blocker", "f", "v")
        # Mix one good key with one wrong-type key. Whole batch must fail
        # with WRONGTYPE; no partial writes.
        with pytest.raises(ResponseError):
            self.client.execute_command(
                "FLASH.MSET", "ms:goodkey", "v", "ms:hash_blocker", "v"
            )
        # ms:goodkey was NEVER created.
        assert self.client.execute_command("FLASH.GET", "ms:goodkey") is None

    def test_mset_wrongtype_against_native_string_aborts_batch(self):
        self.client.execute_command("SET", "ms:native_blocker", "x")
        with pytest.raises(ResponseError):
            self.client.execute_command(
                "FLASH.MSET", "ms:other", "v", "ms:native_blocker", "v"
            )
        assert self.client.execute_command("FLASH.GET", "ms:other") is None

    # ── Arity ──────────────────────────────────────────────────────────────────

    def test_mset_zero_args_raises(self):
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.MSET")

    def test_mset_odd_args_missing_value_raises(self):
        with pytest.raises(ResponseError):
            # cmd, k1, v1, k2 → missing v2
            self.client.execute_command("FLASH.MSET", "ms:x", "v", "ms:y")

    def test_mset_batch_cap_enforced(self):
        # Build a batch one over the cap.
        pairs = []
        for i in range(1025):
            pairs.append(f"ms:cap_{i}")
            pairs.append("v")
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.MSET", *pairs)


class TestFlashMGet(ValkeyFlashTestCase):
    # ── Basic semantics ────────────────────────────────────────────────────────

    def test_mget_single_key(self):
        self.client.execute_command("FLASH.SET", "mg:k1", "v1")
        assert self.client.execute_command("FLASH.MGET", "mg:k1") == [b"v1"]

    def test_mget_three_keys(self):
        self.client.execute_command(
            "FLASH.MSET", "mg:a", "1", "mg:b", "2", "mg:c", "3"
        )
        result = self.client.execute_command("FLASH.MGET", "mg:a", "mg:b", "mg:c")
        assert result == [b"1", b"2", b"3"]

    def test_mget_preserves_argument_order(self):
        self.client.execute_command("FLASH.MSET", "mg:x", "X", "mg:y", "Y", "mg:z", "Z")
        result = self.client.execute_command("FLASH.MGET", "mg:z", "mg:x", "mg:y")
        assert result == [b"Z", b"X", b"Y"]

    # ── nil mapping ────────────────────────────────────────────────────────────

    def test_mget_missing_key_returns_nil(self):
        assert self.client.execute_command("FLASH.MGET", "mg:never") == [None]

    def test_mget_native_string_returns_nil(self):
        # Native key is NOT a FlashString — MGET returns nil for that slot.
        self.client.execute_command("SET", "mg:native", "x")
        assert self.client.execute_command("FLASH.MGET", "mg:native") == [None]

    def test_mget_flash_hash_returns_nil(self):
        # Non-string flash type also maps to nil (no WRONGTYPE).
        self.client.execute_command("FLASH.HSET", "mg:hash", "f", "v")
        assert self.client.execute_command("FLASH.MGET", "mg:hash") == [None]

    def test_mget_flash_list_returns_nil(self):
        self.client.execute_command("FLASH.RPUSH", "mg:list", "x")
        assert self.client.execute_command("FLASH.MGET", "mg:list") == [None]

    def test_mget_flash_zset_returns_nil(self):
        self.client.execute_command("FLASH.ZADD", "mg:zset", "1", "m")
        assert self.client.execute_command("FLASH.MGET", "mg:zset") == [None]

    def test_mget_mixed_existing_and_missing(self):
        self.client.execute_command("FLASH.MSET", "mg:p1", "P1", "mg:p3", "P3")
        result = self.client.execute_command(
            "FLASH.MGET", "mg:p1", "mg:missing", "mg:p3", "mg:also_missing"
        )
        assert result == [b"P1", None, b"P3", None]

    # ── Duplicate keys ─────────────────────────────────────────────────────────

    def test_mget_duplicate_key_returns_value_per_argument(self):
        self.client.execute_command("FLASH.SET", "mg:dup", "X")
        result = self.client.execute_command("FLASH.MGET", "mg:dup", "mg:dup", "mg:dup")
        assert result == [b"X", b"X", b"X"]

    # ── Arity ──────────────────────────────────────────────────────────────────

    def test_mget_zero_args_raises(self):
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.MGET")


class TestFlashMSetMGetRoundTrip(ValkeyFlashTestCase):
    """Confirms wrappers can use MSET + MGET as drop-in replacements for their
    per-key-dispatch implementations without observable behaviour change."""

    def test_round_trip_dozen_keys(self):
        keys = [f"rt:k_{i}" for i in range(12)]
        values = [f"v_{i}" for i in range(12)]

        # Bulk-write via MSET.
        flat = []
        for k, v in zip(keys, values, strict=False):
            flat.extend([k, v])
        assert self.client.execute_command("FLASH.MSET", *flat) == b"OK"

        # Bulk-fetch via MGET.
        result = self.client.execute_command("FLASH.MGET", *keys)
        assert result == [v.encode() for v in values]

    def test_round_trip_with_holes(self):
        # Set 3 keys, then fetch 5 (the middle 2 don't exist).
        self.client.execute_command("FLASH.MSET", "rt:a", "A", "rt:c", "C", "rt:e", "E")
        result = self.client.execute_command(
            "FLASH.MGET", "rt:a", "rt:b", "rt:c", "rt:d", "rt:e"
        )
        assert result == [b"A", None, b"C", None, b"E"]
