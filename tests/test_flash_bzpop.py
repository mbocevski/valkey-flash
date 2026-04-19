"""Integration tests for FLASH.BZPOPMIN and FLASH.BZPOPMAX."""

import threading
import time

import pytest
from valkey import ResponseError
from valkey_flash_test_case import ValkeyFlashTestCase


class TestFlashBZPopMinFastPath(ValkeyFlashTestCase):
    """BZPOPMIN when data is already available (fast path, no blocking)."""

    def test_bzpopmin_returns_key_member_score(self):
        self.client.execute_command("FLASH.ZADD", "bz1", "1.5", "alpha")
        result = self.client.execute_command("FLASH.BZPOPMIN", "bz1", "0")
        assert result == [b"bz1", b"alpha", b"1.5"]

    def test_bzpopmin_pops_lowest_score(self):
        self.client.execute_command("FLASH.ZADD", "bz2", "10", "hi", "1", "lo", "5", "mid")
        result = self.client.execute_command("FLASH.BZPOPMIN", "bz2", "0")
        assert result == [b"bz2", b"lo", b"1"]

    def test_bzpopmax_pops_highest_score(self):
        self.client.execute_command("FLASH.ZADD", "bz3", "10", "hi", "1", "lo", "5", "mid")
        result = self.client.execute_command("FLASH.BZPOPMAX", "bz3", "0")
        assert result == [b"bz3", b"hi", b"10"]

    def test_bzpopmin_empties_set_deletes_key(self):
        self.client.execute_command("FLASH.ZADD", "bz4", "1", "only")
        self.client.execute_command("FLASH.BZPOPMIN", "bz4", "0")
        assert self.client.execute_command("FLASH.ZCARD", "bz4") == 0

    def test_bzpopmin_leaves_remaining_members(self):
        self.client.execute_command("FLASH.ZADD", "bz5", "1", "a", "2", "b", "3", "c")
        self.client.execute_command("FLASH.BZPOPMIN", "bz5", "0")
        assert self.client.execute_command("FLASH.ZCARD", "bz5") == 2
        assert self.client.execute_command("FLASH.ZSCORE", "bz5", "b") == b"2"

    def test_bzpopmax_leaves_remaining_members(self):
        self.client.execute_command("FLASH.ZADD", "bz6", "1", "a", "2", "b", "3", "c")
        self.client.execute_command("FLASH.BZPOPMAX", "bz6", "0")
        assert self.client.execute_command("FLASH.ZCARD", "bz6") == 2
        assert self.client.execute_command("FLASH.ZSCORE", "bz6", "b") == b"2"

    def test_bzpopmin_multi_key_first_populated_wins(self):
        self.client.execute_command("FLASH.ZADD", "bzmk2", "7", "x")
        result = self.client.execute_command(
            "FLASH.BZPOPMIN", "bzmk1", "bzmk2", "bzmk3", "0"
        )
        assert result == [b"bzmk2", b"x", b"7"]

    def test_bzpopmax_multi_key_first_populated_wins(self):
        self.client.execute_command("FLASH.ZADD", "bzmkx2", "42", "y")
        result = self.client.execute_command(
            "FLASH.BZPOPMAX", "bzmkx1", "bzmkx2", "bzmkx3", "0"
        )
        assert result == [b"bzmkx2", b"y", b"42"]

    def test_bzpopmin_integer_score_formatted(self):
        self.client.execute_command("FLASH.ZADD", "bzint", "5", "m")
        result = self.client.execute_command("FLASH.BZPOPMIN", "bzint", "0")
        assert result[2] == b"5"

    def test_bzpopmin_float_score_formatted(self):
        self.client.execute_command("FLASH.ZADD", "bzfloat", "3.14", "m")
        result = self.client.execute_command("FLASH.BZPOPMIN", "bzfloat", "0")
        assert result[2] == b"3.14"

    def test_bzpopmin_negative_score(self):
        self.client.execute_command("FLASH.ZADD", "bzneg", "-5", "neg", "0", "zero")
        result = self.client.execute_command("FLASH.BZPOPMIN", "bzneg", "0")
        assert result == [b"bzneg", b"neg", b"-5"]

    def test_bzpopmax_negative_score(self):
        self.client.execute_command("FLASH.ZADD", "bzneg2", "-5", "neg", "-10", "more_neg")
        result = self.client.execute_command("FLASH.BZPOPMAX", "bzneg2", "0")
        assert result == [b"bzneg2", b"neg", b"-5"]


class TestFlashBZPopErrors(ValkeyFlashTestCase):
    """Error cases for BZPOPMIN / BZPOPMAX."""

    def test_bzpopmin_wrong_arity(self):
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.BZPOPMIN", "k")

    def test_bzpopmax_wrong_arity(self):
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.BZPOPMAX", "k")

    def test_bzpopmin_negative_timeout(self):
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.BZPOPMIN", "k", "-1")

    def test_bzpopmax_negative_timeout(self):
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.BZPOPMAX", "k", "-1")

    def test_bzpopmin_invalid_timeout(self):
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.BZPOPMIN", "k", "notanumber")

    def test_bzpopmax_invalid_timeout(self):
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.BZPOPMAX", "k", "notanumber")

    def test_bzpopmin_wrongtype(self):
        self.client.execute_command("FLASH.HSET", "bzerr_ht", "f", "v")
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.BZPOPMIN", "bzerr_ht", "0")

    def test_bzpopmax_wrongtype(self):
        self.client.execute_command("FLASH.HSET", "bzerr_ht2", "f", "v")
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.BZPOPMAX", "bzerr_ht2", "0")


class TestFlashBZPopTimeout(ValkeyFlashTestCase):
    """BZPOPMIN/BZPOPMAX return nil on timeout."""

    def test_bzpopmin_timeout_returns_nil(self):
        result = self.client.execute_command("FLASH.BZPOPMIN", "bzt1", "0.1")
        assert result is None

    def test_bzpopmax_timeout_returns_nil(self):
        result = self.client.execute_command("FLASH.BZPOPMAX", "bzt2", "0.1")
        assert result is None


class TestFlashBZPopWakeUp(ValkeyFlashTestCase):
    """BZPOPMIN/BZPOPMAX unblock when another client calls ZADD."""

    def test_bzpopmin_wakes_on_zadd(self):
        results = {}

        def blocker():
            results["val"] = self.client.execute_command(
                "FLASH.BZPOPMIN", "bzwake1", "5"
            )

        t = threading.Thread(target=blocker, daemon=True)
        t.start()
        time.sleep(0.15)

        pusher = self.create_client()
        pusher.execute_command("FLASH.ZADD", "bzwake1", "3", "member")

        t.join(timeout=3.0)
        assert not t.is_alive(), "BZPOPMIN did not unblock in time"
        assert results.get("val") == [b"bzwake1", b"member", b"3"]

    def test_bzpopmax_wakes_on_zadd(self):
        results = {}

        def blocker():
            results["val"] = self.client.execute_command(
                "FLASH.BZPOPMAX", "bzwake2", "5"
            )

        t = threading.Thread(target=blocker, daemon=True)
        t.start()
        time.sleep(0.15)

        pusher = self.create_client()
        pusher.execute_command("FLASH.ZADD", "bzwake2", "99", "top")

        t.join(timeout=3.0)
        assert not t.is_alive(), "BZPOPMAX did not unblock in time"
        assert results.get("val") == [b"bzwake2", b"top", b"99"]

    def test_bzpopmin_multi_key_wakes_on_correct_key(self):
        results = {}

        def blocker():
            results["val"] = self.client.execute_command(
                "FLASH.BZPOPMIN", "bzwk_absent", "bzwk_target", "5"
            )

        t = threading.Thread(target=blocker, daemon=True)
        t.start()
        time.sleep(0.15)

        pusher = self.create_client()
        pusher.execute_command("FLASH.ZADD", "bzwk_target", "1", "item")

        t.join(timeout=3.0)
        assert not t.is_alive()
        assert results.get("val") == [b"bzwk_target", b"item", b"1"]

    def test_bzpopmin_wakes_pops_minimum_after_multiple_zadd(self):
        """Wake up, then among multiple members pop the min-score one."""
        results = {}

        def blocker():
            results["val"] = self.client.execute_command(
                "FLASH.BZPOPMIN", "bzwk_multi", "5"
            )

        t = threading.Thread(target=blocker, daemon=True)
        t.start()
        time.sleep(0.15)

        pusher = self.create_client()
        pusher.execute_command("FLASH.ZADD", "bzwk_multi", "10", "high", "2", "low")

        t.join(timeout=3.0)
        assert not t.is_alive()
        assert results.get("val") == [b"bzwk_multi", b"low", b"2"]

    def test_bzpopmax_wakes_pops_maximum(self):
        results = {}

        def blocker():
            results["val"] = self.client.execute_command(
                "FLASH.BZPOPMAX", "bzwk_max", "5"
            )

        t = threading.Thread(target=blocker, daemon=True)
        t.start()
        time.sleep(0.15)

        pusher = self.create_client()
        pusher.execute_command("FLASH.ZADD", "bzwk_max", "10", "high", "2", "low")

        t.join(timeout=3.0)
        assert not t.is_alive()
        assert results.get("val") == [b"bzwk_max", b"high", b"10"]
