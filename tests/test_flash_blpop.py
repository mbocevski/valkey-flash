"""Integration tests for FLASH.BLPOP, FLASH.BRPOP, FLASH.BLMOVE."""

import threading
import time

import pytest
from valkey import ResponseError
from valkey_flash_test_case import ValkeyFlashTestCase


class TestFlashBLPopFastPath(ValkeyFlashTestCase):
    """BLPOP/BRPOP when data is already available (fast path, no blocking)."""

    def test_blpop_fast_path_returns_key_and_element(self):
        self.client.execute_command("FLASH.RPUSH", "bfp1", "alpha")
        result = self.client.execute_command("FLASH.BLPOP", "bfp1", "0")
        assert result == [b"bfp1", b"alpha"]

    def test_brpop_fast_path_pops_from_tail(self):
        self.client.execute_command("FLASH.RPUSH", "bfp2", "x", "y", "z")
        result = self.client.execute_command("FLASH.BRPOP", "bfp2", "0")
        assert result == [b"bfp2", b"z"]

    def test_blpop_multi_key_first_populated_wins(self):
        self.client.execute_command("FLASH.RPUSH", "bfp_k2", "found")
        # k1 absent, k2 has data, k3 absent → k2 wins.
        result = self.client.execute_command("FLASH.BLPOP", "bfp_k1", "bfp_k2", "bfp_k3", "0")
        assert result == [b"bfp_k2", b"found"]

    def test_blpop_empties_list_deletes_key(self):
        self.client.execute_command("FLASH.RPUSH", "bfp3", "only")
        self.client.execute_command("FLASH.BLPOP", "bfp3", "0")
        assert self.client.execute_command("FLASH.LLEN", "bfp3") == 0

    def test_blpop_leaves_remaining_elements(self):
        self.client.execute_command("FLASH.RPUSH", "bfp4", "a", "b", "c")
        self.client.execute_command("FLASH.BLPOP", "bfp4", "0")
        remaining = self.client.execute_command("FLASH.LRANGE", "bfp4", "0", "-1")
        assert remaining == [b"b", b"c"]

    def test_blpop_wrongtype_error(self):
        self.client.execute_command("FLASH.HSET", "bfpht", "f", "v")
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.BLPOP", "bfpht", "0")

    def test_blpop_wrong_arity(self):
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.BLPOP", "k")

    def test_blpop_negative_timeout_error(self):
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.BLPOP", "k", "-1")

    def test_blpop_invalid_timeout_error(self):
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.BLPOP", "k", "notanumber")


class TestFlashBLPopTimeout(ValkeyFlashTestCase):
    """BLPOP/BRPOP when no data is available — timeout path."""

    def test_blpop_timeout_returns_nil(self):
        result = self.client.execute_command("FLASH.BLPOP", "bto1", "0.1")
        assert result is None

    def test_brpop_timeout_returns_nil(self):
        result = self.client.execute_command("FLASH.BRPOP", "bto2", "0.1")
        assert result is None


class TestFlashBLPopWakeUp(ValkeyFlashTestCase):
    """BLPOP/BRPOP unblocks when another client pushes data."""

    def test_blpop_wakes_on_lpush(self):
        results = {}

        def blocker():
            # Block with a 5-second timeout on a non-existent key.
            results["val"] = self.client.execute_command("FLASH.BLPOP", "bwake1", "5")

        t = threading.Thread(target=blocker, daemon=True)
        t.start()
        time.sleep(0.15)  # Give the thread time to block.

        # Push from a second connection.
        pusher = self.server.get_new_client()
        pusher.execute_command("FLASH.RPUSH", "bwake1", "hello")

        t.join(timeout=3.0)
        assert not t.is_alive(), "BLPOP did not unblock in time"
        assert results.get("val") == [b"bwake1", b"hello"]

    def test_brpop_wakes_on_rpush(self):
        results = {}

        def blocker():
            results["val"] = self.client.execute_command("FLASH.BRPOP", "bwake2", "5")

        t = threading.Thread(target=blocker, daemon=True)
        t.start()
        time.sleep(0.15)

        pusher = self.server.get_new_client()
        pusher.execute_command("FLASH.RPUSH", "bwake2", "world")

        t.join(timeout=3.0)
        assert not t.is_alive()
        assert results.get("val") == [b"bwake2", b"world"]

    def test_blpop_multi_key_wakes_on_correct_key(self):
        results = {}

        def blocker():
            results["val"] = self.client.execute_command(
                "FLASH.BLPOP", "bwk_absent", "bwk_target", "5"
            )

        t = threading.Thread(target=blocker, daemon=True)
        t.start()
        time.sleep(0.15)

        pusher = self.server.get_new_client()
        pusher.execute_command("FLASH.RPUSH", "bwk_target", "hit")

        t.join(timeout=3.0)
        assert not t.is_alive()
        assert results.get("val") == [b"bwk_target", b"hit"]


class TestFlashBLMoveFastPath(ValkeyFlashTestCase):
    """BLMOVE when source already has data (fast path, no blocking)."""

    def test_blmove_left_to_right(self):
        self.client.execute_command("FLASH.RPUSH", "bms1", "a", "b", "c")
        result = self.client.execute_command("FLASH.BLMOVE", "bms1", "bmd1", "LEFT", "RIGHT", "0")
        assert result == b"a"
        src = self.client.execute_command("FLASH.LRANGE", "bms1", "0", "-1")
        dst = self.client.execute_command("FLASH.LRANGE", "bmd1", "0", "-1")
        assert src == [b"b", b"c"]
        assert dst == [b"a"]

    def test_blmove_right_to_left(self):
        self.client.execute_command("FLASH.RPUSH", "bms2", "x", "y", "z")
        result = self.client.execute_command("FLASH.BLMOVE", "bms2", "bmd2", "RIGHT", "LEFT", "0")
        assert result == b"z"
        dst = self.client.execute_command("FLASH.LRANGE", "bmd2", "0", "-1")
        assert dst == [b"z"]

    def test_blmove_src_empty_after_single_element(self):
        self.client.execute_command("FLASH.RPUSH", "bms3", "only")
        self.client.execute_command("FLASH.BLMOVE", "bms3", "bmd3", "LEFT", "RIGHT", "0")
        assert self.client.execute_command("FLASH.LLEN", "bms3") == 0
        assert self.client.execute_command("FLASH.LLEN", "bmd3") == 1

    def test_blmove_timeout_returns_nil(self):
        result = self.client.execute_command(
            "FLASH.BLMOVE", "bm_absent", "bm_dst", "LEFT", "RIGHT", "0.1"
        )
        assert result is None

    def test_blmove_wrong_arity(self):
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.BLMOVE", "s", "d", "LEFT", "0")

    def test_blmove_invalid_direction(self):
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.BLMOVE", "s", "d", "UP", "RIGHT", "0")

    def test_blmove_negative_timeout_error(self):
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.BLMOVE", "s", "d", "LEFT", "RIGHT", "-1")


class TestFlashBLMoveWakeUp(ValkeyFlashTestCase):
    """BLMOVE unblocks when another client pushes to source."""

    def test_blmove_wakes_on_lpush(self):
        results = {}

        def blocker():
            results["val"] = self.client.execute_command(
                "FLASH.BLMOVE", "bmsrc", "bmdst", "LEFT", "RIGHT", "5"
            )

        t = threading.Thread(target=blocker, daemon=True)
        t.start()
        time.sleep(0.15)

        pusher = self.server.get_new_client()
        pusher.execute_command("FLASH.LPUSH", "bmsrc", "mover")

        t.join(timeout=3.0)
        assert not t.is_alive(), "BLMOVE did not unblock in time"
        assert results.get("val") == b"mover"
        dst = self.client.execute_command("FLASH.LRANGE", "bmdst", "0", "-1")
        assert dst == [b"mover"]

    def test_blmove_same_key_rotation_wakes_blpop(self):
        """BLMOVE src src (rotation) signals src — unblocks a BLPOP on the same key."""
        # Pre-populate src so BLMOVE fast-path succeeds and rotates.
        self.client.execute_command("FLASH.RPUSH", "bmrot", "a", "b", "c")

        results = {}

        def blocker():
            # Block on bmrot (it has elements, so this tests that the signal
            # fired by a same-key LMOVE wakes a second concurrent BLPOP).
            # Use a separate key that starts empty so we are truly blocking.
            results["val"] = self.client.execute_command("FLASH.BLPOP", "bmrot_empty", "5")

        t = threading.Thread(target=blocker, daemon=True)
        t.start()
        time.sleep(0.15)

        # LMOVE on a different key into bmrot_empty via a second connection.
        pusher = self.server.get_new_client()
        pusher.execute_command("FLASH.RPUSH", "bmrot_src2", "signal_elem")
        pusher.execute_command("FLASH.LMOVE", "bmrot_src2", "bmrot_empty", "LEFT", "RIGHT")

        t.join(timeout=3.0)
        assert not t.is_alive(), "BLPOP on dst did not unblock after FLASH.LMOVE into it"
        assert results.get("val") == [b"bmrot_empty", b"signal_elem"]


class TestFlashBlpopWakesOnLmove(ValkeyFlashTestCase):
    """BLPOP/BRPOP on a destination key wakes when FLASH.LMOVE pushes to it."""

    def test_blpop_wakes_when_lmove_pushes_to_dst(self):
        results = {}

        def blocker():
            results["val"] = self.client.execute_command("FLASH.BLPOP", "lmove_dst", "5")

        t = threading.Thread(target=blocker, daemon=True)
        t.start()
        time.sleep(0.15)

        mover = self.server.get_new_client()
        mover.execute_command("FLASH.RPUSH", "lmove_src", "payload")
        mover.execute_command("FLASH.LMOVE", "lmove_src", "lmove_dst", "LEFT", "RIGHT")

        t.join(timeout=3.0)
        assert not t.is_alive(), "BLPOP on dst did not unblock after FLASH.LMOVE"
        assert results.get("val") == [b"lmove_dst", b"payload"]

    def test_brpop_wakes_when_lmove_pushes_to_dst(self):
        results = {}

        def blocker():
            results["val"] = self.client.execute_command("FLASH.BRPOP", "lmove_dst2", "5")

        t = threading.Thread(target=blocker, daemon=True)
        t.start()
        time.sleep(0.15)

        mover = self.server.get_new_client()
        mover.execute_command("FLASH.RPUSH", "lmove_src2", "p2")
        mover.execute_command("FLASH.LMOVE", "lmove_src2", "lmove_dst2", "RIGHT", "LEFT")

        t.join(timeout=3.0)
        assert not t.is_alive(), "BRPOP on dst did not unblock after FLASH.LMOVE"
        assert results.get("val") == [b"lmove_dst2", b"p2"]

    def test_blpop_wakes_when_blmove_reply_cb_pushes_to_dst(self):
        """A blocked BLPOP on `bdst` wakes when BLMOVE's reply callback pushes to it."""
        results = {}

        def blpop_waiter():
            results["val"] = self.client.execute_command("FLASH.BLPOP", "blmove_chain_dst", "5")

        t = threading.Thread(target=blpop_waiter, daemon=True)
        t.start()
        time.sleep(0.15)

        # Block a BLMOVE on blmove_chain_src → blmove_chain_dst.
        blmove_results = {}

        def blmove_waiter():
            blmove_results["val"] = self.client.execute_command(
                "FLASH.BLMOVE", "blmove_chain_src", "blmove_chain_dst", "LEFT", "RIGHT", "5"
            )

        t2 = threading.Thread(target=blmove_waiter, daemon=True)
        t2.start()
        time.sleep(0.15)

        # Push to blmove_chain_src — wakes BLMOVE, which pushes to dst, which wakes BLPOP.
        pusher = self.server.get_new_client()
        pusher.execute_command("FLASH.RPUSH", "blmove_chain_src", "chained")

        t2.join(timeout=3.0)
        assert not t2.is_alive(), "BLMOVE did not unblock"
        assert blmove_results.get("val") == b"chained"

        t.join(timeout=3.0)
        assert not t.is_alive(), "BLPOP on dst did not unblock via BLMOVE chain"
        assert results.get("val") == [b"blmove_chain_dst", b"chained"]
