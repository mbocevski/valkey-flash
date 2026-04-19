import time

import pytest
from valkey import ResponseError
from valkey_flash_test_case import ValkeyFlashTestCase
from valkeytestframework.conftest import resource_port_tracker


def _try_enable_activedefrag(client):
    """Enable active-defrag with lowest possible thresholds.

    Returns True if the server accepted the config, False if activedefrag is
    unsupported (e.g. built without jemalloc) so the caller can skip.
    """
    try:
        client.execute_command("CONFIG", "SET", "activedefrag", "yes")
        client.execute_command(
            "CONFIG", "SET", "active-defrag-ignore-bytes", "0"
        )
        client.execute_command(
            "CONFIG", "SET", "active-defrag-threshold-lower", "0"
        )
        return True
    except ResponseError:
        return False


class TestFlashDefragString(ValkeyFlashTestCase):

    def test_string_data_intact_with_activedefrag_enabled(self):
        """FLASH.SET values must be readable after activedefrag runs."""
        client = self.client
        n = 200

        for i in range(n):
            client.execute_command("FLASH.SET", f"dfstr_{i}", f"val_{i}" * 8)

        defrag_available = _try_enable_activedefrag(client)

        # Fragment memory: delete even keys then re-insert to force allocations
        # into the gaps.
        for i in range(0, n, 2):
            client.execute_command("DEL", f"dfstr_{i}")
        for i in range(0, n, 2):
            client.execute_command("FLASH.SET", f"dfstr_{i}", f"val_{i}" * 8)

        if defrag_available:
            time.sleep(0.15)

        for i in range(n):
            result = client.execute_command("FLASH.GET", f"dfstr_{i}")
            expected = (f"val_{i}" * 8).encode()
            assert result == expected, (
                f"dfstr_{i} corrupted after defrag: got {result!r}"
            )

    def test_string_server_survives_repeated_writes_with_activedefrag(self):
        """Server must not crash under sustained write pressure with defrag on."""
        client = self.client
        _try_enable_activedefrag(client)

        for i in range(500):
            client.execute_command("FLASH.SET", f"stress_{i}", "x" * 64)
            if i % 50 == 0:
                client.execute_command("DEL", *[f"stress_{j}" for j in range(i, min(i + 10, 500))])

        # Server is still alive if this succeeds.
        assert client.execute_command("PING") == b"PONG"


class TestFlashDefragHash(ValkeyFlashTestCase):

    def test_hash_data_intact_with_activedefrag_enabled(self):
        """FLASH.HSET field values must be readable after activedefrag runs."""
        client = self.client
        n = 100
        fields_per_key = 5

        for i in range(n):
            args = []
            for j in range(fields_per_key):
                args += [f"field_{j}", f"hval_{i}_{j}" * 4]
            client.execute_command("FLASH.HSET", f"dfhash_{i}", *args)

        defrag_available = _try_enable_activedefrag(client)

        # Fragment memory.
        for i in range(0, n, 2):
            client.execute_command("DEL", f"dfhash_{i}")
        for i in range(0, n, 2):
            args = []
            for j in range(fields_per_key):
                args += [f"field_{j}", f"hval_{i}_{j}" * 4]
            client.execute_command("FLASH.HSET", f"dfhash_{i}", *args)

        if defrag_available:
            time.sleep(0.15)

        for i in range(n):
            for j in range(fields_per_key):
                result = client.execute_command(
                    "FLASH.HGET", f"dfhash_{i}", f"field_{j}"
                )
                expected = (f"hval_{i}_{j}" * 4).encode()
                assert result == expected, (
                    f"dfhash_{i}[field_{j}] corrupted after defrag: got {result!r}"
                )

    def test_hash_server_survives_repeated_writes_with_activedefrag(self):
        """Server must not crash under sustained hash write pressure with defrag on."""
        client = self.client
        _try_enable_activedefrag(client)

        for i in range(300):
            client.execute_command(
                "FLASH.HSET", f"hstress_{i}", "f1", "a" * 32, "f2", "b" * 32
            )
            if i % 30 == 0:
                client.execute_command(
                    "DEL", *[f"hstress_{k}" for k in range(i, min(i + 5, 300))]
                )

        assert client.execute_command("PING") == b"PONG"


class TestFlashDefragList(ValkeyFlashTestCase):

    def test_list_data_intact_with_activedefrag_enabled(self):
        """FLASH.RPUSH values must be readable after activedefrag runs."""
        client = self.client
        n = 100

        for i in range(n):
            client.execute_command(
                "FLASH.RPUSH", f"dflist_{i}", *[f"item_{j}" * 4 for j in range(5)]
            )

        defrag_available = _try_enable_activedefrag(client)

        for i in range(0, n, 2):
            client.execute_command("DEL", f"dflist_{i}")
        for i in range(0, n, 2):
            client.execute_command(
                "FLASH.RPUSH", f"dflist_{i}", *[f"item_{j}" * 4 for j in range(5)]
            )

        if defrag_available:
            time.sleep(0.15)

        for i in range(n):
            assert client.execute_command("FLASH.LLEN", f"dflist_{i}") == 5
            assert client.execute_command("FLASH.LINDEX", f"dflist_{i}", 0) == (
                "item_0" * 4
            ).encode()

    def test_list_server_survives_repeated_writes_with_activedefrag(self):
        """Server must not crash under sustained list write pressure with defrag on."""
        client = self.client
        _try_enable_activedefrag(client)

        for i in range(300):
            client.execute_command("FLASH.RPUSH", f"lstress_{i}", "x" * 64, "y" * 64)
            if i % 30 == 0:
                client.execute_command(
                    "DEL", *[f"lstress_{k}" for k in range(i, min(i + 5, 300))]
                )

        assert client.execute_command("PING") == b"PONG"


class TestFlashDefragZSet(ValkeyFlashTestCase):

    def test_zset_data_intact_with_activedefrag_enabled(self):
        """FLASH.ZADD members and scores must be readable after activedefrag runs."""
        client = self.client
        n = 100

        for i in range(n):
            pairs = []
            for j in range(5):
                pairs += [str(float(j)), f"member_{i}_{j}" * 3]
            client.execute_command("FLASH.ZADD", f"dfzset_{i}", *pairs)

        defrag_available = _try_enable_activedefrag(client)

        for i in range(0, n, 2):
            client.execute_command("DEL", f"dfzset_{i}")
        for i in range(0, n, 2):
            pairs = []
            for j in range(5):
                pairs += [str(float(j)), f"member_{i}_{j}" * 3]
            client.execute_command("FLASH.ZADD", f"dfzset_{i}", *pairs)

        if defrag_available:
            time.sleep(0.15)

        for i in range(n):
            assert client.execute_command("FLASH.ZCARD", f"dfzset_{i}") == 5
            score = client.execute_command(
                "FLASH.ZSCORE", f"dfzset_{i}", f"member_{i}_2" * 3
            )
            assert float(score) == 2.0, (
                f"dfzset_{i}[member_{i}_2] corrupted after defrag: got {score!r}"
            )

    def test_zset_server_survives_repeated_writes_with_activedefrag(self):
        """Server must not crash under sustained zset write pressure with defrag on."""
        client = self.client
        _try_enable_activedefrag(client)

        for i in range(300):
            client.execute_command(
                "FLASH.ZADD", f"zstress_{i}", "1.0", "a" * 32, "2.0", "b" * 32
            )
            if i % 30 == 0:
                client.execute_command(
                    "DEL", *[f"zstress_{k}" for k in range(i, min(i + 5, 300))]
                )

        assert client.execute_command("PING") == b"PONG"
