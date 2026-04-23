import os
import time

import pytest
from valkey import ResponseError
from valkey_flash_test_case import FlashReplicationTestCase, ValkeyFlashTestCase

# ── Helper ────────────────────────────────────────────────────────────────────


def _binaries_dir():
    return (
        f"{os.path.dirname(os.path.realpath(__file__))}"
        f"/build/binaries/{os.environ['SERVER_VERSION']}"
    )


def _server_path():
    return os.path.join(_binaries_dir(), "valkey-server")


def _prepend_lib_path():
    binaries_dir = _binaries_dir()
    existing = os.environ.get("LD_LIBRARY_PATH", "")
    os.environ["LD_LIBRARY_PATH"] = f"{binaries_dir}:{existing}" if existing else binaries_dir


def _server_args(extra=None, flash_path=None):
    if flash_path is None:
        import tempfile

        flash_path = os.path.join(tempfile.mkdtemp(prefix="flash_get_"), "flash.bin")
    args = {
        "enable-debug-command": "yes",
        "loadmodule": (f"{os.getenv('MODULE_PATH')} path {flash_path} capacity-bytes 16777216"),
    }
    if extra:
        args.update(extra)
    return args


# ── Basic FLASH.GET tests ─────────────────────────────────────────────────────


class TestFlashGet(ValkeyFlashTestCase):
    def test_get_after_set_returns_value(self):
        self.client.execute_command("FLASH.SET", "getkey", "hello")
        result = self.client.execute_command("FLASH.GET", "getkey")
        assert result == b"hello"

    def test_get_missing_key_returns_nil(self):
        result = self.client.execute_command("FLASH.GET", "nokey")
        assert result is None

    def test_get_overwritten_value(self):
        self.client.execute_command("FLASH.SET", "ow", "first")
        self.client.execute_command("FLASH.SET", "ow", "second")
        result = self.client.execute_command("FLASH.GET", "ow")
        assert result == b"second"

    def test_get_binary_value(self):
        raw = bytes(range(256))
        self.client.execute_command("FLASH.SET", "binkey", raw)
        result = self.client.execute_command("FLASH.GET", "binkey")
        assert result == raw

    # ── Malformed args ────────────────────────────────────────────────────────

    def test_no_args_rejected(self):
        try:
            self.client.execute_command("FLASH.GET")
            pytest.fail("Expected error")
        except ResponseError:
            pass

    # ── WRONGTYPE ─────────────────────────────────────────────────────────────

    def test_wrongtype_when_native_string_exists(self):
        self.client.execute_command("SET", "nativekey", "hello")
        try:
            self.client.execute_command("FLASH.GET", "nativekey")
            pytest.fail("Expected WRONGTYPE error")
        except ResponseError as e:
            assert "WRONGTYPE" in str(e)

    def test_wrongtype_when_list_exists(self):
        self.client.execute_command("RPUSH", "listkey", "a", "b")
        try:
            self.client.execute_command("FLASH.GET", "listkey")
            pytest.fail("Expected WRONGTYPE error")
        except ResponseError as e:
            assert "WRONGTYPE" in str(e)

    # ── TTL expiry ────────────────────────────────────────────────────────────

    def test_get_after_ttl_expiry_returns_nil(self):
        self.client.execute_command("FLASH.SET", "ttlkey", "val", "PX", "50")
        time.sleep(0.2)
        result = self.client.execute_command("FLASH.GET", "ttlkey")
        assert result is None

    def test_get_before_ttl_expiry_returns_value(self):
        self.client.execute_command("FLASH.SET", "livek", "alive", "EX", "60")
        result = self.client.execute_command("FLASH.GET", "livek")
        assert result == b"alive"

    # ── Hot-path latency ──────────────────────────────────────────────────────

    def test_get_hot_path_latency_below_10ms(self):
        self.client.execute_command("FLASH.SET", "latkey", "val")
        self.assert_latency_below_ms(self.client, "FLASH.GET latkey", threshold_ms=10)

    # ── Non-blocking on hot path ──────────────────────────────────────────────

    def test_get_does_not_block_event_loop(self):
        self.client.execute_command("FLASH.SET", "nbkey", "value")
        probe_client = self.server.get_new_client()

        def get_fn():
            self.client.execute_command("FLASH.GET", "nbkey")

        self.verify_nonblocking_during(self.client, get_fn, probe_client, "PING")


# ── Cache-eviction / re-promotion ─────────────────────────────────────────────
#
# Uses a server with a 1 MiB cache so that inserting ~2 MiB of data forces
# the earlier keys to be evicted from FlashCache while the FlashStringObject
# in Valkey's keyspace still holds Tier::Hot. FLASH.GET must re-promote and
# return the correct value.


class TestFlashGetEviction(ValkeyFlashTestCase):
    @pytest.fixture(autouse=True)
    def setup_test(self, setup):
        _prepend_lib_path()
        args = _server_args({"flash.cache-size-bytes": 1048576})  # 1 MiB minimum
        self.server, self.client = self.create_server(
            testdir=self.testdir,
            server_path=_server_path(),
            args=args,
        )

    def test_get_after_cache_eviction_re_promotes(self):
        # Insert 25 × 64 KiB entries = 1.6 MiB → exceeds the 1 MiB cache.
        # Earlier entries will be evicted from FlashCache but remain Hot in
        # the keyspace object; FLASH.GET must still return the correct bytes.
        chunk = b"x" * 65536  # 64 KiB
        keys = [f"evict:{i}" for i in range(25)]
        for k in keys:
            self.client.execute_command("FLASH.SET", k, chunk)

        # The first keys are the most likely to have been evicted.
        result = self.client.execute_command("FLASH.GET", "evict:0")
        assert result == chunk

    def test_get_after_eviction_key_is_findable_by_exists(self):
        chunk = b"y" * 65536
        for i in range(25):
            self.client.execute_command("FLASH.SET", f"ex2:{i}", chunk)
        assert self.client.execute_command("EXISTS", "ex2:0") == 1
        result = self.client.execute_command("FLASH.GET", "ex2:0")
        assert result == chunk


# ── Replication test ──────────────────────────────────────────────────────────


class TestFlashGetReplication(FlashReplicationTestCase):
    @pytest.fixture(autouse=True)
    def setup_test(self, setup):
        _prepend_lib_path()
        import shutil
        import tempfile

        self._flash_dir = os.path.abspath(
            tempfile.mkdtemp(prefix="flash_repl_get_", dir=self.testdir)
        )
        primary_path = os.path.join(self._flash_dir, "primary.bin")
        self.args = _server_args(flash_path=primary_path)
        self.server, self.client = self.create_server(
            testdir=self.testdir,
            server_path=_server_path(),
            args=self.args,
        )
        yield
        shutil.rmtree(self._flash_dir, ignore_errors=True)

    def setup_replication(self, num_replicas=1):
        """Override so each replica gets its own flash.bin path."""
        self.num_replicas = num_replicas
        self.replicas = []
        self.skip_teardown = False
        self.create_replicas(num_replicas)
        for i, replica in enumerate(self.replicas):
            replica_path = os.path.join(self._flash_dir, f"replica{i}.bin")
            replica.args["loadmodule"] = _server_args(flash_path=replica_path)["loadmodule"]
        self.start_replicas()
        self.wait_for_replicas(self.num_replicas)
        self.wait_for_primary_link_up_all_replicas()
        self.wait_for_all_replicas_online(self.num_replicas)
        for i in range(len(self.replicas)):
            self.waitForReplicaToSyncUp(self.replicas[i])
        return self.replicas

    def test_flash_get_on_replica_returns_value(self):
        self.setup_replication(num_replicas=1)
        self.client.execute_command("FLASH.SET", "repkey", "repvalue")
        self.waitForReplicaToSyncUp(self.replicas[0])
        result = self.replicas[0].client.execute_command("FLASH.GET", "repkey")
        assert result == b"repvalue"

    def test_flash_get_missing_on_replica_returns_nil(self):
        self.setup_replication(num_replicas=1)
        self.waitForReplicaToSyncUp(self.replicas[0])
        result = self.replicas[0].client.execute_command("FLASH.GET", "ghost")
        assert result is None
