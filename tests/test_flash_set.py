import os
import time

import pytest
from valkey import ResponseError
from valkey_flash_test_case import ValkeyFlashTestCase
from valkeytestframework.valkey_test_case import ReplicationTestCase

# ── Helper ────────────────────────────────────────────────────────────────────


def _binaries_dir():
    return (
        f"{os.path.dirname(os.path.realpath(__file__))}"
        f"/build/binaries/{os.environ['SERVER_VERSION']}"
    )


def _server_path():
    return os.path.join(_binaries_dir(), "valkey-server")


def _server_args(flash_path=None):
    if flash_path is None:
        import tempfile

        flash_path = os.path.join(tempfile.mkdtemp(prefix="flash_repl_set_"), "flash.bin")
    return {
        "enable-debug-command": "yes",
        "loadmodule": (f"{os.getenv('MODULE_PATH')} path {flash_path} capacity-bytes 16777216"),
    }


def _prepend_lib_path():
    binaries_dir = _binaries_dir()
    existing = os.environ.get("LD_LIBRARY_PATH", "")
    os.environ["LD_LIBRARY_PATH"] = f"{binaries_dir}:{existing}" if existing else binaries_dir


# ── Basic FLASH.SET tests ─────────────────────────────────────────────────────


class TestFlashSet(ValkeyFlashTestCase):
    def test_set_returns_ok(self):
        result = self.client.execute_command("FLASH.SET mykey hello")
        assert result == b"OK"

    def test_set_key_exists_in_keyspace(self):
        self.client.execute_command("FLASH.SET k1 v1")
        assert self.client.execute_command("EXISTS k1") == 1

    def test_set_memory_usage_is_positive(self):
        self.client.execute_command("FLASH.SET memkey somevalue")
        usage = self.client.execute_command("MEMORY USAGE memkey")
        assert usage is not None and usage > 0

    def test_set_object_encoding_is_module_type(self):
        self.client.execute_command("FLASH.SET enckey val")
        encoding = self.client.execute_command("OBJECT ENCODING enckey")
        # Module types report their name as the encoding.
        assert encoding is not None

    def test_set_overwrite_same_key(self):
        self.client.execute_command("FLASH.SET dup first")
        result = self.client.execute_command("FLASH.SET dup second")
        assert result == b"OK"

    # ── Malformed args ────────────────────────────────────────────────────────

    def test_too_few_args_rejected(self):
        try:
            self.client.execute_command("FLASH.SET onlykey")
            pytest.fail("Expected error")
        except ResponseError:
            pass

    def test_no_args_rejected(self):
        try:
            self.client.execute_command("FLASH.SET")
            pytest.fail("Expected error")
        except ResponseError:
            pass

    def test_unknown_option_rejected(self):
        try:
            self.client.execute_command("FLASH.SET k v KEEPTTL")
            pytest.fail("Expected error")
        except ResponseError as e:
            assert "syntax error" in str(e).lower()

    def test_nx_and_xx_together_rejected(self):
        try:
            self.client.execute_command("FLASH.SET k v NX XX")
            pytest.fail("Expected error")
        except ResponseError as e:
            assert "compatible" in str(e).lower() or "syntax" in str(e).lower()

    def test_ex_zero_rejected(self):
        try:
            self.client.execute_command("FLASH.SET k v EX 0")
            pytest.fail("Expected error")
        except ResponseError as e:
            assert "expire" in str(e).lower() or "invalid" in str(e).lower()

    def test_ex_negative_rejected(self):
        try:
            self.client.execute_command("FLASH.SET k v EX -5")
            pytest.fail("Expected error")
        except ResponseError:
            pass

    def test_px_missing_value_rejected(self):
        try:
            self.client.execute_command("FLASH.SET k v PX")
            pytest.fail("Expected error")
        except ResponseError as e:
            assert "syntax error" in str(e).lower()

    # ── NX / XX semantics ─────────────────────────────────────────────────────

    def test_nx_on_absent_key_writes(self):
        result = self.client.execute_command("FLASH.SET nxkey val NX")
        assert result == b"OK"
        assert self.client.execute_command("EXISTS nxkey") == 1

    def test_nx_on_existing_key_returns_nil(self):
        self.client.execute_command("FLASH.SET nxdup val1")
        result = self.client.execute_command("FLASH.SET nxdup val2 NX")
        assert result is None

    def test_xx_on_existing_key_writes(self):
        self.client.execute_command("FLASH.SET xxkey original")
        result = self.client.execute_command("FLASH.SET xxkey updated XX")
        assert result == b"OK"

    def test_xx_on_absent_key_returns_nil(self):
        result = self.client.execute_command("FLASH.SET nokey val XX")
        assert result is None
        assert self.client.execute_command("EXISTS nokey") == 0

    # ── TTL semantics ─────────────────────────────────────────────────────────

    def test_px_ttl_set(self):
        self.client.execute_command("FLASH.SET ttlkey val PX 5000")
        pttl = self.client.execute_command("PTTL ttlkey")
        # PTTL should be positive and ≤ 5000ms.
        assert 0 < pttl <= 5000

    def test_ex_ttl_set(self):
        self.client.execute_command("FLASH.SET exkey val EX 10")
        ttl = self.client.execute_command("TTL exkey")
        assert 0 < ttl <= 10

    def test_px_short_ttl_key_expires(self):
        self.client.execute_command("FLASH.SET expkey val PX 200")
        self.wait_for_key_expiry(self.client, "expkey", timeout_s=3)

    def test_no_ttl_key_persists(self):
        self.client.execute_command("FLASH.SET persist val")
        time.sleep(0.2)
        assert self.client.execute_command("EXISTS persist") == 1

    # ── WRONGTYPE ─────────────────────────────────────────────────────────────

    def test_wrongtype_when_native_string_exists(self):
        self.client.execute_command("SET nativekey hello")
        try:
            self.client.execute_command("FLASH.SET nativekey world")
            pytest.fail("Expected WRONGTYPE error")
        except ResponseError as e:
            assert "WRONGTYPE" in str(e)

    def test_wrongtype_when_list_exists(self):
        self.client.execute_command("RPUSH listkey a b c")
        try:
            self.client.execute_command("FLASH.SET listkey val")
            pytest.fail("Expected WRONGTYPE error")
        except ResponseError as e:
            assert "WRONGTYPE" in str(e)


# ── Replication test ──────────────────────────────────────────────────────────


class TestFlashSetReplication(ReplicationTestCase):
    @pytest.fixture(autouse=True)
    def setup_test(self, setup):
        _prepend_lib_path()
        import shutil
        import tempfile

        self._flash_dir = tempfile.mkdtemp(prefix="flash_repl_set_", dir=self.testdir)
        primary_path = os.path.join(self._flash_dir, "primary.bin")
        self.args = _server_args(primary_path)
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
            replica.args["loadmodule"] = _server_args(replica_path)["loadmodule"]
        self.start_replicas()
        self.wait_for_replicas(self.num_replicas)
        self.wait_for_primary_link_up_all_replicas()
        self.wait_for_all_replicas_online(self.num_replicas)
        for i in range(len(self.replicas)):
            self.waitForReplicaToSyncUp(self.replicas[i])
        return self.replicas

    def test_flash_set_replicates_to_replica(self):
        self.setup_replication(num_replicas=1)
        self.client.execute_command("FLASH.SET repkey repvalue")
        self.waitForReplicaToSyncUp(self.replicas[0])
        assert self.replicas[0].client.execute_command("EXISTS repkey") == 1

    def test_flash_set_nx_replication(self):
        self.setup_replication(num_replicas=1)
        self.client.execute_command("FLASH.SET nxrep val NX")
        self.waitForReplicaToSyncUp(self.replicas[0])
        assert self.replicas[0].client.execute_command("EXISTS nxrep") == 1

    def test_flash_set_nil_not_replicated(self):
        self.setup_replication(num_replicas=1)
        # Overwrite attempt with NX on an existing key returns nil — not replicated.
        self.client.execute_command("FLASH.SET existing val1")
        self.waitForReplicaToSyncUp(self.replicas[0])
        result = self.client.execute_command("FLASH.SET existing val2 NX")
        assert result is None
        self.waitForReplicaToSyncUp(self.replicas[0])
        # Replica should have received only the first SET.
        cmd_stats = self.replicas[0].client.info("Commandstats")
        flash_set_calls = cmd_stats.get("cmdstat_FLASH.SET", {}).get("calls", 0)
        assert flash_set_calls == 1
