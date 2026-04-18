import os
import pytest
from valkey import ResponseError
from valkeytestframework.valkey_test_case import ReplicationTestCase
from valkey_flash_test_case import ValkeyFlashTestCase


# ── Helper ────────────────────────────────────────────────────────────────────

def _binaries_dir():
    return (
        f"{os.path.dirname(os.path.realpath(__file__))}"
        f"/build/binaries/{os.environ['SERVER_VERSION']}"
    )


def _server_path():
    return os.path.join(_binaries_dir(), "valkey-server")


def _server_args():
    return {
        "enable-debug-command": "yes",
        "loadmodule": os.getenv("MODULE_PATH"),
    }


def _prepend_lib_path():
    binaries_dir = _binaries_dir()
    existing = os.environ.get("LD_LIBRARY_PATH", "")
    os.environ["LD_LIBRARY_PATH"] = (
        f"{binaries_dir}:{existing}" if existing else binaries_dir
    )


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
            assert False, "Expected error"
        except ResponseError:
            pass

    def test_no_args_rejected(self):
        try:
            self.client.execute_command("FLASH.SET")
            assert False, "Expected error"
        except ResponseError:
            pass

    def test_unknown_option_rejected(self):
        try:
            self.client.execute_command("FLASH.SET k v KEEPTTL")
            assert False, "Expected error"
        except ResponseError as e:
            assert "syntax error" in str(e).lower()

    def test_nx_and_xx_together_rejected(self):
        try:
            self.client.execute_command("FLASH.SET k v NX XX")
            assert False, "Expected error"
        except ResponseError as e:
            assert "compatible" in str(e).lower() or "syntax" in str(e).lower()

    def test_ex_zero_rejected(self):
        try:
            self.client.execute_command("FLASH.SET k v EX 0")
            assert False, "Expected error"
        except ResponseError as e:
            assert "expire" in str(e).lower() or "invalid" in str(e).lower()

    def test_ex_negative_rejected(self):
        try:
            self.client.execute_command("FLASH.SET k v EX -5")
            assert False, "Expected error"
        except ResponseError:
            pass

    def test_px_missing_value_rejected(self):
        try:
            self.client.execute_command("FLASH.SET k v PX")
            assert False, "Expected error"
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
        import time
        self.client.execute_command("FLASH.SET persist val")
        time.sleep(0.2)
        assert self.client.execute_command("EXISTS persist") == 1

    # ── WRONGTYPE ─────────────────────────────────────────────────────────────

    def test_wrongtype_when_native_string_exists(self):
        self.client.execute_command("SET nativekey hello")
        try:
            self.client.execute_command("FLASH.SET nativekey world")
            assert False, "Expected WRONGTYPE error"
        except ResponseError as e:
            assert "WRONGTYPE" in str(e)

    def test_wrongtype_when_list_exists(self):
        self.client.execute_command("RPUSH listkey a b c")
        try:
            self.client.execute_command("FLASH.SET listkey val")
            assert False, "Expected WRONGTYPE error"
        except ResponseError as e:
            assert "WRONGTYPE" in str(e)


# ── Replication test ──────────────────────────────────────────────────────────

class TestFlashSetReplication(ReplicationTestCase):

    @pytest.fixture(autouse=True)
    def setup_test(self, setup):
        _prepend_lib_path()
        self.args = _server_args()
        self.server, self.client = self.create_server(
            testdir=self.testdir,
            server_path=_server_path(),
            args=self.args,
        )

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
