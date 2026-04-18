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


def _prepend_lib_path():
    binaries_dir = _binaries_dir()
    existing = os.environ.get("LD_LIBRARY_PATH", "")
    os.environ["LD_LIBRARY_PATH"] = (
        f"{binaries_dir}:{existing}" if existing else binaries_dir
    )


def _server_args():
    return {
        "enable-debug-command": "yes",
        "loadmodule": os.getenv("MODULE_PATH"),
    }


# ── Basic FLASH.DEL tests ─────────────────────────────────────────────────────

class TestFlashDel(ValkeyFlashTestCase):

    def test_del_existing_key_returns_one(self):
        self.client.execute_command("FLASH.SET", "delkey", "val")
        result = self.client.execute_command("FLASH.DEL", "delkey")
        assert result == 1

    def test_del_removes_key_from_keyspace(self):
        self.client.execute_command("FLASH.SET", "gone", "v")
        self.client.execute_command("FLASH.DEL", "gone")
        assert self.client.execute_command("EXISTS", "gone") == 0

    def test_del_missing_key_returns_zero(self):
        result = self.client.execute_command("FLASH.DEL", "nokey")
        assert result == 0

    def test_del_variadic_two_present_one_missing_returns_two(self):
        self.client.execute_command("FLASH.SET", "k1", "v1")
        self.client.execute_command("FLASH.SET", "k2", "v2")
        result = self.client.execute_command("FLASH.DEL", "k1", "noexist", "k2")
        assert result == 2

    def test_del_variadic_all_missing_returns_zero(self):
        result = self.client.execute_command("FLASH.DEL", "x", "y", "z")
        assert result == 0

    def test_del_idempotent_double_delete(self):
        self.client.execute_command("FLASH.SET", "idem", "v")
        assert self.client.execute_command("FLASH.DEL", "idem") == 1
        assert self.client.execute_command("FLASH.DEL", "idem") == 0

    # ── Malformed args ────────────────────────────────────────────────────────

    def test_no_args_rejected(self):
        try:
            self.client.execute_command("FLASH.DEL")
            assert False, "Expected error"
        except ResponseError:
            pass

    # ── WRONGTYPE ─────────────────────────────────────────────────────────────

    def test_wrongtype_when_native_string_exists(self):
        self.client.execute_command("SET", "nativekey", "hello")
        try:
            self.client.execute_command("FLASH.DEL", "nativekey")
            assert False, "Expected WRONGTYPE error"
        except ResponseError as e:
            assert "WRONGTYPE" in str(e)

    def test_wrongtype_short_circuits_no_partial_delete(self):
        # k1 is a flash string, k2 is a native string.
        # FLASH.DEL k1 k2 should return WRONGTYPE without deleting k1.
        self.client.execute_command("FLASH.SET", "safe", "v")
        self.client.execute_command("SET", "native", "n")
        try:
            self.client.execute_command("FLASH.DEL", "safe", "native")
            assert False, "Expected WRONGTYPE error"
        except ResponseError as e:
            assert "WRONGTYPE" in str(e)
        # k1 must still exist — no partial deletion.
        assert self.client.execute_command("EXISTS", "safe") == 1

    # ── Cache eviction ────────────────────────────────────────────────────────

    def test_del_hot_key_removes_from_get(self):
        self.client.execute_command("FLASH.SET", "hotkey", "hotval")
        assert self.client.execute_command("FLASH.GET", "hotkey") == b"hotval"
        self.client.execute_command("FLASH.DEL", "hotkey")
        assert self.client.execute_command("FLASH.GET", "hotkey") is None

    def test_del_then_set_same_key_works(self):
        self.client.execute_command("FLASH.SET", "recycle", "old")
        self.client.execute_command("FLASH.DEL", "recycle")
        self.client.execute_command("FLASH.SET", "recycle", "new")
        assert self.client.execute_command("FLASH.GET", "recycle") == b"new"

    # ── Non-blocking during variadic delete ───────────────────────────────────

    def test_del_does_not_block_event_loop(self):
        for i in range(20):
            self.client.execute_command("FLASH.SET", f"nb:{i}", "v" * 1024)
        probe_client = self.server.get_new_client()
        keys = [f"nb:{i}" for i in range(20)]

        def del_fn():
            self.client.execute_command("FLASH.DEL", *keys)

        self.verify_nonblocking_during(
            self.client, del_fn, probe_client, "PING"
        )


# ── Replication test ──────────────────────────────────────────────────────────

class TestFlashDelReplication(ReplicationTestCase):

    @pytest.fixture(autouse=True)
    def setup_test(self, setup):
        _prepend_lib_path()
        self.args = _server_args()
        self.server, self.client = self.create_server(
            testdir=self.testdir,
            server_path=_server_path(),
            args=self.args,
        )

    def test_flash_del_replicates_to_replica(self):
        self.setup_replication(num_replicas=1)
        self.client.execute_command("FLASH.SET", "repkey", "repval")
        self.waitForReplicaToSyncUp(self.replicas[0])
        self.client.execute_command("FLASH.DEL", "repkey")
        self.waitForReplicaToSyncUp(self.replicas[0])
        assert self.replicas[0].client.execute_command("EXISTS", "repkey") == 0

    def test_flash_del_missing_not_replicated(self):
        self.setup_replication(num_replicas=1)
        result = self.client.execute_command("FLASH.DEL", "ghost")
        assert result == 0
        self.waitForReplicaToSyncUp(self.replicas[0])
        cmd_stats = self.replicas[0].client.info("Commandstats")
        flash_del_calls = cmd_stats.get("cmdstat_FLASH.DEL", {}).get("calls", 0)
        assert flash_del_calls == 0
