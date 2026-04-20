import os

import pytest
from valkeytestframework.valkey_test_case import ReplicationTestCase


def _flash_loadmodule_arg(flash_path):
    return f"{os.getenv('MODULE_PATH')} path {flash_path} capacity-bytes 16777216"


class TestFlashReplication(ReplicationTestCase):
    @pytest.fixture(autouse=True)
    def setup_test(self, setup):
        binaries_dir = (
            f"{os.path.dirname(os.path.realpath(__file__))}"
            f"/build/binaries/{os.environ['SERVER_VERSION']}"
        )
        server_path = os.path.join(binaries_dir, "valkey-server")
        existing = os.environ.get("LD_LIBRARY_PATH", "")
        os.environ["LD_LIBRARY_PATH"] = f"{binaries_dir}:{existing}" if existing else binaries_dir
        import shutil
        import tempfile

        self._flash_dir = os.path.abspath(tempfile.mkdtemp(prefix="flash_repl_", dir=self.testdir))
        flash_path = os.path.join(self._flash_dir, "primary.bin")
        self.args = {
            "enable-debug-command": "yes",
            "loadmodule": _flash_loadmodule_arg(flash_path),
        }
        self.server, self.client = self.create_server(
            testdir=self.testdir, server_path=server_path, args=self.args
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
            replica.args["loadmodule"] = _flash_loadmodule_arg(replica_path)
        self.start_replicas()
        self.wait_for_replicas(self.num_replicas)
        self.wait_for_primary_link_up_all_replicas()
        self.wait_for_all_replicas_online(self.num_replicas)
        for i in range(len(self.replicas)):
            self.waitForReplicaToSyncUp(self.replicas[i])
        return self.replicas

    def test_string_key_replicates_to_replica(self):
        self.setup_replication(num_replicas=1)
        self.client.execute_command("FLASH.SET", "rep_str", "hello")
        self.waitForReplicaToSyncUp(self.replicas[0])
        assert self.replicas[0].client.execute_command("FLASH.GET", "rep_str") == b"hello"

    def test_hash_key_replicates_to_replica(self):
        self.setup_replication(num_replicas=1)
        self.client.execute_command("FLASH.HSET", "rep_hash", "f1", "v1", "f2", "v2")
        self.waitForReplicaToSyncUp(self.replicas[0])
        assert self.replicas[0].client.execute_command("FLASH.HGET", "rep_hash", "f1") == b"v1"
        assert self.replicas[0].client.execute_command("FLASH.HGET", "rep_hash", "f2") == b"v2"

    def test_del_replicates_to_replica(self):
        self.setup_replication(num_replicas=1)
        self.client.execute_command("FLASH.SET", "del_key", "v")
        self.waitForReplicaToSyncUp(self.replicas[0])
        assert self.replicas[0].client.execute_command("FLASH.GET", "del_key") == b"v"

        self.client.execute_command("FLASH.DEL", "del_key")
        self.waitForReplicaToSyncUp(self.replicas[0])
        assert self.replicas[0].client.execute_command("FLASH.GET", "del_key") is None

    def test_replica_module_state_is_ready(self):
        self.setup_replication(num_replicas=1)
        self.waitForReplicaToSyncUp(self.replicas[0])
        state = self.replicas[0].client.execute_command("FLASH.DEBUG.STATE")
        assert state == b"ready"

    def test_promoted_replica_serves_replicated_keys(self):
        self.setup_replication(num_replicas=1)
        self.client.execute_command("FLASH.SET", "promo_key", "promo_val")
        self.waitForReplicaToSyncUp(self.replicas[0])

        self.replicas[0].client.execute_command("REPLICAOF", "NO", "ONE")

        assert self.replicas[0].client.execute_command("FLASH.GET", "promo_key") == b"promo_val"

    def test_multiple_keys_replicate(self):
        self.setup_replication(num_replicas=1)
        for i in range(10):
            self.client.execute_command("FLASH.SET", f"multi_{i}", f"val_{i}")
        self.waitForReplicaToSyncUp(self.replicas[0])
        for i in range(10):
            val = self.replicas[0].client.execute_command("FLASH.GET", f"multi_{i}")
            assert val == f"val_{i}".encode()

    def test_promoted_replica_writes_succeed(self):
        self.setup_replication(num_replicas=1)
        self.waitForReplicaToSyncUp(self.replicas[0])

        self.replicas[0].client.execute_command("REPLICAOF", "NO", "ONE")

        result = self.replicas[0].client.execute_command("FLASH.SET", "post_promo", "post_val")
        assert result == b"OK"
        assert self.replicas[0].client.execute_command("FLASH.GET", "post_promo") == b"post_val"

    def test_hdel_replication(self):
        self.setup_replication(num_replicas=1)
        self.client.execute_command("FLASH.HSET", "hdel_hash", "f1", "v1", "f2", "v2")
        self.waitForReplicaToSyncUp(self.replicas[0])

        self.client.execute_command("FLASH.HDEL", "hdel_hash", "f1")
        self.waitForReplicaToSyncUp(self.replicas[0])

        assert self.replicas[0].client.execute_command("FLASH.HGET", "hdel_hash", "f1") is None
        assert self.replicas[0].client.execute_command("FLASH.HGET", "hdel_hash", "f2") == b"v2"
