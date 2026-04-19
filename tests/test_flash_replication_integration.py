"""
Integration tests for FLASH replication: basic propagation, post-promotion writes,
RDB full sync, diskless sync, PSYNC reconnect, and cluster topology.

Scenarios 1-5 use bare valkey-server binaries (fast, no Docker required).
Scenario 6 requires Docker: USE_DOCKER=1 pytest tests/test_flash_replication_integration.py -v
"""

import os
from contextlib import suppress

import pytest
import valkey
from util.waiters import wait_for_true
from valkeytestframework.conftest import resource_port_tracker  # noqa: F401
from valkeytestframework.valkey_test_case import ReplicationTestCase

_MAX_SYNC_WAIT = 60


def _binaries_dir():
    return os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "build",
        "binaries",
        os.environ["SERVER_VERSION"],
    )


def _server_path():
    return os.path.join(_binaries_dir(), "valkey-server")


def _setup_ld_path():
    d = _binaries_dir()
    existing = os.environ.get("LD_LIBRARY_PATH", "")
    os.environ["LD_LIBRARY_PATH"] = f"{d}:{existing}" if existing else d


def _wait_for_replica_sync(primary_client, replica_client, timeout=_MAX_SYNC_WAIT):
    """Poll until master_link_status:up and replica offset matches primary offset."""

    def _synced():
        try:
            rep = replica_client.info("replication")
            if rep.get("master_link_status") != "up":
                return False
            pri = primary_client.info("replication")
            return rep.get("slave_repl_offset") == pri.get("master_repl_offset")
        except Exception:
            return False

    wait_for_true(_synced, timeout=timeout)


class TestFlashReplicationIntegration(ReplicationTestCase):
    @pytest.fixture(autouse=True)
    def setup_test(self, setup):
        _setup_ld_path()
        self.args = {
            "enable-debug-command": "yes",
            "loadmodule": os.getenv("MODULE_PATH"),
        }
        self.server, self.client = self.create_server(
            testdir=self.testdir, server_path=_server_path(), args=self.args
        )

    # -------------------------------------------------------------------------
    # Scenario 1 — basic propagation (SET / DEL / HSET / HDEL)
    # -------------------------------------------------------------------------
    def test_set_del_hash_propagation(self):
        """SET, DEL, HSET, and HDEL all replicate to a live replica."""
        self.setup_replication(num_replicas=1)
        r = self.replicas[0]

        self.client.execute_command("FLASH.SET", "rep_str", "hello")
        self.client.execute_command("FLASH.HSET", "rep_hash", "f1", "v1", "f2", "v2")
        self.waitForReplicaToSyncUp(r)

        assert r.client.execute_command("FLASH.GET", "rep_str") == b"hello"
        assert r.client.execute_command("FLASH.HGET", "rep_hash", "f1") == b"v1"

        self.client.execute_command("FLASH.DEL", "rep_str")
        self.client.execute_command("FLASH.HDEL", "rep_hash", "f1")
        self.waitForReplicaToSyncUp(r)

        assert r.client.execute_command("FLASH.GET", "rep_str") is None
        assert r.client.execute_command("FLASH.HGET", "rep_hash", "f1") is None
        assert r.client.execute_command("FLASH.HGET", "rep_hash", "f2") == b"v2"

    # -------------------------------------------------------------------------
    # Scenario 2 — post-promotion writes (lazy NVMe init, fix #64)
    # -------------------------------------------------------------------------
    def test_post_promotion_writes(self):
        """Promoted replica initialises NVMe backend on REPLICAOF NO ONE and accepts writes."""
        self.setup_replication(num_replicas=1)
        r = self.replicas[0]

        self.client.execute_command("FLASH.SET", "promo_key", "promo_val")
        self.waitForReplicaToSyncUp(r)

        r.client.execute_command("REPLICAOF", "NO", "ONE")

        assert r.client.execute_command("FLASH.SET", "post_promo", "post_val") == b"OK"
        assert r.client.execute_command("FLASH.GET", "post_promo") == b"post_val"
        # Previously-replicated data remains accessible after promotion.
        assert r.client.execute_command("FLASH.GET", "promo_key") == b"promo_val"

    # -------------------------------------------------------------------------
    # Scenario 3 — RDB full resync
    # -------------------------------------------------------------------------
    def test_rdb_full_resync(self):
        """Fresh replica joining after primary already has data receives it all via RDB sync."""
        N = 20
        for i in range(N):
            self.client.execute_command("FLASH.SET", f"rdb_{i}", f"val_{i}")
            self.client.execute_command("FLASH.HSET", f"rdb_h{i}", "field", f"hval_{i}")

        # Replica starts after all writes — must do a full RDB sync to catch up.
        self.setup_replication(num_replicas=1)
        _wait_for_replica_sync(self.client, self.replicas[0].client)

        r = self.replicas[0]
        for i in range(N):
            assert r.client.execute_command("FLASH.GET", f"rdb_{i}") == f"val_{i}".encode()
            assert (
                r.client.execute_command("FLASH.HGET", f"rdb_h{i}", "field") == f"hval_{i}".encode()
            )

    # -------------------------------------------------------------------------
    # Scenario 4 — diskless resync
    # -------------------------------------------------------------------------
    def test_diskless_resync(self):
        """Replica syncs from a diskless in-memory RDB stream."""
        self.client.execute_command("CONFIG", "SET", "repl-diskless-sync", "yes")
        self.client.execute_command("CONFIG", "SET", "repl-diskless-sync-delay", "0")

        N = 15
        for i in range(N):
            self.client.execute_command("FLASH.SET", f"dl_{i}", f"dv_{i}")

        self.setup_replication(num_replicas=1)
        _wait_for_replica_sync(self.client, self.replicas[0].client)

        r = self.replicas[0]
        for i in range(N):
            assert r.client.execute_command("FLASH.GET", f"dl_{i}") == f"dv_{i}".encode()

    # -------------------------------------------------------------------------
    # Scenario 5 — PSYNC reconnect
    # -------------------------------------------------------------------------
    def test_psync_reconnect(self):
        """Replica reconnects after the replication connection is killed and catches up."""
        self.setup_replication(num_replicas=1)
        r = self.replicas[0]

        # Batch 1 — synced before disconnect.
        for i in range(10):
            self.client.execute_command("FLASH.SET", f"ps1_{i}", f"v{i}")
        self.waitForReplicaToSyncUp(r)

        # Kill the replica connection from the primary side to trigger PSYNC on reconnect.
        raw = self.client.execute_command("CLIENT LIST")
        text = raw.decode() if isinstance(raw, bytes) else raw
        for line in text.splitlines():
            parts = dict(tok.split("=", 1) for tok in line.split() if "=" in tok)
            if "S" in parts.get("flags", ""):
                with suppress(Exception):
                    self.client.execute_command("CLIENT KILL", "ID", parts["id"])
                break

        # Batch 2 — written while replica is reconnecting.
        for i in range(10):
            self.client.execute_command("FLASH.SET", f"ps2_{i}", f"v{i}")

        # Replica reconnects automatically via internal retry; wait for full catch-up.
        _wait_for_replica_sync(self.client, r.client)

        for i in range(10):
            assert r.client.execute_command("FLASH.GET", f"ps1_{i}") == f"v{i}".encode()
            assert r.client.execute_command("FLASH.GET", f"ps2_{i}") == f"v{i}".encode()


# -------------------------------------------------------------------------
# Scenario 6 — cluster replication (Docker only)
# -------------------------------------------------------------------------


@pytest.mark.docker_cluster
def test_cluster_flash_replication(docker_cluster):
    """FLASH.SET propagates to replicas; all primaries report one connected replica."""
    client = docker_cluster

    assert client.execute_command("FLASH.SET", "{crep}k", "crep_val") == b"OK"
    assert client.execute_command("FLASH.GET", "{crep}k") == b"crep_val"

    # Every primary must have exactly one connected replica.
    for port in (7001, 7002, 7003):
        info = valkey.Valkey(host="localhost", port=port, socket_timeout=5).info("replication")
        assert info["connected_slaves"] == 1, (
            f"Primary on port {port}: expected 1 replica, got {info['connected_slaves']}"
        )

    # The replica owning the {crep} slot must serve the key via READONLY.
    # Try all three replica ports; one will own the slot, the others return MOVED.
    found = False
    for port in (7004, 7005, 7006):
        rc = valkey.Valkey(host="localhost", port=port, socket_timeout=5)
        try:
            rc.execute_command("READONLY")
            if rc.execute_command("FLASH.GET", "{crep}k") == b"crep_val":
                found = True
                break
        except Exception:
            pass
        finally:
            rc.close()
    assert found, "Key {crep}k not found on any replica port (7004-7006)"
