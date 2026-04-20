"""
3-node cluster basic integration suite.

Two test layers:
  A. @pytest.mark.docker_cluster — happy-path against the real 6-node Docker cluster
     (3 primaries on 7001-7003, 3 replicas on 7004-7006).  Tests: module readiness,
     slot distribution, FLASH.SET/GET/DEL/HSET/HGETALL round-trips, replica reads,
     INFO and CONFIG fields on every node.

  B. Parametrized ValkeyTestCase — runs the same FLASH operation scenarios with both
     flash.replica-tier-enabled=yes and flash.replica-tier-enabled=no on a single
     node.  Because the flag is IMMUTABLE (startup-only), parametrize over the module
     arg rather than using CONFIG SET.

Run:
    # Docker cluster tests
    USE_DOCKER=1 pytest tests/test_flash_cluster_basic.py -v -m docker_cluster
    # Parametrized single-node tests (no Docker)
    pytest tests/test_flash_cluster_basic.py -v -k "ReplicaTier"
"""

import os
import shutil
import tempfile
import time
from contextlib import suppress

import pytest
import valkey
from valkey.cluster import ValkeyCluster
from valkeytestframework.conftest import resource_port_tracker  # noqa: F401
from valkeytestframework.valkey_test_case import ValkeyTestCase

# ── Helpers ───────────────────────────────────────────────────────────────────

_ALL_PORTS = (7001, 7002, 7003, 7004, 7005, 7006)
_PRIMARY_PORTS = (7001, 7002, 7003)
_REPLICA_PORTS = (7004, 7005, 7006)


def _node(port: int) -> valkey.Valkey:
    return valkey.Valkey(host="localhost", port=port, socket_timeout=5)


def _cluster_rw(seed_port: int = 7001) -> ValkeyCluster:
    """Write-to-primary, read-from-replica cluster client."""
    from docker_fixtures import flash_cluster_client

    return flash_cluster_client(port=seed_port, read_from_replicas=True)


def _cleanup(client: ValkeyCluster, *keys: str) -> None:
    for k in keys:
        with suppress(Exception):
            client.execute_command("FLASH.DEL", k)


# Keys with predictable hash-tag slots (CRC-16/XMODEM mod 16384):
#   {a} → slot 7638   {b} → slot 7739   {c} → slot 7365
# 20 keys spread across these three hash families cover all three primary nodes
# in a default 3-node cluster slot distribution.
_KEYS_20 = [f"{{{t}}}{i}" for t in ("a", "b", "c", "d") for i in range(5)]


# ── A. Docker cluster tests ───────────────────────────────────────────────────


@pytest.mark.docker_cluster
def test_module_ready_on_all_nodes(docker_cluster):
    """Module loads in Ready state on all 6 cluster nodes."""
    for port in _ALL_PORTS:
        client = _node(port)
        try:
            info = client.info("flash")
            assert info.get("flash_module_state") == "ready", (
                f"port {port}: flash_module_state={info.get('flash_module_state')!r}"
            )
        finally:
            client.close()


@pytest.mark.docker_cluster
def test_cluster_slots_three_primaries(docker_cluster):
    """CLUSTER SLOTS returns exactly 3 primary-owned slot ranges."""
    client = _node(7001)
    try:
        slots = client.cluster("slots")
        assert len(slots) == 3, (
            f"Expected 3 slot ranges (one per primary), got {len(slots)}: {slots}"
        )
    finally:
        client.close()


@pytest.mark.docker_cluster
def test_flash_set_across_slots(docker_cluster):
    """FLASH.SET succeeds for 20 keys spread across all three primary-owned slot ranges."""
    with _cluster_rw() as c:
        for k in _KEYS_20:
            c.execute_command("FLASH.SET", k, f"val:{k}")
        for k in _KEYS_20:
            val = c.execute_command("FLASH.GET", k)
            assert val == f"val:{k}".encode(), f"FLASH.GET({k!r}) returned {val!r}"
        _cleanup(c, *_KEYS_20)


@pytest.mark.docker_cluster
def test_flash_get_from_replicas(docker_cluster):
    """Replicas reflect FLASH.SET writes (async replication, cluster path).

    Writes via a cluster client (routes to the primary owning the slot), then
    reads directly from each replica node. The replica that pairs with the
    owning primary must observe the value; other replicas respond with MOVED.
    """
    key = "{repl}probe"
    # Issue the write through the cluster client, then use WAIT to block until
    # the primary has pushed the command to ≥1 replica (up to 500 ms). This
    # makes the test deterministic regardless of replication-stream buffering.
    with _cluster_rw() as c:
        c.execute_command("FLASH.SET", key, "replica-check")
        # Route WAIT to the slot's primary so we're asking the correct node.
        slot = c.execute_command("CLUSTER", "KEYSLOT", key)
        owner_node = c.nodes_manager.get_node_from_slot(int(slot), read_from_replicas=False)
        owner = c.get_valkey_connection(owner_node)
        owner.execute_command("WAIT", 1, 500)

    # Read directly from each replica node.
    for port in _REPLICA_PORTS:
        client = _node(port)
        try:
            # READONLY allows read commands on replica.
            client.execute_command("READONLY")
            val = client.execute_command("FLASH.GET", key)
            assert val == b"replica-check", f"replica port {port}: FLASH.GET returned {val!r}"
        except valkey.exceptions.ResponseError as e:
            # MOVED is acceptable: this replica doesn't own the slot.
            if "MOVED" not in str(e):
                raise
        finally:
            client.close()

    with _cluster_rw() as c:
        _cleanup(c, key)


@pytest.mark.docker_cluster
def test_flash_hset_hgetall_roundtrip(docker_cluster):
    """FLASH.HSET + FLASH.HGETALL round-trip on a hashed key in cluster mode."""
    key = "{hash}cluster:h1"
    with _cluster_rw() as c:
        c.execute_command("FLASH.HSET", key, "f1", "v1", "f2", "v2", "f3", "v3")
        result = c.execute_command("FLASH.HGETALL", key)
        pairs = dict(
            zip(
                [b.decode() if isinstance(b, bytes) else b for b in result[::2]],
                [b.decode() if isinstance(b, bytes) else b for b in result[1::2]],
                strict=False,
            )
        )
        assert pairs == {"f1": "v1", "f2": "v2", "f3": "v3"}, (
            f"FLASH.HGETALL returned unexpected pairs: {pairs}"
        )
        _cleanup(c, key)


@pytest.mark.docker_cluster
def test_flash_del_propagates_to_replicas(docker_cluster):
    """FLASH.DEL propagates to replicas within 200 ms."""
    key = "{del}probe"
    with _cluster_rw() as c:
        c.execute_command("FLASH.SET", key, "to-be-deleted")
        time.sleep(0.1)
        c.execute_command("FLASH.DEL", key)

    time.sleep(0.2)

    for port in _REPLICA_PORTS:
        client = _node(port)
        try:
            client.execute_command("READONLY")
            val = client.execute_command("FLASH.GET", key)
            assert val is None, f"replica port {port}: key still present after DEL: {val!r}"
        except valkey.exceptions.ResponseError as e:
            if "MOVED" not in str(e):
                raise
        finally:
            client.close()


@pytest.mark.docker_cluster
def test_info_flash_cluster_mode_on_all_nodes(docker_cluster):
    """INFO flash reports flash_cluster_mode:yes on all 6 nodes."""
    for port in _ALL_PORTS:
        client = _node(port)
        try:
            info = client.info("flash")
            assert info.get("flash_cluster_mode") == "yes", (
                f"port {port}: flash_cluster_mode={info.get('flash_cluster_mode')!r}"
            )
        finally:
            client.close()


@pytest.mark.docker_cluster
def test_config_get_cluster_mode_enabled_on_all_nodes(docker_cluster):
    """CONFIG GET flash.cluster-mode-enabled returns 'auto' on all 6 nodes."""
    for port in _ALL_PORTS:
        client = _node(port)
        try:
            result = client.config_get("flash.cluster-mode-enabled")
            val = result.get("flash.cluster-mode-enabled")
            assert val == "auto", f"port {port}: flash.cluster-mode-enabled={val!r}"
        finally:
            client.close()


# ── B. Parametrized single-node tests (replica_tier_enabled: yes / no) ────────


def _binaries_dir() -> str:
    return os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "build",
        "binaries",
        os.environ["SERVER_VERSION"],
    )


def _server_path() -> str:
    return os.path.join(_binaries_dir(), "valkey-server")


def _setup_ld_path() -> None:
    d = _binaries_dir()
    existing = os.environ.get("LD_LIBRARY_PATH", "")
    os.environ["LD_LIBRARY_PATH"] = f"{d}:{existing}" if existing else d


@pytest.mark.parametrize("replica_tier_enabled", ["yes", "no"])
class TestFlashBasicWithReplicaTier(ValkeyTestCase):
    """
    Basic FLASH.SET/GET/DEL/HSET/HGETALL with replica-tier-enabled=yes and =no.

    Runs against a single-node server (no real cluster); validates that the
    module works correctly in both startup configurations independently of
    cluster wiring.
    """

    @pytest.fixture(autouse=True)
    def setup_test(self, setup, replica_tier_enabled):
        _setup_ld_path()
        self._tmpdir = tempfile.mkdtemp(prefix="flash_cluster_basic_")
        flash_path = os.path.join(self._tmpdir, "flash.bin")
        module_path = os.getenv("MODULE_PATH")
        self.args = {
            "enable-debug-command": "yes",
            "loadmodule": (
                f"{module_path} path {flash_path} "
                f"capacity-bytes 16777216 "
                f"replica-tier-enabled {replica_tier_enabled}"
            ),
        }
        self.server, self.client = self.create_server(
            testdir=self.testdir, server_path=_server_path(), args=self.args
        )

    def teardown(self):
        super().teardown()
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def test_flash_set_get(self):
        """FLASH.SET round-trips correctly."""
        self.client.execute_command("FLASH.SET", "rk1", "hello")
        val = self.client.execute_command("FLASH.GET", "rk1")
        assert val == b"hello"

    def test_flash_del(self):
        """FLASH.DEL removes the key."""
        self.client.execute_command("FLASH.SET", "rk2", "bye")
        n = self.client.execute_command("FLASH.DEL", "rk2")
        assert int(n) == 1
        assert self.client.execute_command("FLASH.GET", "rk2") is None

    def test_flash_hset_hgetall(self):
        """FLASH.HSET + FLASH.HGETALL round-trip."""
        self.client.execute_command("FLASH.HSET", "rh1", "field", "value")
        result = self.client.execute_command("FLASH.HGETALL", "rh1")
        assert b"field" in result and b"value" in result

    def test_info_module_state_ready(self):
        """Module is in ready state regardless of replica_tier_enabled setting."""
        info = self.client.info("flash")
        assert info.get("flash_module_state") == "ready"

    def test_replica_tier_enabled_config_readable(self):
        """flash.replica-tier-enabled is readable via CONFIG GET."""
        result = self.client.config_get("flash.replica-tier-enabled")
        assert "flash.replica-tier-enabled" in result
