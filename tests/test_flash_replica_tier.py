"""
Integration tests for flash.replica-tier-enabled (Task #82).

Scenarios 1–4 use bare valkey-server binaries (no Docker required).
Run: pytest tests/test_flash_replica_tier.py -v

Scenario 1 — default (replica-tier-enabled=false): replica is RAM-only.
Scenario 2 — tier enabled: replica opens its own NVMe backend.
Scenario 3 — tier enabled: FLASH.DEBUG.DEMOTE works on replica's local tier.
Scenario 4 — tier enabled: promotion to primary is instant (backend already open).
"""

import os
import shutil
import tempfile

import pytest
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


# ── Scenario 1 — default: replica is RAM-only ─────────────────────────────────


class TestFlashReplicaTierDefault(ReplicationTestCase):
    """Default (flash.replica-tier-enabled=false): replica never opens NVMe storage."""

    @pytest.fixture(autouse=True)
    def setup_test(self, setup):
        _setup_ld_path()
        self._tmpdir = tempfile.mkdtemp(prefix="flash_test_")
        primary_path = os.path.join(self._tmpdir, "primary.bin")
        self.args = {
            "enable-debug-command": "yes",
            "loadmodule": f"{os.getenv('MODULE_PATH')} path {primary_path} capacity-bytes 16777216",
        }
        self.server, self.client = self.create_server(
            testdir=self.testdir, server_path=_server_path(), args=self.args
        )

    def teardown(self):
        super().teardown()
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def test_replica_storage_not_opened_by_default(self):
        """Replica has no NVMe storage when flash.replica-tier-enabled is false (default)."""
        # Replicas inherit the same loadmodule args (no replica-tier-enabled flag).
        self.setup_replication(num_replicas=1)
        r = self.replicas[0]
        info = r.client.info("flash")
        assert info["flash_storage_capacity_bytes"] == 0, (
            f"replica should have no NVMe storage by default; "
            f"got flash_storage_capacity_bytes={info['flash_storage_capacity_bytes']}"
        )
        assert info["flash_module_state"] == "ready"


# ── Scenarios 2–4: replica-tier-enabled=yes ───────────────────────────────────


class TestFlashReplicaTierEnabled(ReplicationTestCase):
    """flash.replica-tier-enabled=true: replica opens its own local NVMe backend."""

    @pytest.fixture(autouse=True)
    def setup_test(self, setup):
        _setup_ld_path()
        self._tmpdir = tempfile.mkdtemp(prefix="flash_test_")
        primary_path = os.path.join(self._tmpdir, "primary.bin")
        self._replica_path = os.path.join(self._tmpdir, "replica.bin")
        module_path = os.getenv("MODULE_PATH")
        self.args = {
            "enable-debug-command": "yes",
            "loadmodule": f"{module_path} path {primary_path} capacity-bytes 16777216",
        }
        # Replica gets its own unique path + the tier-enabled flag.
        self._replica_args = {
            "enable-debug-command": "yes",
            "loadmodule": (
                f"{module_path} flash.path {self._replica_path} flash.replica-tier-enabled yes"
            ),
        }
        self.server, self.client = self.create_server(
            testdir=self.testdir, server_path=_server_path(), args=self.args
        )

    def teardown(self):
        super().teardown()
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def create_replicas(
        self,
        num_replicas,
        primaryhost=None,
        primaryport=None,
        connection_type="tcp",
        server_path=None,
    ):
        """Pass replica-specific module args (unique path + tier enabled)."""
        primaryhost = primaryhost or self.server.bind_ip
        primaryport = primaryport or self.server.port
        for _ in range(self.num_replicas):
            replica = self._create_replica(primaryhost, primaryport, server_path)
            replica.set_startup_args(self._replica_args)
            self.replicas.append(replica)

    # Scenario 2 — storage opened on replica
    def test_replica_storage_opened_when_tier_enabled(self):
        """Replica opens its own NVMe backend when flash.replica-tier-enabled=yes."""
        self.setup_replication(num_replicas=1)
        r = self.replicas[0]
        info = r.client.info("flash")
        assert info["flash_storage_capacity_bytes"] > 0, (
            "replica should open NVMe storage with flash.replica-tier-enabled=yes"
        )
        assert info["flash_module_state"] == "ready"

    # Scenario 3 — FLASH.DEBUG.DEMOTE works on replica's local tier
    def test_replica_demote_and_cold_read(self):
        """Replica can demote a hot key to its own local NVMe and read it back cold."""
        self.client.execute_command("FLASH.SET", "tier_key", "tier_val")
        self.setup_replication(num_replicas=1)
        r = self.replicas[0]
        _wait_for_replica_sync(self.client, r.client)

        # Demote on the replica uses the replica's own NVMe backend.
        result = r.client.execute_command("FLASH.DEBUG.DEMOTE", "tier_key")
        assert result == b"OK", f"FLASH.DEBUG.DEMOTE on replica returned {result!r}"

        # Cold-tier read fetches the value back from the replica's NVMe.
        assert r.client.execute_command("FLASH.GET", "tier_key") == b"tier_val"

    # Scenario 4 — promotion is instant (backend already initialized)
    def test_promotion_instant_when_tier_enabled(self):
        """Promoting a replica with tier-enabled skips lazy NVMe init (backend already open)."""
        self.client.execute_command("FLASH.SET", "promo_key", "promo_val")
        self.setup_replication(num_replicas=1)
        r = self.replicas[0]
        _wait_for_replica_sync(self.client, r.client)

        # Record storage capacity before promotion.
        pre_info = r.client.info("flash")
        assert pre_info["flash_storage_capacity_bytes"] > 0

        # Promote to primary.
        r.client.execute_command("REPLICAOF", "NO", "ONE")

        # FLASH.SET must succeed immediately — no lazy backend init needed.
        assert r.client.execute_command("FLASH.SET", "new_key", "new_val") == b"OK"
        assert r.client.execute_command("FLASH.GET", "new_key") == b"new_val"
        assert r.client.execute_command("FLASH.GET", "promo_key") == b"promo_val"

        # Storage capacity stays consistent — no re-open.
        post_info = r.client.info("flash")
        assert post_info["flash_storage_capacity_bytes"] == pre_info["flash_storage_capacity_bytes"]
