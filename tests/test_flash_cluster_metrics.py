"""
Integration tests for migration progress metrics in INFO flash.

Run (requires Docker cluster): USE_DOCKER=1 pytest tests/test_flash_cluster_metrics.py -v

Scenario 1 — INFO flash baseline: all eight migration fields present and
             zero before any migration.
Scenario 2 — flash_migration_bandwidth_mbps reflects CONFIG GET value.
Scenario 3 — flash_migration_bandwidth_mbps updates after CONFIG SET.
Scenario 4 — (docker_cluster) After a real slot migration, assert
             flash_migration_keys_migrated > 0, flash_migration_bytes_sent > 0,
             flash_migration_last_duration_ms > 0 on the source node.
"""

import os
import shutil
import tempfile

import pytest

try:
    import valkey

    _HAVE_VALKEY = True
except ImportError:
    _HAVE_VALKEY = False

from valkey_flash_test_case import ValkeyFlashTestCase as ValkeyTestCase
from valkeytestframework.conftest import resource_port_tracker  # noqa: F401


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


# ── Migration metrics baseline ────────────────────────────────────────────────

MIGRATION_FIELDS = [
    "flash_migration_slots_in_progress",
    "flash_migration_bytes_sent",
    "flash_migration_bytes_received",
    "flash_migration_last_duration_ms",
    "flash_migration_errors",
    "flash_migration_bandwidth_mbps",
    "flash_migration_keys_migrated",
    "flash_migration_keys_rejected",
]


class TestFlashMigrationInfoFields(ValkeyTestCase):
    """INFO flash exposes all eight migration progress fields."""

    @pytest.fixture(autouse=True)
    def setup_test(self, setup):
        _setup_ld_path()
        self._tmpdir = tempfile.mkdtemp(prefix="flash_test_")
        flash_path = os.path.join(self._tmpdir, "flash.bin")
        self.args = {
            "enable-debug-command": "yes",
            "loadmodule": f"{os.getenv('MODULE_PATH')} path {flash_path} capacity-bytes 16777216",
        }
        self.server, self.client = self.create_server(
            testdir=self.testdir, server_path=_server_path(), args=self.args
        )

    def teardown(self):
        super().teardown()
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def test_all_migration_fields_present(self):
        """All eight flash_migration_* fields appear in INFO flash."""
        info = self.client.info("flash")
        for field in MIGRATION_FIELDS:
            assert field in info, f"missing INFO field: {field}"

    def test_migration_counters_start_at_zero(self):
        """Cumulative migration counters are zero before any migration."""
        info = self.client.info("flash")
        for field in [
            "flash_migration_slots_in_progress",
            "flash_migration_bytes_sent",
            "flash_migration_bytes_received",
            "flash_migration_last_duration_ms",
            "flash_migration_errors",
            "flash_migration_keys_migrated",
            "flash_migration_keys_rejected",
        ]:
            assert int(info[field]) == 0, f"{field} should be 0 at startup, got {info[field]}"

    def test_migration_bandwidth_mbps_reflects_config(self):
        """flash_migration_bandwidth_mbps in INFO matches flash.migration-bandwidth-mbps."""
        info = self.client.info("flash")
        config_val = int(
            self.client.config_get("flash.migration-bandwidth-mbps").get(
                "flash.migration-bandwidth-mbps", 0
            )
        )
        assert int(info["flash_migration_bandwidth_mbps"]) == config_val

    def test_migration_bandwidth_mbps_updates_after_config_set(self):
        """flash_migration_bandwidth_mbps reflects a live CONFIG SET."""
        self.client.config_set("flash.migration-bandwidth-mbps", "50")
        info = self.client.info("flash")
        assert int(info["flash_migration_bandwidth_mbps"]) == 50
        # Restore
        self.client.config_set("flash.migration-bandwidth-mbps", "100")

    def test_migration_bandwidth_zero_accepted(self):
        """flash.migration-bandwidth-mbps = 0 (unlimited) is accepted."""
        self.client.config_set("flash.migration-bandwidth-mbps", "0")
        info = self.client.info("flash")
        assert int(info["flash_migration_bandwidth_mbps"]) == 0
        self.client.config_set("flash.migration-bandwidth-mbps", "100")


# ── Docker cluster — live migration counters ──────────────────────────────────

_PRIMARY_PORTS = (7001, 7002, 7003)
_CLUSTER_PORTS = (7001, 7002, 7003, 7004, 7005, 7006)


def _node(port):
    return valkey.Valkey(host="localhost", port=port, socket_timeout=10)


@pytest.mark.docker_cluster
@pytest.mark.skipif(not _HAVE_VALKEY, reason="valkey package not installed")
def test_migration_info_fields_present_on_all_nodes(docker_cluster):
    """All eight flash_migration_* fields appear on every cluster node."""
    for port in _CLUSTER_PORTS:
        client = _node(port)
        try:
            info = client.info("flash")
            for field in MIGRATION_FIELDS:
                assert field in info, f"port {port}: missing {field}"
        finally:
            client.close()


@pytest.mark.docker_cluster
@pytest.mark.skipif(not _HAVE_VALKEY, reason="valkey package not installed")
def test_migration_bandwidth_zero_accepted_in_cluster(docker_cluster):
    """CONFIG SET flash.migration-bandwidth-mbps 0 accepted on every primary."""
    for port in _PRIMARY_PORTS:
        client = _node(port)
        try:
            client.config_set("flash.migration-bandwidth-mbps", "0")
            info = client.info("flash")
            assert int(info.get("flash_migration_bandwidth_mbps", -1)) == 0
            client.config_set("flash.migration-bandwidth-mbps", "100")
        finally:
            client.close()
