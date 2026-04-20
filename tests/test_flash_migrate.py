"""
Integration tests for FLASH.MIGRATE.PROBE.

Run: pytest tests/test_flash_migrate.py -v

Scenario 1 — FLASH.MIGRATE.PROBE (no args): returns state/capacity/free/path for local node.
Scenario 2 — FLASH.MIGRATE.PROBE wrong-arity: rejected.
Scenario 3 — FLASH.MIGRATE.PROBE host port (unreachable): returns timeout error message.
Scenario 4 — FLASH.MIGRATE.PROBE host port (no flash): returns "does not have flash-module loaded".
Scenario 5 — FLASH.MIGRATE.PROBE host port (flash loaded): returns probe info.
Scenario 6 — CONFIG GET flash.migration-*: all four knobs readable.
"""

import os
import shutil
import tempfile

import pytest
from valkey import ResponseError
from valkeytestframework.conftest import resource_port_tracker  # noqa: F401
from valkeytestframework.valkey_test_case import ValkeyTestCase


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


def _probe_response_to_dict(resp):
    """Convert a flat list ['key', 'val', ...] to a dict."""
    it = iter(resp)
    return {
        k.decode() if isinstance(k, bytes) else k: v.decode() if isinstance(v, bytes) else v
        for k, v in zip(it, it, strict=False)
    }


# ── Local probe tests ─────────────────────────────────────────────────────────


class TestFlashMigrateProbeLocal(ValkeyTestCase):
    """FLASH.MIGRATE.PROBE with no arguments returns local module info."""

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

    def test_probe_local_returns_state(self):
        """FLASH.MIGRATE.PROBE returns state=ready after module init."""
        resp = self.client.execute_command("FLASH.MIGRATE.PROBE")
        info = _probe_response_to_dict(resp)
        assert info["state"] == "ready", f"expected state=ready, got {info}"

    def test_probe_local_returns_capacity(self):
        """FLASH.MIGRATE.PROBE returns capacity_bytes > 0."""
        resp = self.client.execute_command("FLASH.MIGRATE.PROBE")
        info = _probe_response_to_dict(resp)
        assert int(info["capacity_bytes"]) > 0, f"expected positive capacity, got {info}"

    def test_probe_local_returns_path(self):
        """FLASH.MIGRATE.PROBE returns the configured flash.path."""
        resp = self.client.execute_command("FLASH.MIGRATE.PROBE")
        info = _probe_response_to_dict(resp)
        assert info["path"] != "", f"expected non-empty path, got {info}"

    def test_probe_local_wrong_arity(self):
        """FLASH.MIGRATE.PROBE with 1 arg (not 0 or 2) returns WrongArity."""
        with pytest.raises(Exception, match=r"(?i)wrong"):
            self.client.execute_command("FLASH.MIGRATE.PROBE", "extra")

    def test_probe_local_returns_free_bytes(self):
        """FLASH.MIGRATE.PROBE returns free_bytes field."""
        resp = self.client.execute_command("FLASH.MIGRATE.PROBE")
        info = _probe_response_to_dict(resp)
        assert "free_bytes" in info, f"missing free_bytes: {info}"


# ── Remote probe timeout test ─────────────────────────────────────────────────


class TestFlashMigrateProbeRemote(ValkeyTestCase):
    """FLASH.MIGRATE.PROBE host port returns error when target is unreachable."""

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

    def test_probe_unreachable_returns_timeout_error(self):
        """FLASH.MIGRATE.PROBE to a closed port returns a timeout/refused error."""
        with pytest.raises(Exception) as exc_info:
            # Port 1 is typically closed/refused; gives immediate "connection refused"
            self.client.execute_command("FLASH.MIGRATE.PROBE", "127.0.0.1", "1")
        error = str(exc_info.value)
        # Should contain either "did not respond within timeout" or "invalid address"
        assert "FLASH-MIGRATE" in error, f"Expected FLASH-MIGRATE error message, got: {error}"


# ── Remote probe: no flash module on target ───────────────────────────────────


class TestFlashMigrateProbeNoFlash(ValkeyTestCase):
    """FLASH.MIGRATE.PROBE to a plain Valkey node returns 'does not have flash-module loaded'."""

    @pytest.fixture(autouse=True)
    def setup_test(self, setup):
        _setup_ld_path()
        self._tmpdir = tempfile.mkdtemp(prefix="flash_test_")
        flash_path = os.path.join(self._tmpdir, "flash.bin")
        # Source node has flash module loaded.
        self.args = {
            "enable-debug-command": "yes",
            "loadmodule": f"{os.getenv('MODULE_PATH')} path {flash_path} capacity-bytes 16777216",
        }
        self.server, self.client = self.create_server(
            testdir=self.testdir, server_path=_server_path(), args=self.args
        )
        # Target node has NO flash module.
        self.plain_server, self.plain_client = self.create_server(
            testdir=self.testdir,
            server_path=_server_path(),
            args={"enable-debug-command": "yes"},
        )

    def teardown(self):
        super().teardown()
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def test_probe_plain_server_returns_no_flash_error(self):
        """Probing a plain Valkey node reports that flash-module is not loaded."""
        plain_port = self.plain_server.port
        with pytest.raises(Exception) as exc_info:
            self.client.execute_command("FLASH.MIGRATE.PROBE", "127.0.0.1", str(plain_port))
        error = str(exc_info.value)
        assert "does not have flash-module loaded" in error, (
            f"Expected 'does not have flash-module loaded' error, got: {error}"
        )


# ── Remote probe: flash on target ────────────────────────────────────────────


class TestFlashMigrateProbeWithFlash(ValkeyTestCase):
    """FLASH.MIGRATE.PROBE host port returns probe info when target has flash loaded."""

    @pytest.fixture(autouse=True)
    def setup_test(self, setup):
        _setup_ld_path()
        self._tmpdir = tempfile.mkdtemp(prefix="flash_test_")
        src_path = os.path.join(self._tmpdir, "src.bin")
        dst_path = os.path.join(self._tmpdir, "dst.bin")
        module_path = os.getenv("MODULE_PATH")
        self.args = {
            "enable-debug-command": "yes",
            "loadmodule": f"{module_path} path {src_path} capacity-bytes 16777216",
        }
        self.server, self.client = self.create_server(
            testdir=self.testdir, server_path=_server_path(), args=self.args
        )
        self.target_args = {
            "enable-debug-command": "yes",
            "loadmodule": f"{module_path} path {dst_path} capacity-bytes 16777216",
        }
        self.target_server, self.target_client = self.create_server(
            testdir=self.testdir, server_path=_server_path(), args=self.target_args
        )

    def teardown(self):
        super().teardown()
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def test_probe_flash_target_returns_info(self):
        """Probing a flash-loaded target returns state/capacity/free_bytes/path."""
        target_port = self.target_server.port
        resp = self.client.execute_command("FLASH.MIGRATE.PROBE", "127.0.0.1", str(target_port))
        info = _probe_response_to_dict(resp)
        assert info.get("state") == "ready", f"expected state=ready, got {info}"
        assert int(info.get("capacity_bytes", 0)) > 0, f"expected capacity>0, got {info}"
        assert "path" in info and info["path"] != "", f"missing path: {info}"


# ── Migration config knobs ────────────────────────────────────────────────────


class TestFlashMigrationConfig(ValkeyTestCase):
    """All four flash.migration-* config knobs are readable via CONFIG GET."""

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

    def _config_get(self, knob):
        resp = self.client.config_get(f"flash.{knob}")
        return resp.get(f"flash.{knob}")

    def test_migration_max_key_bytes_default(self):
        val = self._config_get("migration-max-key-bytes")
        assert int(val) == 64 * 1024 * 1024

    def test_migration_bandwidth_mbps_default(self):
        val = self._config_get("migration-bandwidth-mbps")
        assert int(val) == 100

    def test_migration_chunk_timeout_sec_default(self):
        val = self._config_get("migration-chunk-timeout-sec")
        assert int(val) == 30

    def test_migration_probe_cache_sec_default(self):
        val = self._config_get("migration-probe-cache-sec")
        assert int(val) == 60

    def test_migration_bandwidth_mbps_mutable(self):
        """flash.migration-bandwidth-mbps is mutable via CONFIG SET."""
        self.client.config_set("flash.migration-bandwidth-mbps", "200")
        val = self._config_get("migration-bandwidth-mbps")
        assert int(val) == 200
        # Restore
        self.client.config_set("flash.migration-bandwidth-mbps", "100")

    def test_migration_chunk_timeout_mutable(self):
        """flash.migration-chunk-timeout-sec is mutable via CONFIG SET."""
        self.client.config_set("flash.migration-chunk-timeout-sec", "60")
        val = self._config_get("migration-chunk-timeout-sec")
        assert int(val) == 60
        self.client.config_set("flash.migration-chunk-timeout-sec", "30")

    def test_migration_max_key_bytes_immutable(self):
        """flash.migration-max-key-bytes is IMMUTABLE (CONFIG SET should fail)."""
        with pytest.raises(ResponseError):
            self.client.config_set("flash.migration-max-key-bytes", "1024")

    def test_migration_probe_cache_sec_immutable(self):
        """flash.migration-probe-cache-sec is IMMUTABLE (CONFIG SET should fail)."""
        with pytest.raises(ResponseError):
            self.client.config_set("flash.migration-probe-cache-sec", "120")
