"""
Target-no-flash fallback tests (Task #85).

Spec #70 prescribes three incompatible-target scenarios.  Each test uses the
docker_cluster primary (port 7001) as the probe source; incompatible targets
are spawned as standalone servers by each test.

Scenario 1 — Target has no flash module:
  FLASH.MIGRATE.PROBE returns "does not have flash-module loaded".
  MIGRATE of a FLASH.* key fails; source retains ownership.

Scenario 2 — flash.path empty / unreachable:
  INFEASIBLE IN PRACTICE.  flash.path is IMMUTABLE; the module refuses to load
  without a valid path, so a node where flash is "loaded but path-less" cannot
  be constructed.  The capacity=0 / free_bytes=0 branch of local_probe() (when
  STORAGE.get() returns None) is exercised at unit level by
  test_flash_migrate.py::TestFlashMigrateProbeLocal.

Scenario 3 — Target has insufficient flash capacity:
  FLASH.MIGRATE.PROBE returns near-zero free_bytes when target NVMe is full.
  FLASH.MIGRATE (the new capacity-gating wrapper) returns
  "ERR FLASH-MIGRATE target <addr> insufficient flash capacity" when
  free_bytes on the target is less than the estimated NVMe footprint of the
  FLASH keys being migrated.

Run (requires USE_DOCKER=1):
    USE_DOCKER=1 pytest tests/test_flash_cluster_no_flash_target.py -v -m docker_cluster
"""

import os
import shutil
import socket
import subprocess
import tempfile
import time

import pytest
import valkey
import valkey.exceptions


# ── Helpers ───────────────────────────────────────────────────────────────────

_PRIMARY_PORTS = (7001, 7002, 7003)


def _binaries_dir():
    return os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "build", "binaries", os.environ["SERVER_VERSION"],
    )


def _server_path():
    return os.path.join(_binaries_dir(), "valkey-server")


def _find_free_port() -> int:
    """Return an unused local TCP port."""
    with socket.socket() as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class _StandaloneServer:
    """Lifecycle wrapper for a standalone valkey-server subprocess."""

    def __init__(self, port: int, proc, tmpdir: str):
        self.port = port
        self._proc = proc
        self._tmpdir = tmpdir

    def close(self):
        try:
            c = valkey.Valkey(host="127.0.0.1", port=self.port, socket_timeout=1)
            c.execute_command("SHUTDOWN NOSAVE")
            c.close()
        except Exception:
            pass
        try:
            self._proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self._proc.kill()
        shutil.rmtree(self._tmpdir, ignore_errors=True)


def _wait_ready(port: int, timeout: float = 15.0, check_flash: bool = False) -> None:
    """Poll until the server is reachable (and optionally flash_module_state=ready)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            c = valkey.Valkey(host="127.0.0.1", port=port, socket_timeout=1)
            if check_flash:
                info = c.info("flash")
                if info.get("flash_module_state") == "ready":
                    c.close()
                    return
            else:
                c.ping()
                c.close()
                return
        except Exception:
            pass
        time.sleep(0.25)
    raise RuntimeError(f"server on port {port} did not become ready within {timeout}s")


def _start_plain_server() -> _StandaloneServer:
    """Start a standalone valkey-server with no modules loaded."""
    port = _find_free_port()
    tmpdir = tempfile.mkdtemp(prefix="flash85_plain_")
    conf = os.path.join(tmpdir, "valkey.conf")
    with open(conf, "w") as f:
        f.write(f"port {port}\nloglevel warning\n")
    proc = subprocess.Popen(
        [_server_path(), conf],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        cwd=tmpdir,
    )
    _wait_ready(port)
    return _StandaloneServer(port, proc, tmpdir)


def _start_flash_server(capacity_bytes: int) -> _StandaloneServer:
    """Start a standalone valkey-server with flash loaded at the given NVMe capacity."""
    port = _find_free_port()
    tmpdir = tempfile.mkdtemp(prefix="flash85_cap_")
    flash_path = os.path.join(tmpdir, "flash.bin")
    module_path = os.getenv("MODULE_PATH", "")
    conf = os.path.join(tmpdir, "valkey.conf")
    with open(conf, "w") as f:
        f.write(
            f"port {port}\n"
            f"loglevel warning\n"
            f"enable-debug-command yes\n"
            f"loadmodule {module_path} flash.path {flash_path} "
            f"flash.capacity-bytes {capacity_bytes}\n"
        )
    proc = subprocess.Popen(
        [_server_path(), conf],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        cwd=tmpdir,
    )
    _wait_ready(port, timeout=20, check_flash=True)
    return _StandaloneServer(port, proc, tmpdir)


def _probe_dict(resp) -> dict:
    """Convert flat ['key', 'val', ...] probe response to a dict."""
    it = iter(resp)
    return {
        (k.decode() if isinstance(k, bytes) else k):
        (v.decode() if isinstance(v, bytes) else v)
        for k, v in zip(it, it)
    }


def _owning_primary_port(key: str) -> int:
    """Return the external port (7001-7003) of the cluster primary that owns key's slot."""
    c = valkey.Valkey(host="localhost", port=7001, socket_timeout=5)
    try:
        slot = int(c.cluster("keyslot", key))
        slots_data = c.cluster("slots")
    finally:
        c.close()

    owner_nid = None
    for entry in slots_data:
        if int(entry[0]) <= slot <= int(entry[1]):
            nid = entry[2][2]
            owner_nid = nid.decode() if isinstance(nid, bytes) else nid
            break

    if owner_nid is None:
        return 7001

    for ext_port in _PRIMARY_PORTS:
        nc = valkey.Valkey(host="localhost", port=ext_port, socket_timeout=5)
        try:
            my_id = nc.cluster("myid")
            if isinstance(my_id, bytes):
                my_id = my_id.decode()
            if my_id == owner_nid:
                return ext_port
        except Exception:
            pass
        finally:
            nc.close()

    return 7001  # fallback


# ── Scenario 1: target has no flash module ────────────────────────────────────

@pytest.mark.docker_cluster
def test_probe_to_no_flash_target_returns_error(docker_cluster):
    """FLASH.MIGRATE.PROBE from a cluster primary to a plain Valkey node reports
    'does not have flash-module loaded'."""
    server = _start_plain_server()
    src = valkey.Valkey(host="localhost", port=7001, socket_timeout=10)
    try:
        with pytest.raises(valkey.exceptions.ResponseError) as exc_info:
            src.execute_command("FLASH.MIGRATE.PROBE", "127.0.0.1", str(server.port))
        error = str(exc_info.value)
        assert "does not have flash-module loaded" in error, (
            f"Unexpected error from probe: {error!r}"
        )
    finally:
        src.close()
        server.close()


@pytest.mark.docker_cluster
def test_migrate_to_no_flash_target_source_retains_key(docker_cluster):
    """MIGRATE of a FLASH.* key to a no-flash target fails; source retains ownership.

    Valkey's MIGRATE performs DUMP on source and RESTORE on target.  The target
    cannot deserialise the custom module type, so RESTORE fails.  Valkey only
    deletes the key from source after a successful RESTORE — so the key remains
    on source after the failure.
    """
    key = "{nf85}probe"
    server = _start_plain_server()
    try:
        # Write key on the cluster (routes to owning primary).
        docker_cluster.execute_command("FLASH.SET", key, "owner-check")

        # Connect directly to the owning primary and attempt MIGRATE.
        src_port = _owning_primary_port(key)
        src = valkey.Valkey(host="localhost", port=src_port, socket_timeout=10)
        try:
            with pytest.raises(valkey.exceptions.ResponseError):
                src.execute_command(
                    "MIGRATE", "127.0.0.1", str(server.port), key, 0, 10_000
                )
        finally:
            src.close()

        # Key must still be accessible on the cluster.
        val = docker_cluster.execute_command("FLASH.GET", key)
        assert val == b"owner-check", (
            f"Key lost after failed MIGRATE to no-flash target: {val!r}"
        )
    finally:
        server.close()
        try:
            docker_cluster.execute_command("FLASH.DEL", key)
        except Exception:
            pass


# ── Scenario 2: flash.path empty/unreachable — infeasible, no test ────────────
#
# flash.path is IMMUTABLE.  The module refuses to load without a valid, writable
# path — there is no reachable node state where flash is "loaded but path-less".
# See module docstring for rationale.


# ── Scenario 3: insufficient flash capacity ───────────────────────────────────

_1_MiB = 1 * 1024 * 1024  # minimum allowed flash.capacity-bytes


def _fill_flash_storage(port: int) -> int:
    """Write-and-demote FLASH.SET keys until NVMe free_bytes reaches 0 or FLASH.SET fails.

    Returns the number of keys successfully demoted (i.e. written to NVMe).
    Requires enable-debug-command yes on the target.
    """
    c = valkey.Valkey(host="127.0.0.1", port=port, socket_timeout=5)
    try:
        written = 0
        # Each key is ~4 KiB (one NVMe block per key in 4 KiB-block storage).
        # 1 MiB capacity = 256 blocks.  Write up to 300 keys to saturate storage.
        for i in range(300):
            try:
                c.execute_command("FLASH.SET", f"fill{i}", "x" * 4000)
            except Exception:
                break  # storage or cache full

            try:
                c.execute_command("FLASH.DEBUG.DEMOTE", f"fill{i}")
                written += 1
            except Exception:
                break  # NVMe full — no point writing more

        return written
    finally:
        c.close()


@pytest.mark.docker_cluster
def test_probe_to_full_flash_target_shows_low_free_bytes(docker_cluster):
    """FLASH.MIGRATE.PROBE to a nearly-full flash node reports near-zero free_bytes."""
    server = _start_flash_server(capacity_bytes=_1_MiB)
    try:
        keys_demoted = _fill_flash_storage(server.port)
        assert keys_demoted > 0, "Expected to demote at least some keys to Cold/NVMe tier"

        src = valkey.Valkey(host="localhost", port=7001, socket_timeout=10)
        try:
            resp = src.execute_command(
                "FLASH.MIGRATE.PROBE", "127.0.0.1", str(server.port)
            )
            info = _probe_dict(resp)

            assert "free_bytes" in info, f"probe response missing free_bytes: {info}"
            assert "capacity_bytes" in info, f"probe response missing capacity_bytes: {info}"

            free = int(info["free_bytes"])
            capacity = int(info["capacity_bytes"])

            # Target should have consumed ≥ 50% of NVMe capacity.
            assert free < capacity * 0.5, (
                f"Expected target to be at least 50%% full, "
                f"got free_bytes={free} / capacity={capacity} "
                f"({keys_demoted} keys demoted)"
            )
        finally:
            src.close()
    finally:
        server.close()


@pytest.mark.docker_cluster
def test_flash_migrate_rejects_migration_when_target_full():
    """FLASH.MIGRATE returns ERR FLASH-MIGRATE ... insufficient flash capacity when
    the target NVMe free_bytes is less than the key's estimated size (task #96).

    Uses two standalone flash servers (no cluster required): source has ample
    space; target is filled to capacity.  FLASH.MIGRATE on the source is expected
    to probe the target and reject the migration before forwarding to core MIGRATE.
    """
    _VALUE = "x" * 5000
    _KEY = "cap96:migtest"

    source = _start_flash_server(capacity_bytes=8 * _1_MiB)
    target = _start_flash_server(capacity_bytes=_1_MiB)
    try:
        _fill_flash_storage(target.port)

        src_c = valkey.Valkey(host="127.0.0.1", port=source.port, socket_timeout=10)
        try:
            src_c.execute_command("FLASH.SET", _KEY, _VALUE)

            with pytest.raises(valkey.exceptions.ResponseError) as exc_info:
                src_c.execute_command(
                    "FLASH.MIGRATE",
                    "127.0.0.1", str(target.port),
                    "",       # empty key — KEYS form
                    "0",      # destination-db
                    "5000",   # timeout ms
                    "KEYS", _KEY,
                )
            error = str(exc_info.value)
            assert "insufficient flash capacity" in error, (
                f"Expected capacity error, got: {error!r}"
            )
            # Key must still be readable on the source after the rejected migration.
            val = src_c.execute_command("FLASH.GET", _KEY)
            assert val == _VALUE.encode(), (
                f"Key should remain on source after rejected FLASH.MIGRATE; got {val!r}"
            )
        finally:
            src_c.close()
    finally:
        source.close()
        target.close()
