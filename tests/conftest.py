import contextlib
import importlib
import os
import shutil
import subprocess
import sys

import pytest

_tests_dir = os.path.abspath(os.path.dirname(__file__))
_repo_root = os.path.dirname(_tests_dir)

# Make test helpers and the vendored test framework importable
sys.path.insert(0, _tests_dir)
sys.path.insert(0, os.path.join(_tests_dir, "build"))
sys.path.insert(0, os.path.join(_tests_dir, "build/valkeytestframework"))


def _autodetect_test_env() -> None:
    """Populate MODULE_PATH / LD_LIBRARY_PATH from the repo layout when unset.

    CI exports both explicitly, so this is a no-op there. Locally, running
    `uv run pytest tests/` without going through build.sh is easy to get
    wrong — a missing MODULE_PATH becomes the literal string 'None' in the
    loadmodule arg, so valkey-server fails to start and tests fail with an
    opaque 'server not ready' after a 90-second timeout. Auto-detecting
    avoids that footgun.

    SERVER_VERSION must be set explicitly (we don't guess which matrix entry
    the user wants); we only resolve paths that depend on it.
    """
    if not os.environ.get("MODULE_PATH"):
        candidate = os.path.join(_repo_root, "target", "release", "libvalkey_flash.so")
        if os.path.exists(candidate):
            os.environ["MODULE_PATH"] = candidate

    server_version = os.environ.get("SERVER_VERSION")
    if server_version:
        binaries_dir = os.path.join(_tests_dir, "build", "binaries", server_version)
        if os.path.isdir(binaries_dir):
            existing = os.environ.get("LD_LIBRARY_PATH", "")
            # Prepend if not already present so valkey-server finds its
            # shared libs (libvalkey.so etc.) without LD_LIBRARY_PATH tricks.
            if binaries_dir not in existing.split(":"):
                os.environ["LD_LIBRARY_PATH"] = (
                    f"{binaries_dir}:{existing}" if existing else binaries_dir
                )


_autodetect_test_env()

# resource_port_tracker is defined in the vendored testframework's conftest.py, which
# lives in a subdirectory and therefore isn't automatically visible to tests in tests/.
# Importing it here makes it available to every test in this directory tree.
_vtf_conftest = importlib.import_module("valkeytestframework.conftest")
resource_port_tracker = _vtf_conftest.resource_port_tracker

# Register Docker-based fixtures when USE_DOCKER=1.
if os.environ.get("USE_DOCKER", "0") == "1":
    _docker = importlib.import_module("docker_fixtures")
    docker_cluster = _docker.docker_cluster
    docker_cluster_replica_tier = _docker.docker_cluster_replica_tier
    docker_single = _docker.docker_single


def pytest_collection_modifyitems(config, items):
    if os.environ.get("USE_DOCKER") != "1":
        skip_docker = pytest.mark.skip(reason="USE_DOCKER=1 not set")
        for item in items:
            if any(m.name.startswith("docker_") for m in item.iter_markers()):
                item.add_marker(skip_docker)


def _reap_leftover_test_state() -> None:
    """Remove stale test-data files and lingering valkey-server processes from
    previously interrupted runs.

    The valkeytestframework cleans up per-server artifacts (logfile, rdb, aof
    directory) in `ValkeyServerHandle.exit()`, which only runs when pytest
    unwinds the autouse fixtures cleanly. Ctrl-C, SIGKILL, CI timeouts, or any
    crash before teardown leaves hundreds to thousands of stale files in
    `test-data/` plus the server processes themselves (whose cwd remains that
    directory).

    When the next session starts, enough accumulated state in `test-data/`
    manifests as 90-second `wait_for_ready_to_accept_connections` timeouts on
    otherwise-healthy fixtures (observed on `TestFlashMigrateProbeNoFlash`
    with its two-server setup: 2/3 runs hit the 181s timeout once test-data
    grows past ~2k entries). Wiping at session start removes the class of
    failure altogether.

    Kept strictly at session scope: inside a session the framework's own
    fixtures are authoritative. Runs only for the primary pytest process
    (`PYTEST_XDIST_WORKER` absent); xdist workers must not race the wipe.

    Set `KEEP_TEST_DATA=1` to preserve state across sessions for debugging.
    """
    if os.environ.get("KEEP_TEST_DATA") == "1":
        return
    if os.environ.get("PYTEST_XDIST_WORKER"):
        return

    test_data_dir = os.path.join(_repo_root, "test-data")
    if os.path.isdir(test_data_dir):
        try:
            entries = os.listdir(test_data_dir)
        except OSError:
            entries = []
        if entries:
            shutil.rmtree(test_data_dir, ignore_errors=True)
            with contextlib.suppress(OSError):
                os.makedirs(test_data_dir, exist_ok=True)

    # Kill any orphaned valkey-server processes that have our test-data/ as
    # cwd. A plain pgrep-all would kill a developer's local valkey:6379.
    try:
        pids = subprocess.run(
            ["pgrep", "-f", "valkey-server"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        ).stdout.split()
    except (OSError, subprocess.TimeoutExpired):
        return

    for pid in pids:
        try:
            cwd = os.readlink(f"/proc/{pid}/cwd")
        except OSError:
            continue
        if os.path.abspath(cwd) == os.path.abspath(test_data_dir):
            with contextlib.suppress(OSError, ValueError):
                os.kill(int(pid), 9)


_reap_leftover_test_state()


# ── Docker cluster health gate ────────────────────────────────────────────────
#
# Failover / resharding tests kill cluster nodes as part of their scenarios.
# Their own teardown attempts `docker start` + `CLUSTER FAILOVER TAKEOVER` to
# restore topology, but the timing is fragile — a failed recovery used to
# cascade into 19 unrelated ConnectionRefusedError failures on subsequent
# tests that default-target port 7001. This hook runs before each
# `docker_cluster` / `docker_cluster_replica_tier` test, probes every port the
# fixture advertises, and either:
#
#   1. All ports reachable → no-op, test runs.
#   2. Some ports unreachable → `docker start` the matching containers and
#      wait for health. If recovery succeeds, the test runs.
#   3. Recovery fails → `pytest.skip()` with a message identifying the dead
#      port. Converts a cascade of misleading failures into a single clear
#      skip per broken container.


_CLUSTER_PROJECT_PORTS = {
    # marker → (compose project, first-primary port)
    "docker_cluster": ("vf-cluster", 7001),
    "docker_cluster_replica_tier": ("vf-cluster-rt", 7011),
}


def _port_reachable(port: int, timeout: float = 2.0) -> bool:
    import socket as _socket

    try:
        with _socket.create_connection(("127.0.0.1", port), timeout=timeout) as s:
            s.sendall(b"*1\r\n$4\r\nPING\r\n")
            data = s.recv(64)
            return b"+PONG" in data
    except Exception:
        return False


def _compose_project_exists(project: str) -> bool:
    """True if the compose project has any containers tracked (running or not).

    Used to guard `_recover_unhealthy_cluster` from running before the session
    fixture has brought the project up for the first time. Without this guard,
    `_recover_unhealthy_cluster` would see every port unreachable, call
    `docker start` against non-existent container names (silently no-ops), and
    skip every test — preventing the session fixture from ever running.
    """
    import subprocess

    try:
        result = subprocess.run(
            ["docker", "compose", "-p", project, "ps", "-a", "-q"],
            timeout=5,
            capture_output=True,
            text=True,
            check=False,
        )
        return bool(result.stdout.strip())
    except Exception:
        return False


def _recover_unhealthy_cluster(project: str, base_port: int, timeout: float = 30.0) -> list[int]:
    """Attempt to bring any unreachable cluster container back up. Returns the
    list of ports still unreachable after the recovery window."""
    import subprocess
    import time as _time

    name_to_port = {
        f"{project}-flash-primary-1-1": base_port,
        f"{project}-flash-primary-2-1": base_port + 1,
        f"{project}-flash-primary-3-1": base_port + 2,
        f"{project}-flash-replica-1-1": base_port + 3,
        f"{project}-flash-replica-2-1": base_port + 4,
        f"{project}-flash-replica-3-1": base_port + 5,
    }
    unhealthy = {port: cname for cname, port in name_to_port.items() if not _port_reachable(port)}
    if not unhealthy:
        return []

    for _port, cname in unhealthy.items():
        with contextlib.suppress(Exception):
            subprocess.run(
                ["docker", "start", cname],
                timeout=10,
                capture_output=True,
                check=False,
            )

    deadline = _time.monotonic() + timeout
    still_down: list[int] = list(unhealthy.keys())
    while _time.monotonic() < deadline and still_down:
        still_down = [p for p in still_down if not _port_reachable(p)]
        if not still_down:
            break
        _time.sleep(1)
    return still_down


def pytest_runtest_setup(item):
    if os.environ.get("USE_DOCKER") != "1":
        return
    for marker_name, (project, base_port) in _CLUSTER_PROJECT_PORTS.items():
        if not any(m.name == marker_name for m in item.iter_markers()):
            continue
        # Skip the health gate if the compose project doesn't exist yet —
        # the session-scoped fixture brings it up on first test. Without
        # this guard the hook would see all 6 ports unreachable, attempt
        # `docker start` against non-existent containers (silently failing),
        # and skip every test before the fixture ever runs.
        if not _compose_project_exists(project):
            return
        still_down = _recover_unhealthy_cluster(project, base_port)
        if still_down:
            pytest.skip(
                f"docker_cluster health gate: ports {still_down} on project "
                f"'{project}' unreachable after recovery attempt; a prior test "
                f"left a container down and `docker start` did not revive it "
                f"within the window."
            )


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    """Dump compose logs for every known project on docker-test failure.

    The Docker-integration fixture is session-scoped; its `docker.compose.down()`
    teardown runs at pytest exit, BEFORE the GitHub Actions post-pytest
    log-collection step. That's why `docker-logs-*` artifacts have been coming
    back as ~138-byte empty files: containers are gone before the workflow
    reaches `docker compose logs`.

    Running the log dump here — inside pytest, on test failure, before the
    session teardown — captures live container state. Output goes to
    `test-data/compose-<project>-<test>.log`; the workflow's existing artifact
    upload already picks up anything under `test-data/`.
    """
    outcome = yield
    report = outcome.get_result()
    if report.when != "call" or not report.failed:
        return
    if not any(m.name.startswith("docker_") for m in item.iter_markers()):
        return
    if os.environ.get("USE_DOCKER") != "1":
        return

    import subprocess

    os.makedirs("test-data", exist_ok=True)
    safe_name = item.nodeid.replace("/", "_").replace("::", "-")
    for project in ("vf-single", "vf-cluster", "vf-cluster-rt"):
        log_path = f"test-data/compose-{project}-{safe_name}.log"
        ps_path = f"test-data/compose-{project}-{safe_name}.ps"
        try:
            with open(log_path, "w") as f:
                subprocess.run(
                    ["docker", "compose", "-p", project, "logs", "--no-color", "--tail=500"],
                    stdout=f,
                    stderr=subprocess.STDOUT,
                    timeout=30,
                    check=False,
                )
            with open(ps_path, "w") as f:
                subprocess.run(
                    ["docker", "compose", "-p", project, "ps", "-a"],
                    stdout=f,
                    stderr=subprocess.STDOUT,
                    timeout=15,
                    check=False,
                )
        except Exception:
            # Diagnostic path: a failure here must not mask the original test
            # failure. Swallow and move on.
            pass
