import importlib
import os
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
