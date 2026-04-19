import importlib
import os
import sys

import pytest

_tests_dir = os.path.abspath(os.path.dirname(__file__))

# Make test helpers and the vendored test framework importable
sys.path.insert(0, _tests_dir)
sys.path.insert(0, os.path.join(_tests_dir, "build"))
sys.path.insert(0, os.path.join(_tests_dir, "build/valkeytestframework"))

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
