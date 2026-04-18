import sys
import os

_tests_dir = os.path.abspath(os.path.dirname(__file__))

# Make test helpers and the vendored test framework importable
sys.path.insert(0, _tests_dir)
sys.path.insert(0, os.path.join(_tests_dir, "build"))
sys.path.insert(0, os.path.join(_tests_dir, "build/valkeytestframework"))

# Register Docker-based fixtures when USE_DOCKER=1.
if os.environ.get("USE_DOCKER", "0") == "1":
    from docker_fixtures import docker_single, docker_cluster  # noqa: F401
