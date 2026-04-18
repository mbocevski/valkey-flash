"""
Session-scoped pytest fixtures for Docker-based integration tests.

Activated only when the environment variable USE_DOCKER=1 is set.
Without it, tests that request these fixtures are skipped automatically.

Usage:
    USE_DOCKER=1 pytest tests/test_docker_smoke.py -v
"""

import os
import time

import pytest
import valkey
from valkey import ValkeyCluster
from python_on_whales import DockerClient

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_SINGLE_COMPOSE = os.path.join(_REPO_ROOT, "docker", "compose.single.yml")
_CLUSTER_COMPOSE = os.path.join(_REPO_ROOT, "docker", "compose.cluster.yml")

_USE_DOCKER = os.environ.get("USE_DOCKER", "0") == "1"


def _service_healthy(docker: DockerClient, service: str) -> bool:
    for c in docker.compose.ps():
        if service in c.name:
            h = c.state.health
            if h and h.status == "healthy":
                return True
    return False


def _all_healthy(docker: DockerClient, services: list, timeout: float = 90) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if all(_service_healthy(docker, s) for s in services):
            return
        time.sleep(2)
    # Collect logs from unhealthy containers for the failure message.
    logs = docker.compose.logs()
    raise RuntimeError(
        f"Services {services} did not become healthy within {timeout}s.\n{logs}"
    )


def _connect_cluster_with_retry(host: str, port: int, timeout: float = 30) -> ValkeyCluster:
    deadline = time.monotonic() + timeout
    last_exc: Exception = RuntimeError("timeout before first attempt")
    while time.monotonic() < deadline:
        try:
            client = ValkeyCluster(host=host, port=port, socket_timeout=10)
            client.ping()
            return client
        except Exception as exc:
            last_exc = exc
            time.sleep(1)
    raise RuntimeError(f"Cluster at {host}:{port} not ready after {timeout}s: {last_exc}")


def _wait_cluster_init(docker: DockerClient, timeout: float = 120) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for c in docker.compose.ps(all=True):
            if "cluster-init" in c.name:
                if c.state.status == "exited":
                    if c.state.exit_code == 0:
                        return
                    logs = docker.compose.logs()
                    raise RuntimeError(
                        f"cluster-init exited with code {c.state.exit_code}.\n{logs}"
                    )
        time.sleep(2)
    logs = docker.compose.logs()
    raise RuntimeError(f"cluster-init did not complete within {timeout}s.\n{logs}")


@pytest.fixture(scope="session")
def docker_single():
    if not _USE_DOCKER:
        pytest.skip("set USE_DOCKER=1 to run Docker-based tests")

    docker = DockerClient(compose_files=[_SINGLE_COMPOSE], compose_project_name="vf-single")
    docker.compose.up(detach=True, build=True)
    try:
        _all_healthy(docker, ["valkey-flash"])
        client = valkey.Valkey(host="localhost", port=6379, socket_timeout=10)
        yield client
    finally:
        docker.compose.down(volumes=True, timeout=10)


@pytest.fixture(scope="session")
def docker_cluster():
    if not _USE_DOCKER:
        pytest.skip("set USE_DOCKER=1 to run Docker-based tests")

    _PRIMARIES = ["flash-primary-1", "flash-primary-2", "flash-primary-3"]
    _REPLICAS  = ["flash-replica-1", "flash-replica-2", "flash-replica-3"]

    docker = DockerClient(compose_files=[_CLUSTER_COMPOSE], compose_project_name="vf-cluster")
    docker.compose.up(detach=True, build=True)
    try:
        _all_healthy(docker, _PRIMARIES + _REPLICAS)
        _wait_cluster_init(docker)
        client = _connect_cluster_with_retry("localhost", 7001)
        yield client
    finally:
        docker.compose.down(volumes=True, timeout=10)
