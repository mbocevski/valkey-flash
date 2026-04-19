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
from python_on_whales import DockerClient
from valkey import ValkeyCluster

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_SINGLE_COMPOSE = os.path.join(_REPO_ROOT, "docker", "compose.single.yml")
_CLUSTER_COMPOSE = os.path.join(_REPO_ROOT, "docker", "compose.cluster.yml")
_REPLICA_TIER_COMPOSE = os.path.join(_REPO_ROOT, "docker", "compose.cluster.replica-tier.yml")

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
    raise RuntimeError(f"Services {services} did not become healthy within {timeout}s.\n{logs}")


def flash_cluster_client(
    port: int = 7001, compose_project: str = "vf-cluster", **kwargs
) -> ValkeyCluster:
    """Construct a ValkeyCluster pointing at a docker-compose cluster, with
    the address-remap that lets the host reach nodes via `localhost:<host-port>`
    instead of the internal docker-bridge IPs/hostnames advertised by
    `CLUSTER SLOTS`. Extra kwargs are forwarded to `ValkeyCluster`."""
    kwargs.setdefault("socket_timeout", 10)
    return ValkeyCluster(
        host="localhost",
        port=port,
        address_remap=_build_address_remap(port, compose_project),
        **kwargs,
    )


def _build_address_remap(base_port: int, compose_project: str):
    """Return an `address_remap` callable that rewrites the cluster-internal
    addresses advertised by CLUSTER SLOTS back to `localhost:<host-port>`.

    `compose.cluster.yml` sets `--cluster-announce-hostname flash-primary-N`;
    the nodes advertise both that hostname AND their docker-bridge IP
    (172.18.x.x). Neither is reachable from the test process on the host —
    clients must talk to `localhost:700N`. Build a map of both shapes:
      - hostname → host-port (static, from compose layout)
      - container IP → host-port (discovered via `docker inspect`)"""
    import json
    import subprocess

    name_to_port = {
        "flash-primary-1": base_port,
        "flash-primary-2": base_port + 1,
        "flash-primary-3": base_port + 2,
        "flash-replica-1": base_port + 3,
        "flash-replica-2": base_port + 4,
        "flash-replica-3": base_port + 5,
    }

    ip_to_port: dict[str, int] = {}
    try:
        container_names = (
            subprocess.check_output(
                [
                    "docker",
                    "ps",
                    "--filter",
                    f"label=com.docker.compose.project={compose_project}",
                    "--format",
                    "{{.Names}}",
                ],
                timeout=5,
            )
            .decode()
            .split()
        )
        for cname in container_names:
            service = next((s for s in name_to_port if s in cname), None)
            if service is None:
                continue
            ip_json = (
                subprocess.check_output(
                    ["docker", "inspect", "-f", "{{json .NetworkSettings.Networks}}", cname],
                    timeout=5,
                )
                .decode()
                .strip()
            )
            for net_info in json.loads(ip_json).values():
                ip = net_info.get("IPAddress")
                if ip:
                    ip_to_port[ip] = name_to_port[service]
                    break
    except Exception:
        # Discovery failure degrades to hostname-only remap.
        pass

    def remap(addr):
        host, _ = addr
        if host in name_to_port:
            return ("localhost", name_to_port[host])
        if host in ip_to_port:
            return ("localhost", ip_to_port[host])
        return addr

    return remap


def _connect_cluster_with_retry(
    host: str, port: int, compose_project: str, timeout: float = 60
) -> ValkeyCluster:
    """Wait for the cluster to report `cluster_state:ok` on every node, then
    build a ValkeyCluster client with address remapping. Retries until all
    primaries and replicas have converged (gossip settles a few seconds after
    cluster-init exits)."""
    # Wait for cluster-state=ok on the entry node first via a plain client.
    plain_deadline = time.monotonic() + timeout
    last_exc: Exception = RuntimeError("timeout before first attempt")
    while time.monotonic() < plain_deadline:
        try:
            plain = valkey.Valkey(host=host, port=port, socket_timeout=5)
            info = plain.execute_command("CLUSTER INFO")
            state = info.get("cluster_state") if isinstance(info, dict) else None
            if state == "ok":
                break
            last_exc = RuntimeError(f"cluster_state={state}")
        except Exception as exc:
            last_exc = exc
        time.sleep(1)
    else:
        raise RuntimeError(f"Cluster at {host}:{port} not ok after {timeout}s: {last_exc}")

    # Build the discovery map *after* the cluster is up so every container is
    # running and has a stable IP.
    remap = _build_address_remap(port, compose_project)
    cluster_deadline = time.monotonic() + timeout
    while time.monotonic() < cluster_deadline:
        try:
            client = ValkeyCluster(
                host=host,
                port=port,
                socket_timeout=10,
                address_remap=remap,
            )
            client.ping()
            # Force a round-trip that hits every shard so cluster-side
            # discovery completes before the test's first write.
            client.execute_command("FLASH.SET", "{_boot_probe}", "1")
            client.execute_command("FLASH.DEL", "{_boot_probe}")
            return client
        except Exception as exc:
            last_exc = exc
            time.sleep(1)
    raise RuntimeError(f"Cluster at {host}:{port} not ready after {timeout}s: {last_exc}")


def _wait_cluster_init(docker: DockerClient, timeout: float = 120) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for c in docker.compose.ps(all=True):
            if "cluster-init" in c.name and c.state.status == "exited":
                if c.state.exit_code == 0:
                    return
                logs = docker.compose.logs()
                raise RuntimeError(f"cluster-init exited with code {c.state.exit_code}.\n{logs}")
        time.sleep(2)
    logs = docker.compose.logs()
    raise RuntimeError(f"cluster-init did not complete within {timeout}s.\n{logs}")


@pytest.fixture(scope="session")
def docker_single():
    if not _USE_DOCKER:
        pytest.skip("set USE_DOCKER=1 to run Docker-based tests")

    docker = DockerClient(compose_files=[_SINGLE_COMPOSE], compose_project_name="vf-single")
    docker.compose.up(detach=True, build=True, quiet=True)
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
    _REPLICAS = ["flash-replica-1", "flash-replica-2", "flash-replica-3"]

    docker = DockerClient(compose_files=[_CLUSTER_COMPOSE], compose_project_name="vf-cluster")
    docker.compose.up(detach=True, build=True, quiet=True)
    try:
        _all_healthy(docker, _PRIMARIES + _REPLICAS)
        _wait_cluster_init(docker)
        client = _connect_cluster_with_retry("localhost", 7001, "vf-cluster")
        # Let every replica's gossip finalize its primary pairing before any
        # test starts. `cluster-init` completing only guarantees slots are
        # covered; each node's CLUSTER NODES view of the other 5 nodes can
        # still show a few "handshake" flags for 2-3 seconds. Tests that kill
        # a primary and expect its paired replica to auto-promote need every
        # node's view consistent.
        time.sleep(3)
        yield client
    finally:
        docker.compose.down(volumes=True, timeout=10)


@pytest.fixture(scope="session")
def docker_cluster_replica_tier():
    """6-node cluster with flash.replica-tier-enabled=yes on all replica nodes.

    Uses ports 7011-7016 (primaries 7011-7013, replicas 7014-7016) so it can
    run alongside the default docker_cluster fixture without port conflicts.
    """
    if not _USE_DOCKER:
        pytest.skip("set USE_DOCKER=1 to run Docker-based tests")

    _PRIMARIES = ["flash-primary-1", "flash-primary-2", "flash-primary-3"]
    _REPLICAS = ["flash-replica-1", "flash-replica-2", "flash-replica-3"]

    docker = DockerClient(
        compose_files=[_CLUSTER_COMPOSE, _REPLICA_TIER_COMPOSE],
        compose_project_name="vf-cluster-rt",
    )
    docker.compose.up(detach=True, build=True, quiet=True)
    try:
        _all_healthy(docker, _PRIMARIES + _REPLICAS)
        _wait_cluster_init(docker)
        client = _connect_cluster_with_retry("localhost", 7011, "vf-cluster-rt")
        # Let every replica's gossip finalize before any test runs.
        # See docker_cluster() for rationale.
        time.sleep(3)
        yield client
    finally:
        docker.compose.down(volumes=True, timeout=10)
