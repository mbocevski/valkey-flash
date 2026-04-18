"""
Cluster-aware module init tests (Task #77).

Verifies that the flash module:
  1. Loads successfully on all 6 cluster nodes (3 primaries + 3 replicas).
  2. Reports flash_cluster_mode:yes in INFO flash on every node.
  3. Returns 'auto' for CONFIG GET flash.cluster-mode-enabled on every node.

Requires Docker: USE_DOCKER=1 pytest tests/test_flash_cluster_init.py -v
The docker_cluster fixture connects to the cluster via port 7001 by default;
we probe all 6 nodes directly via their individual ports (7001-7006).
"""

import pytest
import valkey


_CLUSTER_PORTS = (7001, 7002, 7003, 7004, 7005, 7006)


def _node_client(port):
    return valkey.Valkey(host="localhost", port=port, socket_timeout=5)


@pytest.mark.docker_cluster
def test_cluster_module_loads_on_all_nodes(docker_cluster):
    """Module is loaded and in Ready state on all 6 cluster nodes."""
    for port in _CLUSTER_PORTS:
        client = _node_client(port)
        try:
            info = client.info("flash")
            assert info.get("flash_module_state") == "ready", (
                f"Node on port {port}: flash_module_state is "
                f"{info.get('flash_module_state')!r}, expected 'ready'"
            )
        finally:
            client.close()


@pytest.mark.docker_cluster
def test_cluster_mode_reported_in_info_flash(docker_cluster):
    """INFO flash reports flash_cluster_mode:yes on every cluster node."""
    for port in _CLUSTER_PORTS:
        client = _node_client(port)
        try:
            info = client.info("flash")
            assert info.get("flash_cluster_mode") == "yes", (
                f"Node on port {port}: flash_cluster_mode is "
                f"{info.get('flash_cluster_mode')!r}, expected 'yes'"
            )
        finally:
            client.close()


@pytest.mark.docker_cluster
def test_cluster_mode_enabled_config_returns_auto(docker_cluster):
    """CONFIG GET flash.cluster-mode-enabled returns 'auto' on every cluster node."""
    for port in _CLUSTER_PORTS:
        client = _node_client(port)
        try:
            result = client.config_get("flash.cluster-mode-enabled")
            value = result.get("flash.cluster-mode-enabled")
            assert value == "auto", (
                f"Node on port {port}: flash.cluster-mode-enabled is {value!r}, "
                f"expected 'auto'"
            )
        finally:
            client.close()
