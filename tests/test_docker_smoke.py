"""
Docker smoke tests.

Run with:
    USE_DOCKER=1 pytest tests/test_docker_smoke.py -v
"""

import valkey
import pytest


@pytest.mark.docker_single
def test_docker_single(docker_single):
    client = docker_single

    # valkey-py 6.x returns MODULE LIST as a list of dicts
    modules = client.execute_command("MODULE LIST")
    names = [m[b"name"] for m in modules]
    assert b"flash" in names, f"flash module not loaded; MODULE LIST={modules}"

    assert client.execute_command("FLASH.SET", "smoke:key", "smoke:val") == b"OK"
    assert client.execute_command("FLASH.GET", "smoke:key") == b"smoke:val"


@pytest.mark.docker_cluster
def test_docker_cluster(docker_cluster):
    client = docker_cluster

    # Use hash tags to pin keys to the same slot for a single-hop roundtrip.
    assert client.execute_command("FLASH.SET", "{smoke}key", "smoke:val") == b"OK"
    assert client.execute_command("FLASH.GET", "{smoke}key") == b"smoke:val"

    # Verify cluster health via a plain client to one primary.
    # valkey-py 6.x parses CLUSTER INFO into a dict.
    plain = valkey.Valkey(host="localhost", port=7001, socket_timeout=10)
    info = plain.execute_command("CLUSTER INFO")
    assert info["cluster_state"] == "ok"
    assert info["cluster_size"] == "3"
