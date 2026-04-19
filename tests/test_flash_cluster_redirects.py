"""
Redirect-safe dispatch verification for all FLASH.* commands (Task #81).

Valkey core handles MOVED/ASK redirects for module commands automatically as long
as each command's key_spec (first_key, last_key, step) is registered correctly.
All FLASH.* key-bearing commands are registered with first_key=1, last_key=1,
step=1 (FLASH.DEL uses last_key=-1 for its variadic tail) — Valkey can therefore
compute the correct hash slot and emit the right MOVED target.

Tests use two approaches:
  1. valkey-py ValkeyCluster (cluster-aware client): commands succeed transparently
     regardless of which node receives them first.  This is the primary
     correctness signal — if a command's key_spec is wrong, Valkey either routes
     to the wrong slot (and the command fails on the target) or refuses to proxy.
  2. Single-node direct client: issuing a command for a key on a different node
     raises a MOVED error, confirming the server computes the redirect correctly
     rather than executing on the wrong node.

Requires Docker: USE_DOCKER=1 pytest tests/test_flash_cluster_redirects.py -v
"""

from contextlib import suppress

import pytest

try:
    import valkey
    import valkey.exceptions

    try:
        from valkey.cluster import ValkeyCluster

        _HAVE_CLUSTER_CLIENT = True
    except ImportError:
        _HAVE_CLUSTER_CLIENT = False
    _HAVE_VALKEY = True
except ImportError:
    _HAVE_VALKEY = False
    _HAVE_CLUSTER_CLIENT = False


_SEED_PORT = 7001
_PRIMARY_PORTS = (7001, 7002, 7003)

# Keys chosen so they hash to different slots (verified via CRC-16/XMODEM mod 16384):
#   slot({alpha}) = 7638   slot({beta}) = 7753   slot({gamma}) = 8017
# Using hash tags ensures predictable slots independent of key suffix.
_KEY_A = "{alpha}:test"
_KEY_B = "{beta}:test"
_HASH_KEY = "{gamma}:hash"


def _cluster_client():
    """Return a ValkeyCluster client seeded at port 7001."""
    from docker_fixtures import flash_cluster_client

    return flash_cluster_client(port=_SEED_PORT)


def _direct_client(port):
    return valkey.Valkey(host="localhost", port=port, socket_timeout=5)


def _cleanup(client, *keys):
    """Best-effort delete keys via cluster client."""
    for k in keys:
        with suppress(Exception):
            client.execute_command("FLASH.DEL", k)


# ── Cluster-client tests (transparent redirect) ───────────────────────────────


@pytest.mark.docker_cluster
@pytest.mark.skipif(not _HAVE_CLUSTER_CLIENT, reason="ValkeyCluster not available")
def test_flash_set_get_redirect(docker_cluster):
    """FLASH.SET / FLASH.GET succeed via transparent MOVED redirect."""
    with _cluster_client() as c:
        c.execute_command("FLASH.SET", _KEY_A, "hello")
        val = c.execute_command("FLASH.GET", _KEY_A)
        assert val == b"hello", f"expected b'hello', got {val!r}"
        _cleanup(c, _KEY_A)


@pytest.mark.docker_cluster
@pytest.mark.skipif(not _HAVE_CLUSTER_CLIENT, reason="ValkeyCluster not available")
def test_flash_del_redirect(docker_cluster):
    """FLASH.DEL succeeds via transparent MOVED redirect."""
    with _cluster_client() as c:
        c.execute_command("FLASH.SET", _KEY_A, "todelete")
        deleted = c.execute_command("FLASH.DEL", _KEY_A)
        assert int(deleted) == 1, f"expected 1 deleted key, got {deleted}"


@pytest.mark.docker_cluster
@pytest.mark.skipif(not _HAVE_CLUSTER_CLIENT, reason="ValkeyCluster not available")
def test_flash_hset_hget_redirect(docker_cluster):
    """FLASH.HSET / FLASH.HGET succeed via transparent MOVED redirect."""
    with _cluster_client() as c:
        c.execute_command("FLASH.HSET", _HASH_KEY, "field1", "value1")
        val = c.execute_command("FLASH.HGET", _HASH_KEY, "field1")
        assert val == b"value1", f"expected b'value1', got {val!r}"
        _cleanup(c, _HASH_KEY)


@pytest.mark.docker_cluster
@pytest.mark.skipif(not _HAVE_CLUSTER_CLIENT, reason="ValkeyCluster not available")
def test_flash_hgetall_redirect(docker_cluster):
    """FLASH.HGETALL returns all fields via transparent MOVED redirect."""
    with _cluster_client() as c:
        c.execute_command("FLASH.HSET", _HASH_KEY, "f1", "v1", "f2", "v2")
        result = c.execute_command("FLASH.HGETALL", _HASH_KEY)
        assert result is not None and len(result) >= 4, (
            f"FLASH.HGETALL returned unexpected result: {result!r}"
        )
        _cleanup(c, _HASH_KEY)


@pytest.mark.docker_cluster
@pytest.mark.skipif(not _HAVE_CLUSTER_CLIENT, reason="ValkeyCluster not available")
def test_flash_hdel_redirect(docker_cluster):
    """FLASH.HDEL removes a field via transparent MOVED redirect."""
    with _cluster_client() as c:
        c.execute_command("FLASH.HSET", _HASH_KEY, "field", "value")
        removed = c.execute_command("FLASH.HDEL", _HASH_KEY, "field")
        assert int(removed) == 1, f"expected 1 removed field, got {removed}"
        _cleanup(c, _HASH_KEY)


@pytest.mark.docker_cluster
@pytest.mark.skipif(not _HAVE_CLUSTER_CLIENT, reason="ValkeyCluster not available")
def test_flash_hexists_redirect(docker_cluster):
    """FLASH.HEXISTS returns correct result via transparent MOVED redirect."""
    with _cluster_client() as c:
        c.execute_command("FLASH.HSET", _HASH_KEY, "present", "yes")
        exists = c.execute_command("FLASH.HEXISTS", _HASH_KEY, "present")
        assert int(exists) == 1, f"expected 1, got {exists}"
        missing = c.execute_command("FLASH.HEXISTS", _HASH_KEY, "absent")
        assert int(missing) == 0, f"expected 0, got {missing}"
        _cleanup(c, _HASH_KEY)


@pytest.mark.docker_cluster
@pytest.mark.skipif(not _HAVE_CLUSTER_CLIENT, reason="ValkeyCluster not available")
def test_flash_hlen_redirect(docker_cluster):
    """FLASH.HLEN returns field count via transparent MOVED redirect."""
    with _cluster_client() as c:
        c.execute_command("FLASH.HSET", _HASH_KEY, "a", "1", "b", "2", "c", "3")
        length = c.execute_command("FLASH.HLEN", _HASH_KEY)
        assert int(length) == 3, f"expected 3, got {length}"
        _cleanup(c, _HASH_KEY)


# ── Direct single-node test: verify MOVED is returned, not an error ───────────


@pytest.mark.docker_cluster
@pytest.mark.skipif(not _HAVE_VALKEY, reason="valkey package not installed")
def test_flash_set_wrong_node_returns_moved(docker_cluster):
    """FLASH.SET on the wrong node returns MOVED, not an application error.

    Connects directly to each primary and issues FLASH.SET for _KEY_A.
    Exactly one node owns the slot — that node succeeds; the others return
    MOVED.  If a node were to return a generic ERR (e.g., because key_spec
    is wrong), that would be a bug.
    """
    moved_count = 0
    success_count = 0

    for port in _PRIMARY_PORTS:
        client = _direct_client(port)
        try:
            client.execute_command("FLASH.SET", _KEY_A, "probe")
            success_count += 1
            # Clean up on the owning node.
            with suppress(Exception):
                client.execute_command("FLASH.DEL", _KEY_A)
        except valkey.exceptions.ResponseError as e:
            err = str(e)
            assert err.startswith("MOVED"), (
                f"Expected MOVED redirect from port {port}, got: {err!r}"
            )
            moved_count += 1
        finally:
            client.close()

    assert success_count == 1, f"Expected exactly 1 owning node, got {success_count}"
    assert moved_count == len(_PRIMARY_PORTS) - 1, (
        f"Expected {len(_PRIMARY_PORTS) - 1} MOVED redirects, got {moved_count}"
    )
