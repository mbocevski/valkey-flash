"""
Cluster failover integration tests (Task #86).

Two @slow @docker_cluster tests that verify replica promotion + post-promotion
write correctness:

  Variant A — standard cluster (replica-tier-enabled=false, default):
    Replica uses lazy NVMe init on promotion (task #64).
    Uses docker_cluster fixture (ports 7001-7006).

  Variant B — replica-tier-enabled cluster:
    Replica already has its NVMe backend open before promotion, so writes are
    accepted immediately without lazy init.
    Uses docker_cluster_replica_tier fixture (ports 7011-7016).

Scenario (both variants):
  1. Populate 30 FLASH.* keys across all 3 primaries.
  2. Hard-kill primary-1 (docker kill — simulates an unclean crash).
  3. Poll CLUSTER NODES from a surviving primary until the replica of the
     killed primary reports "master" (≤ 30 s given 5 000 ms cluster-node-timeout).
  4. Assert: FLASH.SET on the new primary succeeds immediately (validates #64).
  5. Assert: all 30 pre-failover keys are readable (no data loss for acked writes).
  6. Assert: 5 new post-promotion writes are readable.
  7. Assert: CLUSTER INFO shows cluster_state=ok.
  8. Cleanup: docker start <container> so the session-scoped fixture's cluster
     recovers before subsequent tests run.

Budget: ~60 s per run.

Run (requires USE_DOCKER=1):
    USE_DOCKER=1 pytest tests/test_flash_cluster_failover.py -v -m "docker_cluster and slow"
"""

import subprocess
import time
from contextlib import suppress

import pytest
import valkey
import valkey.exceptions
from valkey.cluster import ValkeyCluster

# ── Helpers ───────────────────────────────────────────────────────────────────


def _node_id(port: int) -> str:
    c = valkey.Valkey(host="localhost", port=port, socket_timeout=5)
    try:
        nid = c.cluster("myid")
        return nid.decode() if isinstance(nid, bytes) else nid
    finally:
        c.close()


def _slot_ranges_for_node(node_id: str, seed_port: int) -> list[tuple[int, int]]:
    """Return slot ranges owned by the given node ID."""
    c = valkey.Valkey(host="localhost", port=seed_port, socket_timeout=5)
    try:
        ranges = []
        for entry in c.cluster("slots"):
            nid = entry[2][2]
            if isinstance(nid, bytes):
                nid = nid.decode()
            if nid == node_id:
                ranges.append((int(entry[0]), int(entry[1])))
        return ranges
    finally:
        c.close()


def _cluster_nodes_as_list(nodes) -> list[dict]:
    """Normalize `c.cluster("nodes")` output across valkey-py versions.

    valkey-py 6.x returns a dict `{addr: {node_id, flags, master_id, ...}}`;
    older versions return the raw bytes of the CLUSTER NODES output. This
    helper produces a list of dicts with canonical keys: id, flags, master_id."""
    if isinstance(nodes, dict):
        return [
            {
                "id": entry.get("node_id", ""),
                "flags": entry.get("flags", ""),
                "master_id": entry.get("master_id", "-"),
            }
            for entry in nodes.values()
        ]
    text = nodes.decode() if isinstance(nodes, (bytes, bytearray)) else str(nodes)
    out = []
    for line in text.strip().split("\n"):
        parts = line.split()
        if len(parts) >= 4:
            out.append({"id": parts[0], "flags": parts[2], "master_id": parts[3]})
    return out


def _find_replica_port(primary_port: int, replica_ports: tuple[int, ...]) -> int | None:
    """Return the external port of the replica that replicates from primary_port."""
    primary_id = _node_id(primary_port)
    for rep_port in replica_ports:
        c = valkey.Valkey(host="localhost", port=rep_port, socket_timeout=5)
        try:
            my_id_raw = c.cluster("myid")
            my_id = my_id_raw.decode() if isinstance(my_id_raw, bytes) else my_id_raw
            for n in _cluster_nodes_as_list(c.cluster("nodes")):
                if (
                    n["id"] == my_id
                    and "slave" in n["flags"]
                    and n["master_id"] == primary_id
                ):
                    return rep_port
        except Exception:
            pass
        finally:
            c.close()
    return None


def _wait_for_promotion(
    survivor_port: int,
    replica_port: int,
    timeout: float = 30.0,
) -> bool:
    """Poll CLUSTER NODES from survivor_port until replica_port reports 'master'."""
    replica_id = _node_id(replica_port)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            c = valkey.Valkey(host="localhost", port=survivor_port, socket_timeout=5)
            try:
                for n in _cluster_nodes_as_list(c.cluster("nodes")):
                    if (
                        n["id"] == replica_id
                        and "master" in n["flags"]
                        and "fail" not in n["flags"]
                    ):
                        return True
            finally:
                c.close()
        except Exception:
            pass
        time.sleep(0.5)
    return False


def _wait_for_cluster_stable(seed_port: int, timeout: float = 30.0) -> bool:
    """Poll CLUSTER INFO until cluster_state=ok."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            c = valkey.Valkey(host="localhost", port=seed_port, socket_timeout=5)
            try:
                raw = c.cluster("info")
                # valkey-py 6.x returns a dict; older versions a text buffer.
                if isinstance(raw, dict):
                    if raw.get("cluster_state") == "ok":
                        return True
                else:
                    text = raw.decode() if isinstance(raw, bytes) else str(raw)
                    if "cluster_state:ok" in text:
                        return True
            finally:
                c.close()
        except Exception:
            pass
        time.sleep(1)
    return False


def _new_cluster_client(seed_port: int) -> ValkeyCluster:
    """Create a fresh ValkeyCluster client (fetches current topology)."""
    from docker_fixtures import flash_cluster_client

    project = "vf-cluster-rt" if seed_port >= 7011 else "vf-cluster"
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        try:
            c = flash_cluster_client(port=seed_port, compose_project=project)
            c.ping()
            return c
        except Exception:
            time.sleep(1)
    raise RuntimeError(f"Could not connect ValkeyCluster to seed port {seed_port}")


# ── Shared scenario ───────────────────────────────────────────────────────────


def _run_failover_scenario(
    seed_port: int,
    killed_port: int,
    survivor_port: int,
    replica_ports: tuple[int, ...],
    project_name: str,
) -> None:
    """
    Core failover test logic, shared by both standard and replica-tier variants.

    Args:
        seed_port:      Port for initial cluster operations (same as killed_port).
        killed_port:    External port of the primary to kill (port of primary-1).
        survivor_port:  External port of a surviving primary (used as fallback seed).
        replica_ports:  External ports of all replica nodes.
        project_name:   Docker Compose project name (for container naming).
    """
    container_name = f"{project_name}-flash-primary-1-1"

    # ── Step 1: Pre-failover population ──────────────────────────────────────
    # Write 30 keys.  Keys use varied hash tags so they land on different slots
    # and span all three primaries.
    pre_keys: dict[str, str] = {}
    write_client = _new_cluster_client(seed_port)
    try:
        for i in range(30):
            key = f"{{fail86:{i}}}k"
            val = f"pre:{i}"
            write_client.execute_command("FLASH.SET", key, val)
            pre_keys[key] = val
        # Also write some hash keys.
        write_client.execute_command("FLASH.HSET", "{fail86:h}k", "f", "hval")
        pre_keys["{fail86:h}k:hf"] = "hval"  # marker — checked separately
    finally:
        write_client.close()

    # Find the replica of the primary we're about to kill.
    replica_port = _find_replica_port(killed_port, replica_ports)
    assert replica_port is not None, f"Could not find a replica of primary on port {killed_port}"

    # Wait for the cluster's gossip state to fully converge before the kill.
    # A freshly-brought-up compose cluster has `cluster_state:ok` on the seed
    # node the moment `cluster-init` exits, but every node's view of *every*
    # other node's role is not consistent for a few seconds after that. A
    # failover election started against an inconsistent view takes far longer
    # to propagate the new-primary news back to `survivor_port`, and the 60 s
    # poll window runs out.
    assert _wait_for_cluster_stable(seed_port, timeout=30.0), (
        f"cluster at seed port {seed_port} not stable before failover trigger"
    )

    # ── Step 2: Hard-kill primary-1 ───────────────────────────────────────────
    subprocess.run(["docker", "kill", container_name], check=True, capture_output=True)

    # ── Step 3: Wait for replica promotion ────────────────────────────────────
    promoted = _wait_for_promotion(
        survivor_port=survivor_port,
        replica_port=replica_port,
        timeout=120.0,
    )
    assert promoted, (
        f"Replica on port {replica_port} did not promote to master within 120 s "
        f"(cluster-node-timeout=5000ms)"
    )

    # ── Step 4: Post-promotion write on new primary ───────────────────────────
    # Fresh cluster client picks up the new topology.
    post_client = _new_cluster_client(survivor_port)
    try:
        # This validates task #64: lazy NVMe init on the promoted node.
        post_key = "{fail86:new}k"
        post_client.execute_command("FLASH.SET", post_key, "post-promotion")
        val = post_client.execute_command("FLASH.GET", post_key)
        assert val == b"post-promotion", f"FLASH.SET/GET failed on promoted primary: got {val!r}"

        # ── Step 5: Pre-failover data survives ────────────────────────────────
        losses = []
        for key, expected in pre_keys.items():
            if key.endswith(":hf"):
                # Hash key — skip in this loop (checked below).
                continue
            try:
                actual = post_client.execute_command("FLASH.GET", key)
                if actual is None:
                    losses.append(f"{key!r}: missing")
                elif actual.decode() != expected:
                    losses.append(f"{key!r}: expected={expected!r} got={actual.decode()!r}")
            except Exception as e:
                losses.append(f"{key!r}: error={e}")

        assert not losses, f"{len(losses)} pre-failover key(s) lost or mismatched:\n" + "\n".join(
            f"  {loss}" for loss in losses[:10]
        )

        # Verify the hash key survived.
        hval = post_client.execute_command("FLASH.HGET", "{fail86:h}k", "f")
        assert hval == b"hval", f"FLASH.HGET after failover: {hval!r}"

        # ── Step 6: New post-promotion writes ─────────────────────────────────
        new_keys = {}
        for i in range(5):
            nk = f"{{fail86:post:{i}}}k"
            post_client.execute_command("FLASH.SET", nk, f"new:{i}")
            new_keys[nk] = f"new:{i}"

        for key, expected in new_keys.items():
            actual = post_client.execute_command("FLASH.GET", key)
            assert actual is not None and actual.decode() == expected, (
                f"Post-promotion key {key!r} not readable: {actual!r}"
            )

        # ── Step 7: Cluster health check ──────────────────────────────────────
        # The cluster may still show degraded state (missing replica for the
        # failed-over shard) — cluster_state:ok means no unreachable slots.
        stable = _wait_for_cluster_stable(survivor_port, timeout=10.0)
        assert stable, "CLUSTER INFO did not show cluster_state:ok after failover"

    finally:
        post_client.close()

        # ── Cleanup: restart the killed container ─────────────────────────────
        # Best-effort — ensures the session-scoped cluster fixture leaves the
        # stack in a recoverable state for subsequent tests.
        with suppress(Exception):
            subprocess.run(["docker", "start", container_name], check=True, capture_output=True)

        # ── Restore topology: hand the primary role back to the original node ─
        # After docker-start, the resurrected container rejoins as a replica of
        # the node that was promoted during the test. Subsequent tests that
        # iterate primaries 7001/7002/7003 assume the original layout, so
        # flip the role back by forcing a CLUSTER FAILOVER TAKEOVER on the
        # container that just came back up.
        _restore_original_primary_role(killed_port=killed_port, timeout=30.0)


def _restore_original_primary_role(killed_port: int, timeout: float = 30.0) -> None:
    """After a failover test, reclaim the primary role for the container on
    `killed_port` so the session-scoped cluster fixture is usable by the next
    test. Uses CLUSTER FAILOVER TAKEOVER on the original primary; the node
    that was promoted during the test demotes itself back to a replica."""
    deadline = time.monotonic() + timeout
    # Wait for the restarted container to be reachable.
    while time.monotonic() < deadline:
        try:
            c = valkey.Valkey(host="localhost", port=killed_port, socket_timeout=3)
            if c.ping():
                break
        except Exception:
            time.sleep(1)
    else:
        return  # best-effort; next test's own stability check will still fire

    # Issue TAKEOVER. Safe even if the node is already primary (no-op).
    for _ in range(int(timeout)):
        try:
            c = valkey.Valkey(host="localhost", port=killed_port, socket_timeout=3)
            try:
                c.execute_command("CLUSTER", "FAILOVER", "TAKEOVER")
                break
            except valkey.exceptions.ResponseError as e:
                # "You should send CLUSTER FAILOVER to a replica" means we're
                # already the primary — nothing to do.
                if "replica" in str(e).lower():
                    return
                time.sleep(1)
            finally:
                c.close()
        except Exception:
            time.sleep(1)

    # Give the cluster one more beat to converge on the new roles.
    _wait_for_cluster_stable(killed_port, timeout=10.0)


# ── Test A — standard cluster (replica-tier-enabled=false) ───────────────────


@pytest.mark.docker_cluster
@pytest.mark.slow
def test_failover_promotes_replica_and_retains_data(docker_cluster):
    """Hard-kill primary-1, wait for replica promotion, assert zero data loss and
    immediate FLASH.SET on the new primary (validates task #64 lazy NVMe init)."""
    _run_failover_scenario(
        seed_port=7001,
        killed_port=7001,
        survivor_port=7002,
        replica_ports=(7004, 7005, 7006),
        project_name="vf-cluster",
    )


# ── Test B — replica-tier-enabled cluster ────────────────────────────────────


@pytest.mark.docker_cluster
@pytest.mark.slow
def test_failover_with_replica_tier_enabled(docker_cluster_replica_tier):
    """Same failover scenario with flash.replica-tier-enabled=yes on replicas.

    In this variant the replica already has its NVMe backend initialised before
    promotion, so there is no lazy-init delay on the first post-promotion write.
    The same zero-loss and write-correctness assertions apply.
    """
    _run_failover_scenario(
        seed_port=7011,
        killed_port=7011,
        survivor_port=7012,
        replica_ports=(7014, 7015, 7016),
        project_name="vf-cluster-rt",
    )
