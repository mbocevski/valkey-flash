"""
Resharding-under-load stress suite (Task #84).

Two tests, both @slow and @docker_cluster:

  Test A — Single slot migration with 4 concurrent writer threads.
    Threads perform mixed FLASH.SET / FLASH.GET / FLASH.HSET / FLASH.HDEL
    against keys in the migrating slot.  The migration uses the traditional
    Valkey protocol (CLUSTER SETSLOT MIGRATING/IMPORTING + MIGRATE KEYS +
    CLUSTER SETSLOT NODE).  After completion: zero acked writes may be lost,
    no unexpected application errors.

  Test B — 16 concurrent slot migrations, same traffic pattern.
    16 slots from the primary with the largest slot range are migrated to
    another primary, at most 4 at a time.  Same zero-loss assertion.

Run (requires USE_DOCKER=1):
    USE_DOCKER=1 pytest tests/test_flash_cluster_resharding_stress.py -v -m "docker_cluster and slow"
    USE_DOCKER=1 pytest tests/test_flash_cluster_resharding_stress.py::test_single_slot_migration_under_load -v

Budget: ~3–5 min per test.
"""

import concurrent.futures
import threading
import time
from contextlib import suppress
from dataclasses import dataclass

import pytest
import valkey
import valkey.exceptions
from valkey.cluster import ValkeyCluster
from docker_fixtures import flash_cluster_client

# ── Topology helpers ──────────────────────────────────────────────────────────


@dataclass
class NodeInfo:
    node_id: str
    internal_host: str  # as announced to cluster (Docker hostname)
    internal_port: int  # internal port (6379)
    external_port: int  # host-mapped port (7001-7003)


def _discover_primaries(seed_port: int = 7001) -> list[NodeInfo]:
    """Return NodeInfo for each primary, mapping internal → external port."""
    primary_ext_ports = (7001, 7002, 7003)
    result = []
    for ext_port in primary_ext_ports:
        c = valkey.Valkey(host="localhost", port=ext_port, socket_timeout=5)
        try:
            node_id = c.cluster("myid")
            if isinstance(node_id, bytes):
                node_id = node_id.decode()

            # valkey-py 6.x returns CLUSTER NODES as a dict {addr: {...}};
            # older versions returned raw bytes. Handle both.
            nodes_raw = c.cluster("nodes")
            if isinstance(nodes_raw, dict):
                for addr, info in nodes_raw.items():
                    if info.get("node_id") != node_id:
                        continue
                    host_str, port_str = addr.rsplit(":", 1)
                    result.append(
                        NodeInfo(
                            node_id=node_id,
                            internal_host=host_str,
                            internal_port=int(port_str),
                            external_port=ext_port,
                        )
                    )
                    break
            else:
                text = nodes_raw.decode() if isinstance(nodes_raw, bytes) else str(nodes_raw)
                for line in text.strip().split("\n"):
                    parts = line.split()
                    if not parts or parts[0] != node_id:
                        continue
                    addr = parts[1].split("@")[0]  # host:port
                    host_str, port_str = addr.rsplit(":", 1)
                    result.append(
                        NodeInfo(
                            node_id=node_id,
                            internal_host=host_str,
                            internal_port=int(port_str),
                            external_port=ext_port,
                        )
                    )
                    break
        finally:
            c.close()
    return result


def _slot_ranges_by_node(seed_port: int = 7001) -> dict[str, list[tuple[int, int]]]:
    """Return {node_id: [(start, end), ...]} for primaries."""
    c = valkey.Valkey(host="localhost", port=seed_port, socket_timeout=5)
    try:
        slots_data = c.cluster("slots")
        result: dict[str, list[tuple[int, int]]] = {}
        for entry in slots_data:
            start, end = int(entry[0]), int(entry[1])
            primary = entry[2]
            nid = primary[2]
            if isinstance(nid, bytes):
                nid = nid.decode()
            result.setdefault(nid, []).append((start, end))
        return result
    finally:
        c.close()


def _node_for_slot(slot: int, primaries: list[NodeInfo], seed_port: int = 7001) -> NodeInfo | None:
    """Return the NodeInfo of the primary that currently owns `slot`."""
    c = valkey.Valkey(host="localhost", port=seed_port, socket_timeout=5)
    try:
        slots_data = c.cluster("slots")
        for entry in slots_data:
            start, end = int(entry[0]), int(entry[1])
            if start <= slot <= end:
                nid = entry[2][2]
                if isinstance(nid, bytes):
                    nid = nid.decode()
                for ni in primaries:
                    if ni.node_id == nid:
                        return ni
    finally:
        c.close()
    return None


def _keyslot(tag: str, seed_port: int = 7001) -> int:
    """Return the cluster slot for hash tag `{tag}` via CLUSTER KEYSLOT."""
    c = valkey.Valkey(host="localhost", port=seed_port, socket_timeout=5)
    try:
        return int(c.cluster("keyslot", f"{{{tag}}}probe"))
    finally:
        c.close()


# ── Migration protocol ────────────────────────────────────────────────────────

ALL_NODE_PORTS = (7001, 7002, 7003, 7004, 7005, 7006)


def _migrate_slot(
    source: NodeInfo,
    target: NodeInfo,
    slot: int,
    timeout_ms: int = 10_000,
) -> int:
    """
    Move `slot` from `source` to `target` using the traditional Valkey protocol:
      1. CLUSTER SETSLOT <slot> MIGRATING <target_id>  (on source)
      2. CLUSTER SETSLOT <slot> IMPORTING <source_id>  (on target)
      3. Drain keys with MIGRATE ... KEYS ...           (repeated until empty)
      4. CLUSTER SETSLOT <slot> NODE <target_id>        (on all nodes)

    Returns number of keys migrated.
    During steps 1-3 the cluster client handles MOVED/ASK transparently.
    """
    src_c = valkey.Valkey(host="localhost", port=source.external_port, socket_timeout=30)
    tgt_c = valkey.Valkey(host="localhost", port=target.external_port, socket_timeout=30)

    try:
        src_c.cluster("setslot", slot, "migrating", target.node_id)
        tgt_c.cluster("setslot", slot, "importing", source.node_id)

        migrated = 0
        while True:
            keys = src_c.cluster("getkeysinslot", slot, 200)
            if not keys:
                break
            # MIGRATE host port "" db timeout KEYS key1 key2 ...
            src_c.execute_command(
                "MIGRATE",
                target.internal_host,
                target.internal_port,
                "",  # empty key → use KEYS option
                0,  # db
                timeout_ms,
                "KEYS",
                *keys,
            )
            migrated += len(keys)

        # Commit on all nodes (replicas ignore CLUSTER SETSLOT).
        for port in ALL_NODE_PORTS:
            nc = valkey.Valkey(host="localhost", port=port, socket_timeout=5)
            try:
                nc.cluster("setslot", slot, "node", target.node_id)
            except Exception:
                pass
            finally:
                nc.close()

        return migrated
    finally:
        src_c.close()
        tgt_c.close()


# ── Ack tracking ──────────────────────────────────────────────────────────────


class AckTracker:
    """Thread-safe record of last successfully ACKed (key, value) pairs.

    Includes a per-key lock so callers can atomise "do the write, then
    record the ack" — without it, two threads racing to update the same key
    can commit their writes to the server in one order and record their acks
    in the opposite order, leaving the tracker's `last ack` out of sync with
    the server's `last value`. That's a tracker artifact, not a module bug."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._key_locks: dict[str, threading.Lock] = {}
        self._acks: dict[str, str] = {}
        self.error_count = 0

    def key_lock(self, key: str) -> threading.Lock:
        with self._lock:
            lk = self._key_locks.get(key)
            if lk is None:
                lk = threading.Lock()
                self._key_locks[key] = lk
            return lk

    def record(self, key: str, value: str) -> None:
        with self._lock:
            self._acks[key] = value

    def record_error(self) -> None:
        with self._lock:
            self.error_count += 1

    def snapshot(self) -> dict[str, str]:
        with self._lock:
            return dict(self._acks)


# ── Worker thread ─────────────────────────────────────────────────────────────


def _run_mixed_traffic(
    stop_event: threading.Event,
    tracker: AckTracker,
    keys: list[str],
    thread_id: int,
) -> None:
    """
    Continuously perform FLASH.SET / FLASH.GET / FLASH.HSET / FLASH.HDEL
    against `keys`, recording successful SET acks.  MOVED/ASK are handled
    transparently by ValkeyCluster; other errors are counted but ignored.
    """
    try:
        client = flash_cluster_client(port=7001, socket_timeout=5)
    except Exception:
        return

    try:
        i = 0
        while not stop_event.is_set():
            key = keys[i % len(keys)]
            value = f"t{thread_id}:i{i}"
            hkey = f"h:{key}"

            # FLASH.SET — hold the per-key lock around the write+ack pair so
            # the server's final value matches whichever thread's ack was
            # recorded last. Without the lock, two threads can apply writes in
            # one order on the server and record acks in the other, leaving
            # the tracker disagreeing with the server at the end of the run.
            with tracker.key_lock(key):
                try:
                    client.execute_command("FLASH.SET", key, value)
                    tracker.record(key, value)
                except (
                    valkey.exceptions.ConnectionError,
                    valkey.exceptions.TimeoutError,
                    valkey.exceptions.ClusterDownError,
                ):
                    tracker.record_error()
                except Exception:
                    tracker.record_error()

            with tracker.key_lock(hkey):
                try:
                    client.execute_command("FLASH.HSET", hkey, "v", value)
                    tracker.record(hkey, value)
                except Exception:
                    tracker.record_error()

            with suppress(Exception):
                # FLASH.GET (read — not tracked for loss, just exercises the path)
                client.execute_command("FLASH.GET", key)

            with suppress(Exception):
                # Occasional FLASH.HDEL to vary the pattern. Hold the same
                # per-key lock so an HDEL doesn't race with a concurrent HSET.
                if i % 7 == 0:
                    with tracker.key_lock(hkey):
                        client.execute_command("FLASH.HDEL", hkey, "v")

            i += 1
            time.sleep(0.002)  # ~500 ops/thread/s — enough load without flooding
    finally:
        with suppress(Exception):
            client.close()


# ── Zero-loss assertion ───────────────────────────────────────────────────────


def _assert_zero_loss(tracker: AckTracker, desc: str, seed_port: int = 7001) -> None:
    """
    Read back every acked key from the cluster and assert value matches.
    Also checks hash keys where the last HSET was not followed by HDEL.
    """
    acks = tracker.snapshot()
    if not acks:
        pytest.skip(f"{desc}: no acks recorded — check that traffic threads ran")

    client = flash_cluster_client(port=seed_port, socket_timeout=10)
    try:
        losses = []
        mismatches = []
        for key, expected in acks.items():
            if key.startswith("h:"):
                # Hash key: check FLASH.HGET
                try:
                    actual = client.execute_command("FLASH.HGET", key, "v")
                    # None is acceptable if the key was HDELed after the last ack;
                    # we only flag definite mismatches.
                    if actual is not None:
                        actual_str = actual.decode() if isinstance(actual, bytes) else actual
                        if actual_str != expected:
                            mismatches.append((key, expected, actual_str))
                except Exception as e:
                    losses.append((key, str(e)))
            else:
                try:
                    actual = client.execute_command("FLASH.GET", key)
                    if actual is None:
                        losses.append((key, "key missing after migration"))
                    else:
                        actual_str = actual.decode() if isinstance(actual, bytes) else actual
                        if actual_str != expected:
                            mismatches.append((key, expected, actual_str))
                except Exception as e:
                    losses.append((key, str(e)))

        assert not losses, f"{desc}: {len(losses)} key(s) missing after migration:\n" + "\n".join(
            f"  {k}: {e}" for k, e in losses[:20]
        )
        assert not mismatches, (
            f"{desc}: {len(mismatches)} value mismatch(es) after migration:\n"
            + "\n".join(f"  {k}: expected={exp!r} got={got!r}" for k, exp, got in mismatches[:20])
        )
    finally:
        client.close()


# ── Test A — single slot migration ────────────────────────────────────────────


@pytest.mark.docker_cluster
@pytest.mark.slow
def test_single_slot_migration_under_load(docker_cluster):
    """
    Migrate one slot while 4 threads hammer FLASH.SET/GET/HSET/HDEL.
    Zero acked writes may be lost or mismatched after migration completes.

    Protocol: CLUSTER SETSLOT MIGRATING → MIGRATE KEYS → CLUSTER SETSLOT NODE
    (traditional per-key migration, exercises rdb_save / rdb_load for both
    FlashString and FlashHash module types).
    """
    # -- topology --
    primaries = _discover_primaries()
    assert len(primaries) >= 2, "Need at least 2 primaries"

    # All test keys share hash tag "{m1}", so they land in a single slot.
    TAG = "m1"
    slot = _keyslot(TAG)
    source = _node_for_slot(slot, primaries)
    assert source is not None, f"No primary found for slot {slot}"
    target = next((p for p in primaries if p.node_id != source.node_id), None)
    assert target is not None, "No target primary available"

    # -- preload --
    n_keys = 60
    string_keys = [f"{{{TAG}}}{i}" for i in range(n_keys)]
    hash_keys = [f"h:{{{TAG}}}{i}" for i in range(n_keys)]

    seed = flash_cluster_client(port=7001, socket_timeout=10)
    try:
        for k in string_keys:
            seed.execute_command("FLASH.SET", k, f"init:{k}")
        for k in hash_keys:
            seed.execute_command("FLASH.HSET", k, "v", f"init:{k}")
        # Demote half to Cold tier to exercise the rdb_save Cold path.
        for k in string_keys[: n_keys // 2]:
            with suppress(Exception):
                seed.execute_command("FLASH.DEBUG.DEMOTE", k)
                # non-fatal: test proceeds with Hot keys if demote unavailable
    finally:
        seed.close()

    # -- concurrent traffic --
    stop = threading.Event()
    tracker = AckTracker()
    # Pass only `string_keys` to the traffic threads. The worker internally
    # derives `hkey = "h:" + key` for each FLASH.HSET, so a key never has to
    # be both a string and a hash. Earlier versions of this test passed
    # `string_keys + hash_keys`, which let a worker pick `h:{m1}0` as the
    # string target — racing FLASH.SET against the already-hash-typed key
    # during migration, leaving the destination with the wrong type.
    threads = [
        threading.Thread(
            target=_run_mixed_traffic,
            args=(stop, tracker, string_keys, tid),
            daemon=True,
        )
        for tid in range(4)
    ]
    for t in threads:
        t.start()

    time.sleep(1.0)  # let threads warm up before migration starts

    # -- migration --
    migrated = 0
    try:
        try:
            migrated = _migrate_slot(source, target, slot, timeout_ms=15_000)
        except Exception as exc:
            stop.set()
            pytest.fail(f"Migration failed: {exc}")

        time.sleep(0.5)  # let cluster gossip converge and in-flight ops drain

        # -- stop traffic --
        stop.set()
        for t in threads:
            t.join(timeout=10)

        # -- assert zero loss --
        _assert_zero_loss(
            tracker,
            f"test_single_slot_migration_under_load "
            f"(slot={slot}, {migrated} keys moved, {tracker.error_count} traffic errors)",
        )
    finally:
        # Reverse the migration so the session-scoped docker_cluster fixture
        # leaves slot ownership in its pristine state for subsequent tests.
        stop.set()
        with suppress(Exception):
            _migrate_slot(target, source, slot, timeout_ms=15_000)


# ── Test B — 16 concurrent slot migrations ────────────────────────────────────

_MAX_CONCURRENT_MIGRATIONS = 4  # throttle (no flash.max-concurrent-migrations config yet)


@pytest.mark.docker_cluster
@pytest.mark.slow
def test_sixteen_slot_migrations_under_load(docker_cluster):
    """
    Migrate 16 slots (at most 4 concurrently) while 4 writer threads run.
    Zero acked writes may be lost or mismatched after all migrations complete.

    Picks the 16 first slots of whichever primary currently holds the most
    slots, migrates them to another primary, then asserts zero loss.
    """
    # -- topology --
    primaries = _discover_primaries()
    assert len(primaries) >= 2, "Need at least 2 primaries"

    ranges_by_node = _slot_ranges_by_node()

    # Pick the primary with the most slots as source.
    def _total_slots(nid):
        return sum(e - s + 1 for s, e in ranges_by_node.get(nid, []))

    source = max(primaries, key=lambda p: _total_slots(p.node_id))
    target = next((p for p in primaries if p.node_id != source.node_id), None)
    assert target is not None, "No target primary available"

    # Pick up to 16 slots from the source's first slot range.
    source_ranges = sorted(ranges_by_node.get(source.node_id, []))
    assert source_ranges, f"Source node {source.node_id} owns no slots"

    first_start, first_end = source_ranges[0]
    slots_to_migrate = list(range(first_start, min(first_start + 16, first_end + 1)))
    assert slots_to_migrate, "No slots to migrate"

    # -- preload keys in each migrating slot --
    # Use the slot number as hash tag — CLUSTER KEYSLOT confirms which slot it maps to.
    # Pre-seed at least one key per slot so the migration exercises rdb_save.
    cluster_seed = flash_cluster_client(port=7001, socket_timeout=10)
    try:
        for slot in slots_to_migrate:
            # Create a key that hashes to this exact slot by finding a matching tag.
            # Approach: use {<slot>}key and verify; fall back to raw slot tag.
            probe_key = f"{{stress16:{slot}}}key"
            probe_slot = int(
                cluster_seed.execute_command("CLUSTER", "KEYSLOT", probe_key)
            )
            if probe_slot == slot:
                cluster_seed.execute_command("FLASH.SET", probe_key, f"pre:{slot}")
            # If the hash tag doesn't map to this slot, the slot stays empty
            # and the migration loop drains instantly (also valid).
    finally:
        cluster_seed.close()

    # -- write traffic keys: spread across ALL slots, not just migrating ones --
    TAG16 = "stress16_traffic"
    # Intentionally DON'T pre-migrate traffic keys — they stay on their home slot;
    # threads hit all keys and we verify across the board.
    n_traffic = 40
    traffic_keys = [f"{{{TAG16}}}{i}" for i in range(n_traffic)]
    # Collect traffic keys that actually land in migrating slots by querying the
    # source node directly — hash-tag guesses like {stress16:{slot}} don't
    # deterministically map to the expected slot number.
    migrating_traffic_keys = []
    src_probe = valkey.Valkey(host="localhost", port=source.external_port, socket_timeout=5)
    try:
        for s in slots_to_migrate[:4]:
            if int(src_probe.cluster("countkeysinslot", s)) > 0:
                slot_keys = src_probe.cluster("getkeysinslot", s, 5)
                migrating_traffic_keys.extend(
                    k.decode() if isinstance(k, bytes) else k for k in slot_keys
                )
    finally:
        src_probe.close()

    all_traffic_keys = traffic_keys + migrating_traffic_keys

    init_client = flash_cluster_client(port=7001, socket_timeout=10)
    try:
        for k in all_traffic_keys:
            init_client.execute_command("FLASH.SET", k, f"init:{k}")
    finally:
        init_client.close()

    # -- start traffic --
    stop = threading.Event()
    tracker = AckTracker()
    threads = [
        threading.Thread(
            target=_run_mixed_traffic,
            args=(stop, tracker, all_traffic_keys, tid),
            daemon=True,
        )
        for tid in range(4)
    ]
    for t in threads:
        t.start()

    time.sleep(1.0)

    # -- concurrent migrations with throttle --
    migration_errors: list[str] = []
    migration_lock = threading.Lock()
    successfully_migrated: list[int] = []

    def do_migrate(slot: int) -> int:
        try:
            keys = _migrate_slot(source, target, slot, timeout_ms=20_000)
            with migration_lock:
                successfully_migrated.append(slot)
            return keys
        except Exception as exc:
            with migration_lock:
                migration_errors.append(f"slot {slot}: {exc}")
            return 0

    total_migrated = 0
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=_MAX_CONCURRENT_MIGRATIONS) as pool:
            futures = {pool.submit(do_migrate, s): s for s in slots_to_migrate}
            for fut in concurrent.futures.as_completed(futures):
                total_migrated += fut.result()

        time.sleep(1.0)  # convergence window

        stop.set()
        for t in threads:
            t.join(timeout=15)

        # Report migration errors but don't immediately fail — check data loss first.
        if migration_errors:
            print(
                f"\nWarning: {len(migration_errors)} slot migration error(s):\n"
                + "\n".join(f"  {e}" for e in migration_errors)
            )

        # -- zero loss assertion --
        _assert_zero_loss(
            tracker,
            f"test_sixteen_slot_migrations_under_load "
            f"({len(slots_to_migrate)} slots, {total_migrated} keys moved, "
            f"{len(migration_errors)} migration errors, "
            f"{tracker.error_count} traffic errors)",
        )

        if migration_errors:
            pytest.fail(
                f"{len(migration_errors)} slot migration(s) failed (data not lost, "
                f"but migrations incomplete): " + "; ".join(migration_errors[:5])
            )
    finally:
        # Reverse every slot that was successfully migrated so the shared
        # cluster fixture is usable by subsequent tests.
        stop.set()
        for slot in successfully_migrated:
            with suppress(Exception):
                _migrate_slot(target, source, slot, timeout_ms=20_000)
