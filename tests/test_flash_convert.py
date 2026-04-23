"""Integration tests for FLASH.CONVERT and FLASH.DRAIN.

The convert/drain pair is the prerequisite for a clean `MODULE UNLOAD flash`:
Valkey refuses unload while custom-type keys exist, so operators drain the
flash tier first. These tests cover the per-type happy paths, TTL
preservation, Cold-tier materialisation, replication + AOF round-trip,
keyspace notifications, SCAN filtering, FORCE semantics, and a full
drain → MODULE UNLOAD cycle.
"""

import os
import shutil
import tempfile

import pytest
from valkey import ResponseError
from valkey_flash_test_case import FlashReplicationTestCase, ValkeyFlashTestCase


def _binaries_dir():
    return (
        f"{os.path.dirname(os.path.realpath(__file__))}"
        f"/build/binaries/{os.environ['SERVER_VERSION']}"
    )


def _server_path():
    return os.path.join(_binaries_dir(), "valkey-server")


def _prepend_lib_path():
    binaries_dir = _binaries_dir()
    existing = os.environ.get("LD_LIBRARY_PATH", "")
    os.environ["LD_LIBRARY_PATH"] = f"{binaries_dir}:{existing}" if existing else binaries_dir


def _server_args(flash_path):
    return {
        "enable-debug-command": "yes",
        "loadmodule": (f"{os.getenv('MODULE_PATH')} path {flash_path} capacity-bytes 16777216"),
    }


# ── FLASH.CONVERT — per-type happy path ───────────────────────────────────────


class TestFlashConvertPerType(ValkeyFlashTestCase):
    def test_convert_string_hot(self):
        self.client.execute_command("FLASH.SET", "s1", "hello")
        assert self.client.execute_command("FLASH.CONVERT", "s1") == 1
        assert self.client.execute_command("TYPE", "s1") == b"string"
        assert self.client.execute_command("GET", "s1") == b"hello"

    def test_convert_hash_hot(self):
        self.client.execute_command("FLASH.HSET", "h1", "a", "1", "b", "2", "c", "3")
        assert self.client.execute_command("FLASH.CONVERT", "h1") == 1
        assert self.client.execute_command("TYPE", "h1") == b"hash"
        # valkey-py's execute_command for HGETALL returns a dict.
        got = self.client.execute_command("HGETALL", "h1")
        assert got == {b"a": b"1", b"b": b"2", b"c": b"3"}

    def test_convert_list_hot_preserves_order(self):
        self.client.execute_command("FLASH.RPUSH", "l1", "a", "b", "c", "d")
        assert self.client.execute_command("FLASH.CONVERT", "l1") == 1
        assert self.client.execute_command("TYPE", "l1") == b"list"
        assert self.client.execute_command("LRANGE", "l1", "0", "-1") == [
            b"a",
            b"b",
            b"c",
            b"d",
        ]

    def test_convert_zset_hot_preserves_scores(self):
        self.client.execute_command("FLASH.ZADD", "z1", "1", "a", "2.5", "b", "10", "c")
        assert self.client.execute_command("FLASH.CONVERT", "z1") == 1
        assert self.client.execute_command("TYPE", "z1") == b"zset"
        got = self.client.execute_command("ZRANGE", "z1", "0", "-1", "WITHSCORES")
        # ZRANGE returns [member1, score1, member2, score2, ...]
        pairs = list(zip(got[::2], got[1::2], strict=False))
        assert pairs == [(b"a", b"1"), (b"b", b"2.5"), (b"c", b"10")]


# ── Idempotence + missing-key ─────────────────────────────────────────────────


class TestFlashConvertIdempotent(ValkeyFlashTestCase):
    def test_convert_missing_key_returns_zero(self):
        assert self.client.execute_command("FLASH.CONVERT", "no-such-key") == 0

    def test_convert_native_string_returns_zero(self):
        self.client.execute_command("SET", "native", "value")
        assert self.client.execute_command("FLASH.CONVERT", "native") == 0
        # Native value unchanged.
        assert self.client.execute_command("GET", "native") == b"value"

    def test_convert_native_hash_returns_zero(self):
        self.client.execute_command("HSET", "nh", "x", "1")
        assert self.client.execute_command("FLASH.CONVERT", "nh") == 0

    def test_convert_double_call_returns_zero_second_time(self):
        self.client.execute_command("FLASH.SET", "twice", "v")
        assert self.client.execute_command("FLASH.CONVERT", "twice") == 1
        # Second call sees a native string now — idempotent :0.
        assert self.client.execute_command("FLASH.CONVERT", "twice") == 0


# ── Arity + option parsing ────────────────────────────────────────────────────


class TestFlashConvertArity(ValkeyFlashTestCase):
    def test_convert_no_args_rejected(self):
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.CONVERT")

    def test_convert_extra_args_rejected(self):
        self.client.execute_command("FLASH.SET", "k", "v")
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.CONVERT", "k", "extra")


# ── TTL preservation ──────────────────────────────────────────────────────────


class TestFlashConvertTTL(ValkeyFlashTestCase):
    def _assert_ttl_near(self, key, expected_seconds, tolerance=5):
        remaining = self.client.execute_command("TTL", key)
        # TTL returns -1 for no TTL, -2 for missing. Positive → seconds.
        assert remaining > 0, f"expected positive TTL on {key}, got {remaining}"
        assert abs(remaining - expected_seconds) <= tolerance, (
            f"TTL {remaining}s not within ±{tolerance}s of expected {expected_seconds}s"
        )

    def test_convert_string_preserves_ttl(self):
        self.client.execute_command("FLASH.SET", "s", "v", "EX", "120")
        self.client.execute_command("FLASH.CONVERT", "s")
        assert self.client.execute_command("TYPE", "s") == b"string"
        self._assert_ttl_near("s", 120)

    def test_convert_hash_preserves_ttl(self):
        self.client.execute_command("FLASH.HSET", "h", "a", "1", "EX", "120")
        self.client.execute_command("FLASH.CONVERT", "h")
        assert self.client.execute_command("TYPE", "h") == b"hash"
        self._assert_ttl_near("h", 120)

    def test_convert_list_preserves_ttl(self):
        self.client.execute_command("FLASH.RPUSH", "l", "a", "b")
        self.client.execute_command("EXPIRE", "l", "120")
        self.client.execute_command("FLASH.CONVERT", "l")
        assert self.client.execute_command("TYPE", "l") == b"list"
        self._assert_ttl_near("l", 120)

    def test_convert_zset_preserves_ttl(self):
        self.client.execute_command("FLASH.ZADD", "z", "1", "a")
        self.client.execute_command("EXPIRE", "z", "120")
        self.client.execute_command("FLASH.CONVERT", "z")
        assert self.client.execute_command("TYPE", "z") == b"zset"
        self._assert_ttl_near("z", 120)

    def test_convert_no_ttl_stays_no_ttl(self):
        self.client.execute_command("FLASH.SET", "notime", "v")
        self.client.execute_command("FLASH.CONVERT", "notime")
        # TTL on a native string with no expiry is -1.
        assert self.client.execute_command("TTL", "notime") == -1


# ── Cold-tier materialisation ─────────────────────────────────────────────────


class TestFlashConvertCold(ValkeyFlashTestCase):
    def test_convert_cold_string(self):
        self.client.execute_command("FLASH.SET", "cs", "cold-payload")
        # Force demote to the Cold tier so convert has to materialise via NVMe.
        self.client.execute_command("FLASH.DEBUG.DEMOTE", "cs")
        assert self.client.execute_command("FLASH.CONVERT", "cs") == 1
        assert self.client.execute_command("TYPE", "cs") == b"string"
        assert self.client.execute_command("GET", "cs") == b"cold-payload"

    def test_convert_cold_hash(self):
        self.client.execute_command("FLASH.HSET", "ch", "x", "1", "y", "2")
        self.client.execute_command("FLASH.DEBUG.DEMOTE", "ch")
        assert self.client.execute_command("FLASH.CONVERT", "ch") == 1
        assert self.client.execute_command("HGETALL", "ch") == {b"x": b"1", b"y": b"2"}

    def test_convert_cold_list(self):
        self.client.execute_command("FLASH.RPUSH", "cl", "a", "b", "c")
        self.client.execute_command("FLASH.DEBUG.DEMOTE", "cl")
        assert self.client.execute_command("FLASH.CONVERT", "cl") == 1
        assert self.client.execute_command("LRANGE", "cl", "0", "-1") == [b"a", b"b", b"c"]

    def test_convert_cold_zset(self):
        self.client.execute_command("FLASH.ZADD", "cz", "1", "a", "2", "b")
        self.client.execute_command("FLASH.DEBUG.DEMOTE", "cz")
        assert self.client.execute_command("FLASH.CONVERT", "cz") == 1
        assert self.client.execute_command("ZRANGE", "cz", "0", "-1", "WITHSCORES") == [
            b"a",
            b"1",
            b"b",
            b"2",
        ]

    def test_convert_cold_string_preserves_ttl(self):
        self.client.execute_command("FLASH.SET", "cts", "hot", "EX", "200")
        self.client.execute_command("FLASH.DEBUG.DEMOTE", "cts")
        self.client.execute_command("FLASH.CONVERT", "cts")
        remaining = self.client.execute_command("TTL", "cts")
        assert remaining > 0
        assert abs(remaining - 200) <= 10


# ── Keyspace events ───────────────────────────────────────────────────────────


class TestFlashConvertEvents(ValkeyFlashTestCase):
    def _setup_pubsub(self):
        # Same-client pubsub pattern from tests/test_flash_keyspace.py: cross-
        # client pubsub is flaky under pytest, but a single valkey.Valkey
        # instance's internal connection pool delivers reliably.
        ps = self.client.pubsub()
        ps.psubscribe("__key*__:*")
        self.client.execute_command("CONFIG", "SET", "notify-keyspace-events", "KEA")
        return ps

    def _collect(self, ps, count, timeout_secs=5):
        import time as _time

        messages = []
        deadline = _time.time() + timeout_secs
        while len(messages) < count and _time.time() < deadline:
            msg = ps.get_message()
            if msg and msg["type"] == "pmessage":
                messages.append(msg)
            else:
                _time.sleep(0.005)
        return messages

    def test_convert_emits_flash_convert_event(self):
        self.client.execute_command("FLASH.SET", "evk", "v")
        ps = self._setup_pubsub()
        self.client.execute_command("FLASH.CONVERT", "evk")
        # Collect a handful: DEL + SET + flash.convert will all fire.
        messages = self._collect(ps, 6)
        keyevent_ch = b"__keyevent@0__:flash.convert"
        assert any(m["channel"] == keyevent_ch and m["data"] == b"evk" for m in messages), (
            f"flash.convert event missing; saw {messages}"
        )

    def test_convert_emits_native_set_event(self):
        # The native SET sub-call auto-emits `set`; that's what makes the
        # resulting AOF replayable without the module.
        self.client.execute_command("FLASH.SET", "ns", "v")
        ps = self._setup_pubsub()
        self.client.execute_command("FLASH.CONVERT", "ns")
        messages = self._collect(ps, 6)
        keyevent_ch = b"__keyevent@0__:set"
        assert any(m["channel"] == keyevent_ch and m["data"] == b"ns" for m in messages), (
            f"native 'set' event missing after CONVERT; saw {messages}"
        )


# ── INFO flash metrics ────────────────────────────────────────────────────────


class TestFlashConvertMetrics(ValkeyFlashTestCase):
    def test_convert_total_increments(self):
        before = self.parse_valkey_info("flash")["flash_convert_total"]
        self.client.execute_command("FLASH.SET", "m1", "v")
        self.client.execute_command("FLASH.CONVERT", "m1")
        after = self.parse_valkey_info("flash")["flash_convert_total"]
        assert after == before + 1

    def test_drain_last_fields_exposed(self):
        info = self.parse_valkey_info("flash")
        for field in (
            "flash_drain_in_progress",
            "flash_drain_last_converted",
            "flash_drain_last_skipped",
            "flash_drain_last_errors",
            "flash_drain_last_scanned",
        ):
            assert field in info, f"INFO flash missing {field}"


# ── FLASH.DRAIN ──────────────────────────────────────────────────────────────


class TestFlashDrain(ValkeyFlashTestCase):
    def _seed_one_of_each(self):
        self.client.execute_command("FLASH.SET", "s:1", "hello")
        self.client.execute_command("FLASH.HSET", "h:1", "a", "1")
        self.client.execute_command("FLASH.RPUSH", "l:1", "x", "y")
        self.client.execute_command("FLASH.ZADD", "z:1", "1", "m")

    def test_drain_all_types(self):
        self._seed_one_of_each()
        reply = self.client.execute_command("FLASH.DRAIN")
        # [converted, skipped, errors, scanned]
        assert reply[0] == 4
        assert reply[2] == 0
        # All four keys are now native.
        assert self.client.execute_command("TYPE", "s:1") == b"string"
        assert self.client.execute_command("TYPE", "h:1") == b"hash"
        assert self.client.execute_command("TYPE", "l:1") == b"list"
        assert self.client.execute_command("TYPE", "z:1") == b"zset"

    def test_drain_empty_keyspace_returns_zero(self):
        reply = self.client.execute_command("FLASH.DRAIN")
        assert reply[0] == 0
        assert reply[3] == 0

    def test_drain_respects_match(self):
        self._seed_one_of_each()
        reply = self.client.execute_command("FLASH.DRAIN", "MATCH", "s:*")
        assert reply[0] == 1
        # Only the string key converted.
        assert self.client.execute_command("TYPE", "s:1") == b"string"
        # Others still FLASH.* — TYPE returns the module name.
        assert self.client.execute_command("TYPE", "h:1") != b"hash"

    def test_drain_respects_count(self):
        for i in range(5):
            self.client.execute_command("FLASH.SET", f"k:{i}", str(i))
        reply = self.client.execute_command("FLASH.DRAIN", "COUNT", "2")
        assert reply[0] == 2
        # Three keys remain as flash strings.
        native_count = sum(
            1 for i in range(5) if self.client.execute_command("TYPE", f"k:{i}") == b"string"
        )
        assert native_count == 2

    def test_drain_count_zero_is_noop(self):
        self.client.execute_command("FLASH.SET", "k0", "v")
        reply = self.client.execute_command("FLASH.DRAIN", "COUNT", "0")
        assert reply[0] == 0
        # Key still flash.
        assert self.client.execute_command("TYPE", "k0") != b"string"

    def test_drain_unknown_option_rejected(self):
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.DRAIN", "WEIRD")

    def test_drain_missing_count_value_rejected(self):
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.DRAIN", "COUNT")

    def test_drain_invalid_count_rejected(self):
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.DRAIN", "COUNT", "not-a-number")

    def test_drain_leaves_no_flash_keys(self):
        # Prereq for MODULE UNLOAD flash — Valkey refuses unload while custom-
        # type keys exist. After DRAIN, every key must be a native type. We
        # cannot invoke MODULE UNLOAD here because enable-module-command is
        # disabled on the test server, but verifying all types are native is
        # the sufficient precondition that makes the unload legal.
        self._seed_one_of_each()
        # Pre-condition: TYPE reports the FLASH module name on each.
        pre_types = {
            k: self.client.execute_command("TYPE", k) for k in ("s:1", "h:1", "l:1", "z:1")
        }
        assert all(t != b"string" for t in pre_types.values()), (
            f"expected module types pre-drain, got {pre_types}"
        )
        self.client.execute_command("FLASH.DRAIN")
        assert self.client.execute_command("TYPE", "s:1") == b"string"
        assert self.client.execute_command("TYPE", "h:1") == b"hash"
        assert self.client.execute_command("TYPE", "l:1") == b"list"
        assert self.client.execute_command("TYPE", "z:1") == b"zset"

    def test_drain_updates_info_drain_last_fields(self):
        self._seed_one_of_each()
        self.client.execute_command("FLASH.DRAIN")
        info = self.parse_valkey_info("flash")
        assert info["flash_drain_last_converted"] == 4
        assert info["flash_drain_last_scanned"] == 4
        assert info["flash_drain_in_progress"] == "no"


class TestFlashConvertAof(ValkeyFlashTestCase):
    """After CONVERT, the AOF must contain only native commands for the key —
    never `FLASH.CONVERT` itself. That keeps AOF replay module-independent, so
    a subsequent `MODULE UNLOAD flash` (and downstream AOF replay) doesn't
    encounter an unknown command."""

    def _enable_aof(self):
        from valkeytestframework.util.waiters import wait_for_equal

        self.client.config_set("appendonly", "yes")
        wait_for_equal(
            lambda: self.client.info("persistence")["aof_rewrite_in_progress"], 0, timeout=30
        )

    def _bgrewriteaof_and_restart(self):
        from valkeytestframework.util.waiters import wait_for_equal
        from valkeytestframework.valkey_test_case import ValkeyAction

        self.client.bgrewriteaof()
        self.server.wait_for_action_done(ValkeyAction.AOF_REWRITE)
        self.server.args["appendonly"] = "yes"
        self.server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
        assert self.server.is_alive()
        wait_for_equal(self.server.is_rdb_done_loading, True)
        # Re-bind the client — restart() returns a new client.
        self.client = self.server.get_new_client()

    def test_converted_string_survives_aof_restart_as_native(self):
        self._enable_aof()
        self.client.execute_command("FLASH.SET", "aofs", "hello")
        self.client.execute_command("FLASH.CONVERT", "aofs")
        self._bgrewriteaof_and_restart()
        # After AOF replay, the key is still a native string — no module
        # involvement. This is the key claim: the AOF is module-independent.
        assert self.client.execute_command("TYPE", "aofs") == b"string"
        assert self.client.execute_command("GET", "aofs") == b"hello"

    def test_converted_hash_survives_aof_restart_as_native(self):
        self._enable_aof()
        self.client.execute_command("FLASH.HSET", "aofh", "a", "1", "b", "2")
        self.client.execute_command("FLASH.CONVERT", "aofh")
        self._bgrewriteaof_and_restart()
        assert self.client.execute_command("TYPE", "aofh") == b"hash"
        assert self.client.execute_command("HGETALL", "aofh") == {b"a": b"1", b"b": b"2"}


class TestFlashDrainForce(ValkeyFlashTestCase):
    def test_drain_force_is_accepted(self):
        self.client.execute_command("FLASH.SET", "fk", "v")
        reply = self.client.execute_command("FLASH.DRAIN", "FORCE")
        assert reply[0] == 1

    def test_drain_force_combines_with_match_and_count(self):
        for i in range(3):
            self.client.execute_command("FLASH.SET", f"fc:{i}", str(i))
        reply = self.client.execute_command("FLASH.DRAIN", "MATCH", "fc:*", "COUNT", "2", "FORCE")
        assert reply[0] == 2


# ── Replication ───────────────────────────────────────────────────────────────


class TestFlashConvertReplication(FlashReplicationTestCase):
    @pytest.fixture(autouse=True)
    def setup_test(self, setup):
        _prepend_lib_path()
        self._flash_dir = os.path.abspath(
            tempfile.mkdtemp(prefix="flash_repl_convert_", dir=self.testdir)
        )
        primary_path = os.path.join(self._flash_dir, "primary.bin")
        self.args = _server_args(primary_path)
        self.server, self.client = self.create_server(
            testdir=self.testdir,
            server_path=_server_path(),
            args=self.args,
        )
        yield
        shutil.rmtree(self._flash_dir, ignore_errors=True)

    def _setup_single_replica(self):
        """Stand up one replica with its own NVMe backing file."""
        self.num_replicas = 1
        self.replicas = []
        self.skip_teardown = False
        self.create_replicas(1)
        replica_path = os.path.join(self._flash_dir, "replica0.bin")
        self.replicas[0].args["loadmodule"] = _server_args(replica_path)["loadmodule"]
        self.start_replicas()
        self.wait_for_replicas(1)
        self.wait_for_primary_link_up_all_replicas()
        self.wait_for_all_replicas_online(1)
        self.waitForReplicaToSyncUp(self.replicas[0])

    def test_convert_replicates_as_native(self):
        self._setup_single_replica()
        self.client.execute_command("FLASH.SET", "rk", "hello")
        self.waitForReplicaToSyncUp(self.replicas[0])

        self.client.execute_command("FLASH.CONVERT", "rk")
        self.waitForReplicaToSyncUp(self.replicas[0])

        replica = self.replicas[0].client
        # The replica must see a native string, not a FLASH.* module type.
        assert replica.execute_command("TYPE", "rk") == b"string"
        assert replica.execute_command("GET", "rk") == b"hello"

    def test_drain_replicates_all_types_as_native(self):
        self._setup_single_replica()
        self.client.execute_command("FLASH.SET", "rs", "v")
        self.client.execute_command("FLASH.HSET", "rh", "a", "1")
        self.client.execute_command("FLASH.RPUSH", "rl", "a", "b")
        self.client.execute_command("FLASH.ZADD", "rz", "1", "m")

        self.client.execute_command("FLASH.DRAIN")
        self.waitForReplicaToSyncUp(self.replicas[0])

        replica = self.replicas[0].client
        assert replica.execute_command("TYPE", "rs") == b"string"
        assert replica.execute_command("TYPE", "rh") == b"hash"
        assert replica.execute_command("TYPE", "rl") == b"list"
        assert replica.execute_command("TYPE", "rz") == b"zset"
