import contextlib
import os
import shutil
import tempfile
import threading
import time

import pytest
from valkey import ResponseError
from valkeytestframework.valkey_test_case import ReplicationTestCase, ValkeyTestCase

# Tests only write tiny payloads, but the module's default capacity is 1 GiB
# per flash.bin. Without an override every test would leave behind a 1 GiB
# file — hundreds of tests fill the host disk. Cap at 16 MiB by default.
FLASH_TEST_CAPACITY_BYTES = 16 * 1024 * 1024


class ValkeyFlashTestCase(ValkeyTestCase):
    @pytest.fixture(autouse=True)
    def _flash_server_cleanup(self, setup):
        """Always-runs per-test teardown for any ValkeyServerHandle the test
        registered via `self.create_server(...)` or appended to
        `self.server_list` / `self.replicas`.

        The upstream framework exposes a `teardown()` method but never wires
        it to a pytest hook, so it's dead code. Subclasses often override
        `setup_test` to parameterise server args, and the overrides don't
        always yield or replicate the primary's shutdown — leaking server
        processes out of the test. Those processes inherit pytest's stdout
        fd; under AddressSanitizer the LSAN atexit scan delays the leaked
        process's death long enough to keep `tee` on the right of
        `pytest ... 2>&1 | tee log` alive past the CI step budget.

        This fixture declares a teardown that runs regardless of what the
        subclass's `setup_test` does, graceful-shutdowns every tracked
        server, and SIGKILL-falls-back on anything still alive so the
        stdout fd is released before the step exits.
        """
        yield
        # Teardown order matters: replicas first (they hold a link to the
        # primary, whose graceful SHUTDOWN path waits for the link to drop),
        # then the primary and any other tracked servers.
        for replica in getattr(self, "replicas", []):
            _shutdown_server_handle(replica)
        if hasattr(self, "replicas"):
            self.replicas = []
        for server in getattr(self, "server_list", []):
            _shutdown_server_handle(server)
        if hasattr(self, "server_list"):
            self.server_list = []
        # `self.server` may be set to something not in server_list (e.g. a
        # handle constructed by a subclass's custom helper); catch that too.
        standalone = getattr(self, "server", None)
        if standalone is not None:
            _shutdown_server_handle(standalone)
            self.server = None

    @pytest.fixture(autouse=True)
    def setup_test(self, setup):
        binaries_dir = (
            f"{os.path.dirname(os.path.realpath(__file__))}"
            f"/build/binaries/{os.environ['SERVER_VERSION']}"
        )
        server_path = os.path.join(binaries_dir, "valkey-server")
        # Ensure libvalkeylua.so (present in newer Valkey unstable builds) is
        # findable by the server process when it lives next to the binary.
        existing = os.environ.get("LD_LIBRARY_PATH", "")
        os.environ["LD_LIBRARY_PATH"] = f"{binaries_dir}:{existing}" if existing else binaries_dir
        # Each test gets its own NVMe backing file in a fresh per-test subdir
        # under self.testdir (which the framework shares across the class);
        # state from one test never leaks into the next. The loadmodule arg is
        # a single space-separated string: the .so path followed by any module
        # config name/value pairs. Note: module-init args use the raw config
        # name (`path`), without the `flash.` prefix — the `flash.` prefix
        # only applies when addressing the knob via `CONFIG GET`/`SET`.
        self.flash_dir = os.path.abspath(tempfile.mkdtemp(prefix="flash_", dir=self.testdir))
        flash_path = os.path.join(self.flash_dir, "flash.bin")
        args = {
            "enable-debug-command": "yes",
            "loadmodule": (
                f"{os.getenv('MODULE_PATH')} "
                f"path {flash_path} "
                f"capacity-bytes {FLASH_TEST_CAPACITY_BYTES}"
            ),
        }
        self.server, self.client = self.create_server(
            testdir=self.testdir, server_path=server_path, args=args
        )
        yield
        # Drop the per-test flash directory so test-data/ doesn't balloon by
        # `capacity-bytes` for every test the session runs. Server shutdown
        # happens in `_flash_server_cleanup` so subclasses that override
        # `setup_test` (without re-implementing this yield side) still get
        # clean teardown.
        shutil.rmtree(self.flash_dir, ignore_errors=True)

    def verify_error_response(self, client, cmd, expected_err_reply):
        try:
            client.execute_command(cmd)
            pytest.fail(f"Expected ResponseError for: {cmd}")
        except ResponseError as e:
            assert str(e) == expected_err_reply, (
                f"Actual error '{str(e)}' != expected '{expected_err_reply}'"
            )
            return str(e)

    def verify_command_success_reply(self, client, cmd, expected_result):
        actual = client.execute_command(cmd)
        assert actual == expected_result, (
            f"Actual response '{actual}' != expected '{expected_result}'"
        )
        return actual

    def verify_server_key_count(self, client, expected_num_keys):
        actual = self.server.num_keys()
        assert actual == expected_num_keys, (
            f"Actual key count {actual} != expected {expected_num_keys}"
        )

    def parse_valkey_info(self, section):
        # valkey-py's client.info(section) returns a dict with native types
        # (int/float/str) and keys already at the module's field names
        # (e.g. "flash_cache_hits"), so no prefix-stripping needed.
        return self.client.info(section)

    def verify_nonblocking_during(self, blocking_client, blocking_cmd_fn, probe_client, probe_cmd):
        """Issue blocking_cmd_fn() on blocking_client in a thread, then verify the server
        can still respond to probe_cmd on probe_client while the first command is in flight.
        blocking_cmd_fn is a zero-arg callable that performs the command and returns."""
        errors = []

        def run_blocking():
            try:
                blocking_cmd_fn()
            except Exception as e:
                errors.append(e)

        t = threading.Thread(target=run_blocking, daemon=True)
        t.start()
        # Give the blocking command a moment to reach the server before probing.
        time.sleep(0.05)
        try:
            probe_client.execute_command(probe_cmd)
        except Exception as e:
            errors.append(AssertionError(f"Server blocked during probe: {e}"))
        t.join(timeout=5)
        if errors:
            raise errors[0]

    def assert_latency_below_ms(self, client, cmd, threshold_ms, iterations=100):
        """Assert that the p99 round-trip latency for cmd is below threshold_ms."""
        latencies = []
        for _ in range(iterations):
            start = time.perf_counter()
            client.execute_command(cmd)
            latencies.append((time.perf_counter() - start) * 1000)
        latencies.sort()
        p99 = latencies[int(len(latencies) * 0.99)]
        assert p99 < threshold_ms, (
            f"p99 latency {p99:.2f}ms exceeds threshold {threshold_ms}ms for: {cmd}"
        )

    def wait_for_key_expiry(self, client, key, timeout_s=2):
        """Poll until key is absent or timeout_s elapses; raise AssertionError on timeout."""
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            if client.execute_command(f"EXISTS {key}") == 0:
                return
            time.sleep(0.05)
        raise AssertionError(f"Key '{key}' did not expire within {timeout_s}s")


def _repl_offset(client, field):
    """Read one integer field from `INFO replication` (e.g. master_repl_offset,
    slave_repl_offset). Returns -1 on missing key so callers can keep polling
    until the field becomes readable."""
    info = client.info("replication")
    value = info.get(field)
    if value is None:
        return -1
    return int(value)


def wait_for_replica_caught_up(primary_client, replica_client, timeout_s=5.0):
    """Block until the replica's applied offset matches the primary's current
    offset.

    The upstream `ReplicationTestCase.waitForReplicaToSyncUp` only checks that
    the primary→replica TCP link is up (master_link_status=up), which is
    established during the initial handshake and does not reflect whether a
    post-link command has been streamed and applied. Under load that gap lets
    a follow-up read on the replica miss the write that the test just
    performed on the primary.

    Poll both sides' `INFO replication` offsets: capture the primary's
    `master_repl_offset` at call time and wait until the replica's
    `slave_repl_offset` is at least that high. 50 ms poll interval keeps the
    overhead negligible while still resolving sub-second races.
    """
    target = _repl_offset(primary_client, "master_repl_offset")
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if _repl_offset(replica_client, "slave_repl_offset") >= target >= 0:
            return
        time.sleep(0.05)
    raise AssertionError(f"Replica did not catch up to offset {target} within {timeout_s}s")


class FlashReplicationTestCase(ReplicationTestCase, ValkeyFlashTestCase):
    """ReplicationTestCase that waits for applied-offset parity (not just
    link up) in `waitForReplicaToSyncUp`, and inherits
    `ValkeyFlashTestCase`'s `_flash_server_cleanup` fixture so every replica
    and primary it starts is torn down regardless of how subclasses override
    `setup_test`. See `wait_for_replica_caught_up` for the offset reason and
    `_flash_server_cleanup` for the teardown reason."""

    @pytest.fixture(autouse=True)
    def setup_test(self, setup):
        # Replication subclasses always override this to stand up a primary
        # with per-test module args. The no-op default here prevents
        # ValkeyFlashTestCase's `setup_test` from silently creating a second
        # primary when a subclass forgets to override.
        yield

    def waitForReplicaToSyncUp(self, server):
        # First satisfy the upstream contract (link up), then upgrade to an
        # offset-based wait using the replica's public client.
        super().waitForReplicaToSyncUp(server)
        wait_for_replica_caught_up(self.client, server.client)


def _shutdown_server_handle(handle):
    """Best-effort teardown of a ValkeyServerHandle: graceful SHUTDOWN NOSAVE
    (so the module's `deinitialize` can flush LLVM profile counters, join
    background threads, and close the io_uring), then SIGKILL if the process
    is still alive."""
    with contextlib.suppress(Exception):
        handle.exit()
    proc = getattr(handle, "server", None)
    if proc is not None and hasattr(proc, "poll") and proc.poll() is None:
        with contextlib.suppress(Exception):
            proc.kill()
            proc.wait(timeout=5)
