import os
import threading
import time

import pytest
from valkey import ResponseError
from valkeytestframework.valkey_test_case import ValkeyTestCase


class ValkeyFlashTestCase(ValkeyTestCase):
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
        args = {
            "enable-debug-command": "yes",
            "loadmodule": os.getenv("MODULE_PATH"),
        }
        self.server, self.client = self.create_server(
            testdir=self.testdir, server_path=server_path, args=args
        )

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
        raw = self.client.execute_command("INFO " + section)
        lines = raw.decode("utf-8").split("\r\n")
        result = {}
        for line in lines:
            if ":" in line:
                key, value = line.split(":", 1)
                result[key.strip()] = value.strip()
        return result

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
