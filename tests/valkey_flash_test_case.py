import os
import pytest
from valkeytestframework.valkey_test_case import ValkeyTestCase
from valkey import ResponseError


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
        os.environ["LD_LIBRARY_PATH"] = (
            f"{binaries_dir}:{existing}" if existing else binaries_dir
        )
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
            assert False, f"Expected ResponseError for: {cmd}"
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
