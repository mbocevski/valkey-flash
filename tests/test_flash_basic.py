from valkey_flash_test_case import ValkeyFlashTestCase


class TestFlashBasic(ValkeyFlashTestCase):
    def test_module_loads(self):
        client = self.server.get_new_client()
        modules = client.execute_command("MODULE LIST")
        names = [m[b"name"] for m in modules]
        assert b"flash" in names, f"flash module not found in MODULE LIST: {names}"
