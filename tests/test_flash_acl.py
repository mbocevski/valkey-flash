import pytest
from valkey_flash_test_case import ValkeyFlashTestCase
from valkeytestframework.conftest import resource_port_tracker


ALL_FLASH_COMMANDS = [
    b"flash.set",
    b"flash.get",
    b"flash.del",
    b"flash.hset",
    b"flash.hget",
    b"flash.hgetall",
    b"flash.hdel",
    b"flash.hexists",
    b"flash.hlen",
    b"flash.debug.demote",
    b"flash.debug.state",
    b"flash.compaction.stats",
    b"flash.compaction.trigger",
    b"flash.aux.info",
]


class TestFlashACLCategory(ValkeyFlashTestCase):

    def _acl_setuser(self, name, rules):
        self.client.execute_command("ACL", "SETUSER", name, *rules.split())

    def _acl_deluser(self, *names):
        for name in names:
            try:
                self.client.execute_command("ACL", "DELUSER", name)
            except Exception:
                pass

    def test_acl_cat_flash_lists_flash_commands(self):
        cmds = self.client.execute_command("ACL", "CAT", "flash")
        for cmd in ALL_FLASH_COMMANDS:
            assert cmd in cmds, f"{cmd} not found in ACL CAT flash: {cmds}"

    def test_command_list_filterby_aclcat_flash(self):
        cmds = self.client.execute_command("COMMAND", "LIST", "FILTERBY", "ACLCAT", "flash")
        for cmd in ALL_FLASH_COMMANDS:
            assert cmd in cmds, f"{cmd} not found in COMMAND LIST FILTERBY ACLCAT flash"

    def test_flash_set_acl_categories(self):
        info = self.client.execute_command("COMMAND", "INFO", "FLASH.SET")
        categories = info[0][6]
        assert b"@write" in categories
        assert b"@flash" in categories

    def test_flash_get_acl_categories(self):
        info = self.client.execute_command("COMMAND", "INFO", "FLASH.GET")
        categories = info[0][6]
        assert b"@read" in categories
        assert b"@flash" in categories

    def test_flash_debug_demote_acl_categories(self):
        info = self.client.execute_command("COMMAND", "INFO", "FLASH.DEBUG.DEMOTE")
        categories = info[0][6]
        assert b"@admin" in categories
        assert b"@dangerous" in categories
        assert b"@flash" in categories

    def test_flash_compaction_trigger_acl_categories(self):
        info = self.client.execute_command("COMMAND", "INFO", "FLASH.COMPACTION.TRIGGER")
        categories = info[0][6]
        assert b"@admin" in categories
        assert b"@dangerous" in categories
        assert b"@flash" in categories

    def test_full_flash_user_can_read_and_write(self):
        self._acl_setuser("flash_full", "on >pass ~* &* -@all +@flash")
        try:
            c = self.server.get_new_client()
            c.execute_command("AUTH", "flash_full", "pass")
            assert c.execute_command("FLASH.SET", "acl_k", "acl_v") == b"OK"
            assert c.execute_command("FLASH.GET", "acl_k") == b"acl_v"
        finally:
            self._acl_deluser("flash_full")

    def test_read_only_flash_user_cannot_write(self):
        self._acl_setuser("flash_ro", "on >pass ~* &* -@all +@read +@flash")
        try:
            c = self.server.get_new_client()
            c.execute_command("AUTH", "flash_ro", "pass")
            # Seed key as admin.
            self.client.execute_command("FLASH.SET", "acl_ro_k", "val")
            # Read should work.
            assert c.execute_command("FLASH.GET", "acl_ro_k") == b"val"
            # Write must be denied.
            with pytest.raises(Exception) as exc_info:
                c.execute_command("FLASH.SET", "acl_ro_k", "new")
            assert "NOPERM" in str(exc_info.value) or "no permissions" in str(exc_info.value).lower()
        finally:
            self._acl_deluser("flash_ro")

    def test_no_admin_user_cannot_demote(self):
        self._acl_setuser("flash_noadmin", "on >pass ~* &* -@all +@flash -@admin")
        try:
            c = self.server.get_new_client()
            c.execute_command("AUTH", "flash_noadmin", "pass")
            self.client.execute_command("FLASH.SET", "acl_demote_k", "v")
            with pytest.raises(Exception) as exc_info:
                c.execute_command("FLASH.DEBUG.DEMOTE", "acl_demote_k")
            assert "NOPERM" in str(exc_info.value) or "no permissions" in str(exc_info.value).lower()
        finally:
            self._acl_deluser("flash_noadmin")

    def test_no_flash_user_cannot_run_any_flash_command(self):
        self._acl_setuser("flash_none", "on >pass ~* &* -@flash")
        try:
            c = self.server.get_new_client()
            c.execute_command("AUTH", "flash_none", "pass")
            with pytest.raises(Exception) as exc_info:
                c.execute_command("FLASH.SET", "acl_none_k", "v")
            assert "NOPERM" in str(exc_info.value) or "no permissions" in str(exc_info.value).lower()
        finally:
            self._acl_deluser("flash_none")
