import pytest
from valkey_flash_test_case import ValkeyFlashTestCase

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
    # List commands
    b"flash.lpush",
    b"flash.rpush",
    b"flash.lpushx",
    b"flash.rpushx",
    b"flash.lpop",
    b"flash.rpop",
    b"flash.lrange",
    b"flash.llen",
    b"flash.lindex",
    b"flash.lset",
    b"flash.linsert",
    b"flash.lrem",
    b"flash.ltrim",
    b"flash.lmove",
    b"flash.blpop",
    b"flash.brpop",
    b"flash.blmove",
    # ZSet commands
    b"flash.zadd",
    b"flash.zrem",
    b"flash.zincrby",
    b"flash.zpopmin",
    b"flash.zpopmax",
    b"flash.zscore",
    b"flash.zrank",
    b"flash.zrevrank",
    b"flash.zcard",
    b"flash.zcount",
    b"flash.zlexcount",
    b"flash.zrange",
    b"flash.zrangebyscore",
    b"flash.zrevrangebyscore",
    b"flash.zrangebylex",
    b"flash.zrevrangebylex",
    b"flash.zscan",
    b"flash.bzpopmin",
    b"flash.bzpopmax",
    b"flash.zunionstore",
    b"flash.zinterstore",
    b"flash.zdiffstore",
    b"flash.zrangestore",
    # Admin
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
        # Grant all @flash commands then revoke @write: FLASH.GET (read) is
        # permitted but FLASH.SET (write) is denied.
        self._acl_setuser("flash_ro", "on >pass ~* &* -@all +@flash -@write")
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
        # Start with +@all to ensure other commands work, then revoke @flash:
        # FLASH.SET must be denied while the user still has general access.
        self._acl_setuser("flash_none", "on >pass ~* &* +@all -@flash")
        try:
            c = self.server.get_new_client()
            c.execute_command("AUTH", "flash_none", "pass")
            with pytest.raises(Exception) as exc_info:
                c.execute_command("FLASH.SET", "acl_none_k", "v")
            assert "NOPERM" in str(exc_info.value) or "no permissions" in str(exc_info.value).lower()
        finally:
            self._acl_deluser("flash_none")

    def test_flash_list_acl_categories(self):
        def categories(cmd):
            return self.client.execute_command("COMMAND", "INFO", cmd)[0][6]

        # Write commands must be @write @flash
        lp = categories("FLASH.LPUSH")
        assert b"@write" in lp
        assert b"@flash" in lp
        assert b"@read" not in lp

        # Read commands must be @read @flash
        lr = categories("FLASH.LRANGE")
        assert b"@read" in lr
        assert b"@flash" in lr
        assert b"@write" not in lr

        # O(1) read must also carry @fast
        ll = categories("FLASH.LLEN")
        assert b"@read" in ll
        assert b"@fast" in ll
        assert b"@flash" in ll

        # Blocking commands must be @slow @flash
        bl = categories("FLASH.BLPOP")
        assert b"@write" in bl
        assert b"@slow" in bl
        assert b"@flash" in bl

    def test_flash_zset_acl_categories(self):
        def categories(cmd):
            return self.client.execute_command("COMMAND", "INFO", cmd)[0][6]

        # Write commands must be @write @flash
        za = categories("FLASH.ZADD")
        assert b"@write" in za
        assert b"@flash" in za
        assert b"@read" not in za

        # O(1) read commands must be @read @fast @flash
        zs = categories("FLASH.ZSCORE")
        assert b"@read" in zs
        assert b"@fast" in zs
        assert b"@flash" in zs

        zc = categories("FLASH.ZCARD")
        assert b"@read" in zc
        assert b"@fast" in zc
        assert b"@flash" in zc

        # Range reads must be @read @flash (not @fast — O(log N + M))
        zr = categories("FLASH.ZRANGE")
        assert b"@read" in zr
        assert b"@flash" in zr

        # Blocking commands must be @slow @flash
        bz = categories("FLASH.BZPOPMIN")
        assert b"@write" in bz
        assert b"@slow" in bz
        assert b"@flash" in bz

        # Store ops must be @write @slow @flash
        zu = categories("FLASH.ZUNIONSTORE")
        assert b"@write" in zu
        assert b"@slow" in zu
        assert b"@flash" in zu
