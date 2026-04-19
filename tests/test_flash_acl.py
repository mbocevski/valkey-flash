from contextlib import suppress

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
            with suppress(Exception):
                self.client.execute_command("ACL", "DELUSER", name)

    def _cat(self, category):
        """Return the set of commands in an ACL category as lowercase bytes."""
        cmds = self.client.execute_command("ACL", "CAT", category)
        return {c.lower() if isinstance(c, bytes) else c.lower().encode() for c in cmds}

    def test_acl_cat_flash_lists_flash_commands(self):
        cmds = self._cat("flash")
        for cmd in ALL_FLASH_COMMANDS:
            assert cmd in cmds, f"{cmd} not found in ACL CAT flash: {sorted(cmds)}"

    def test_flash_set_acl_categories(self):
        assert b"flash.set" in self._cat("write")
        assert b"flash.set" in self._cat("flash")

    def test_flash_get_acl_categories(self):
        assert b"flash.get" in self._cat("read")
        assert b"flash.get" in self._cat("flash")

    def test_flash_debug_demote_acl_categories(self):
        assert b"flash.debug.demote" in self._cat("admin")
        assert b"flash.debug.demote" in self._cat("dangerous")
        assert b"flash.debug.demote" in self._cat("flash")

    def test_flash_compaction_trigger_acl_categories(self):
        assert b"flash.compaction.trigger" in self._cat("admin")
        assert b"flash.compaction.trigger" in self._cat("dangerous")
        assert b"flash.compaction.trigger" in self._cat("flash")

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
            assert (
                "NOPERM" in str(exc_info.value) or "no permissions" in str(exc_info.value).lower()
            )
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
            assert (
                "NOPERM" in str(exc_info.value) or "no permissions" in str(exc_info.value).lower()
            )
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
            assert (
                "NOPERM" in str(exc_info.value) or "no permissions" in str(exc_info.value).lower()
            )
        finally:
            self._acl_deluser("flash_none")

    def test_flash_list_acl_categories(self):
        write, read, fast, slow, flash = (
            self._cat("write"), self._cat("read"), self._cat("fast"),
            self._cat("slow"), self._cat("flash"),
        )
        # Write commands must be @write @flash
        assert b"flash.lpush" in write and b"flash.lpush" in flash
        assert b"flash.lpush" not in read
        # Read commands must be @read @flash
        assert b"flash.lrange" in read and b"flash.lrange" in flash
        assert b"flash.lrange" not in write
        # O(1) read must also carry @fast
        assert b"flash.llen" in read and b"flash.llen" in fast and b"flash.llen" in flash
        # Blocking commands must be @write @slow @flash
        assert b"flash.blpop" in write and b"flash.blpop" in slow and b"flash.blpop" in flash

    def test_flash_zset_acl_categories(self):
        write, read, fast, slow, flash = (
            self._cat("write"), self._cat("read"), self._cat("fast"),
            self._cat("slow"), self._cat("flash"),
        )
        # Write commands must be @write @flash
        assert b"flash.zadd" in write and b"flash.zadd" in flash
        assert b"flash.zadd" not in read
        # O(1) read commands must be @read @fast @flash
        assert b"flash.zscore" in read and b"flash.zscore" in fast and b"flash.zscore" in flash
        assert b"flash.zcard" in read and b"flash.zcard" in fast and b"flash.zcard" in flash
        # Range reads must be @read @flash (not @fast — O(log N + M))
        assert b"flash.zrange" in read and b"flash.zrange" in flash
        # Blocking commands must be @write @slow @flash
        assert b"flash.bzpopmin" in write and b"flash.bzpopmin" in slow and b"flash.bzpopmin" in flash
        # Store ops must be @write @slow @flash
        assert b"flash.zunionstore" in write and b"flash.zunionstore" in slow and b"flash.zunionstore" in flash
