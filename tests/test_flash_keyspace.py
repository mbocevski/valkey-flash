import time

from valkey_flash_test_case import ValkeyFlashTestCase


class TestFlashKeyspaceNotifications(ValkeyFlashTestCase):
    def _setup_pubsub(self):
        # Pattern from valkey-bloom tests/test_bloom_keyspace.py: pubsub on the
        # same client object that runs the command. Cross-client pubsub delivery
        # is unreliable in this test environment (standalone Python works, but
        # pytest doesn't forward events between two distinct valkey.Valkey
        # instances bound to the same server). Using one client with its pool
        # internally carving out a dedicated pubsub connection works.
        ps = self.client.pubsub()
        ps.psubscribe("__key*__:*")
        self.client.execute_command(
            "CONFIG", "SET", "notify-keyspace-events", "KEA"
        )
        return ps

    def _collect_messages(self, ps, count, timeout_secs=5):
        # We don't pre-drain the psubscribe ack in _setup_pubsub, so the first
        # call here sees a non-pmessage (psubscribe ack) which we silently skip.
        messages = []
        deadline = time.time() + timeout_secs
        while len(messages) < count and time.time() < deadline:
            msg = ps.get_message()
            if msg and msg["type"] == "pmessage":
                messages.append(msg)
            else:
                time.sleep(0.005)
        return messages

    def _assert_event(self, messages, event, key):
        keyspace_ch = f"__keyspace@0__:{key}".encode()
        keyevent_ch = f"__keyevent@0__:{event}".encode()
        key_b = key.encode()
        event_b = event.encode()
        assert any(m["channel"] == keyspace_ch and m["data"] == event_b for m in messages), (
            f"No keyspace message for event={event} key={key}; got {messages}"
        )
        assert any(m["channel"] == keyevent_ch and m["data"] == key_b for m in messages), (
            f"No keyevent message for event={event} key={key}; got {messages}"
        )

    def test_flash_set_emits_keyspace_event(self):
        ps = self._setup_pubsub()
        self.client.execute_command("FLASH.SET", "ks_str", "hello")
        messages = self._collect_messages(ps, 2)
        self._assert_event(messages, "flash.set", "ks_str")

    def test_flash_del_emits_keyspace_event(self):
        self.client.execute_command("FLASH.SET", "ks_del", "v")
        ps = self._setup_pubsub()
        self.client.execute_command("FLASH.DEL", "ks_del")
        messages = self._collect_messages(ps, 2)
        self._assert_event(messages, "flash.del", "ks_del")

    def test_flash_del_multiple_keys_emits_per_key(self):
        self.client.execute_command("FLASH.SET", "ks_del_a", "v1")
        self.client.execute_command("FLASH.SET", "ks_del_b", "v2")
        ps = self._setup_pubsub()
        self.client.execute_command("FLASH.DEL", "ks_del_a", "ks_del_b")
        messages = self._collect_messages(ps, 4)
        self._assert_event(messages, "flash.del", "ks_del_a")
        self._assert_event(messages, "flash.del", "ks_del_b")

    def test_flash_hset_emits_keyspace_event(self):
        ps = self._setup_pubsub()
        self.client.execute_command("FLASH.HSET", "ks_hash", "f1", "v1")
        messages = self._collect_messages(ps, 2)
        self._assert_event(messages, "flash.hset", "ks_hash")

    def test_flash_hdel_emits_keyspace_event(self):
        self.client.execute_command("FLASH.HSET", "ks_hdel", "f1", "v1", "f2", "v2")
        ps = self._setup_pubsub()
        self.client.execute_command("FLASH.HDEL", "ks_hdel", "f1")
        messages = self._collect_messages(ps, 2)
        self._assert_event(messages, "flash.hdel", "ks_hdel")

    def test_flash_hdel_last_field_emits_keyspace_event(self):
        self.client.execute_command("FLASH.HSET", "ks_hdel_last", "only", "v")
        ps = self._setup_pubsub()
        self.client.execute_command("FLASH.HDEL", "ks_hdel_last", "only")
        messages = self._collect_messages(ps, 2)
        self._assert_event(messages, "flash.hdel", "ks_hdel_last")

    def test_flash_debug_demote_emits_evict_event(self):
        self.client.execute_command("FLASH.SET", "ks_evict", "evict_me")
        ps = self._setup_pubsub()
        self.client.execute_command("FLASH.DEBUG.DEMOTE", "ks_evict")
        messages = self._collect_messages(ps, 2)
        self._assert_event(messages, "flash.evict", "ks_evict")

    def test_no_event_when_notifications_disabled(self):
        self.client.execute_command("CONFIG", "SET", "notify-keyspace-events", "")
        sub_client = self.server.get_new_client()
        ps = sub_client.pubsub()
        ps.psubscribe("__key*__:*")
        time.sleep(0.05)
        # drain subscribe confirmation
        ps.get_message()
        self.client.execute_command("FLASH.SET", "ks_silent", "noevent")
        time.sleep(0.1)
        messages = []
        msg = ps.get_message()
        while msg:
            if msg["type"] == "pmessage":
                messages.append(msg)
            msg = ps.get_message()
        assert messages == [], f"Expected no events but got: {messages}"

    # ── List notifications ─────────────────────────────────────────────────

    def test_list_rpush_emits_keyspace_event(self):
        ps = self._setup_pubsub()
        self.client.execute_command("FLASH.RPUSH", "ks_rpush", "a", "b")
        messages = self._collect_messages(ps, 2)
        self._assert_event(messages, "flash.rpush", "ks_rpush")

    def test_list_lpush_emits_keyspace_event(self):
        ps = self._setup_pubsub()
        self.client.execute_command("FLASH.LPUSH", "ks_lpush", "a", "b")
        messages = self._collect_messages(ps, 2)
        self._assert_event(messages, "flash.lpush", "ks_lpush")

    def test_list_lpop_emits_keyspace_event(self):
        self.client.execute_command("FLASH.RPUSH", "ks_lpop", "x", "y")
        ps = self._setup_pubsub()
        self.client.execute_command("FLASH.LPOP", "ks_lpop")
        messages = self._collect_messages(ps, 2)
        self._assert_event(messages, "flash.lpop", "ks_lpop")

    def test_list_rpop_emits_keyspace_event(self):
        self.client.execute_command("FLASH.RPUSH", "ks_rpop", "x", "y")
        ps = self._setup_pubsub()
        self.client.execute_command("FLASH.RPOP", "ks_rpop")
        messages = self._collect_messages(ps, 2)
        self._assert_event(messages, "flash.rpop", "ks_rpop")

    def test_list_lset_emits_keyspace_event(self):
        self.client.execute_command("FLASH.RPUSH", "ks_lset", "a", "b")
        ps = self._setup_pubsub()
        self.client.execute_command("FLASH.LSET", "ks_lset", "0", "A")
        messages = self._collect_messages(ps, 2)
        self._assert_event(messages, "flash.lset", "ks_lset")

    def test_list_linsert_emits_keyspace_event(self):
        self.client.execute_command("FLASH.RPUSH", "ks_lins", "a", "c")
        ps = self._setup_pubsub()
        self.client.execute_command("FLASH.LINSERT", "ks_lins", "BEFORE", "c", "b")
        messages = self._collect_messages(ps, 2)
        self._assert_event(messages, "flash.linsert", "ks_lins")

    def test_list_lrem_emits_keyspace_event(self):
        self.client.execute_command("FLASH.RPUSH", "ks_lrem", "a", "a", "b")
        ps = self._setup_pubsub()
        self.client.execute_command("FLASH.LREM", "ks_lrem", "0", "a")
        messages = self._collect_messages(ps, 2)
        self._assert_event(messages, "flash.lrem", "ks_lrem")

    def test_list_ltrim_emits_keyspace_event(self):
        self.client.execute_command("FLASH.RPUSH", "ks_ltrim", "a", "b", "c")
        ps = self._setup_pubsub()
        self.client.execute_command("FLASH.LTRIM", "ks_ltrim", "0", "1")
        messages = self._collect_messages(ps, 2)
        self._assert_event(messages, "flash.ltrim", "ks_ltrim")

    def test_list_lmove_emits_keyspace_event(self):
        self.client.execute_command("FLASH.RPUSH", "ks_lmv_src", "a", "b")
        ps = self._setup_pubsub()
        self.client.execute_command("FLASH.LMOVE", "ks_lmv_src", "ks_lmv_dst", "LEFT", "RIGHT")
        messages = self._collect_messages(ps, 4)
        self._assert_event(messages, "flash.lmove", "ks_lmv_src")
        self._assert_event(messages, "flash.lmove", "ks_lmv_dst")

    # ── ZSet notifications ─────────────────────────────────────────────────

    def test_zset_notifications_fire_on_zadd(self):
        ps = self._setup_pubsub()
        self.client.execute_command("FLASH.ZADD", "ks_zadd", "1.0", "a", "2.0", "b")
        messages = self._collect_messages(ps, 2)
        self._assert_event(messages, "flash.zadd", "ks_zadd")

    def test_zset_zrem_emits_keyspace_event(self):
        self.client.execute_command("FLASH.ZADD", "ks_zrem", "1.0", "a", "2.0", "b")
        ps = self._setup_pubsub()
        self.client.execute_command("FLASH.ZREM", "ks_zrem", "a")
        messages = self._collect_messages(ps, 2)
        self._assert_event(messages, "flash.zrem", "ks_zrem")

    def test_zset_zincrby_emits_keyspace_event(self):
        self.client.execute_command("FLASH.ZADD", "ks_zinc", "3.0", "m")
        ps = self._setup_pubsub()
        self.client.execute_command("FLASH.ZINCRBY", "ks_zinc", "2.0", "m")
        messages = self._collect_messages(ps, 2)
        self._assert_event(messages, "flash.zincrby", "ks_zinc")

    def test_zset_zpopmin_emits_keyspace_event(self):
        self.client.execute_command("FLASH.ZADD", "ks_zpm", "1.0", "a", "2.0", "b")
        ps = self._setup_pubsub()
        self.client.execute_command("FLASH.ZPOPMIN", "ks_zpm")
        messages = self._collect_messages(ps, 2)
        self._assert_event(messages, "flash.zpopmin", "ks_zpm")

    def test_zset_zpopmax_emits_keyspace_event(self):
        self.client.execute_command("FLASH.ZADD", "ks_zpmax", "1.0", "a", "2.0", "b")
        ps = self._setup_pubsub()
        self.client.execute_command("FLASH.ZPOPMAX", "ks_zpmax")
        messages = self._collect_messages(ps, 2)
        self._assert_event(messages, "flash.zpopmax", "ks_zpmax")

    def test_zset_zunionstore_emits_keyspace_event(self):
        self.client.execute_command("FLASH.ZADD", "ks_zu1", "1.0", "a")
        self.client.execute_command("FLASH.ZADD", "ks_zu2", "2.0", "b")
        ps = self._setup_pubsub()
        self.client.execute_command("FLASH.ZUNIONSTORE", "ks_zudst", "2", "ks_zu1", "ks_zu2")
        messages = self._collect_messages(ps, 2)
        self._assert_event(messages, "flash.zunionstore", "ks_zudst")

    def test_zset_zinterstore_emits_keyspace_event(self):
        self.client.execute_command("FLASH.ZADD", "ks_zi1", "1.0", "x")
        self.client.execute_command("FLASH.ZADD", "ks_zi2", "2.0", "x")
        ps = self._setup_pubsub()
        self.client.execute_command("FLASH.ZINTERSTORE", "ks_zidst", "2", "ks_zi1", "ks_zi2")
        messages = self._collect_messages(ps, 2)
        self._assert_event(messages, "flash.zinterstore", "ks_zidst")

    def test_zset_zdiffstore_emits_keyspace_event(self):
        self.client.execute_command("FLASH.ZADD", "ks_zd1", "1.0", "a", "2.0", "b")
        self.client.execute_command("FLASH.ZADD", "ks_zd2", "1.0", "a")
        ps = self._setup_pubsub()
        self.client.execute_command("FLASH.ZDIFFSTORE", "ks_zddst", "2", "ks_zd1", "ks_zd2")
        messages = self._collect_messages(ps, 2)
        self._assert_event(messages, "flash.zdiffstore", "ks_zddst")

    def test_zset_zrangestore_emits_keyspace_event(self):
        self.client.execute_command("FLASH.ZADD", "ks_zrs_src", "1.0", "a", "2.0", "b")
        ps = self._setup_pubsub()
        self.client.execute_command("FLASH.ZRANGESTORE", "ks_zrs_dst", "ks_zrs_src", "0", "-1")
        messages = self._collect_messages(ps, 2)
        self._assert_event(messages, "flash.zrangestore", "ks_zrs_dst")

    def test_zadd_nx_existing_fires_no_notification(self):
        self.client.execute_command("FLASH.ZADD", "ks_zadd_nx", "1.0", "m")
        self.client.execute_command("CONFIG", "SET", "notify-keyspace-events", "KEA")
        sub_client = self.server.get_new_client()
        ps = sub_client.pubsub()
        ps.psubscribe("__keyevent@0__:flash.zadd")
        # Drain subscribe confirmation.
        deadline = time.time() + 5
        while time.time() < deadline:
            msg = ps.get_message()
            if msg and msg["type"] == "psubscribe":
                break
            time.sleep(0.01)
        # NX on an existing member — should be a no-op, no notification.
        self.client.execute_command("FLASH.ZADD", "ks_zadd_nx", "NX", "1.0", "m")
        time.sleep(0.1)
        messages = []
        msg = ps.get_message()
        while msg:
            if msg["type"] == "pmessage":
                messages.append(msg)
            msg = ps.get_message()
        assert messages == [], f"Expected no flash.zadd event on NX no-op but got: {messages}"
