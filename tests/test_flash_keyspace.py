import time
from valkey_flash_test_case import ValkeyFlashTestCase
from valkeytestframework.conftest import resource_port_tracker


class TestFlashKeyspaceNotifications(ValkeyFlashTestCase):

    def _setup_pubsub(self):
        self.client.execute_command("CONFIG", "SET", "notify-keyspace-events", "KEA")
        sub_client = self.server.get_new_client()
        ps = sub_client.pubsub()
        ps.psubscribe("__key*__:*")
        # Drain the subscription confirmation message.
        timeout = time.time() + 5
        while time.time() < timeout:
            msg = ps.get_message()
            if msg and msg["type"] == "psubscribe":
                break
            time.sleep(0.01)
        return ps

    def _collect_messages(self, ps, count, timeout_secs=5):
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
        assert any(
            m["channel"] == keyspace_ch and m["data"] == event_b for m in messages
        ), f"No keyspace message for event={event} key={key}; got {messages}"
        assert any(
            m["channel"] == keyevent_ch and m["data"] == key_b for m in messages
        ), f"No keyevent message for event={event} key={key}; got {messages}"

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
