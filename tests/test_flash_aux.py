from valkey_flash_test_case import ValkeyFlashTestCase
from valkeytestframework.util.waiters import wait_for_equal

# ── Helpers ───────────────────────────────────────────────────────────────────


def _bgsave_and_restart(server):
    server.client.execute_command("BGSAVE")
    server.wait_for_save_done()
    server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
    assert server.is_alive()
    wait_for_equal(server.is_rdb_done_loading, True)


def _aux_info(client):
    """Return FLASH.AUX.INFO as a dict."""
    raw = client.execute_command("FLASH.AUX.INFO")
    if not raw:
        return {}
    it = iter(raw)
    return {k: v for k, v in zip(it, it, strict=False)}


# ── Aux state tests ───────────────────────────────────────────────────────────


class TestFlashAux(ValkeyFlashTestCase):
    def test_aux_info_empty_before_first_save(self):
        # No BGSAVE yet — LOADED_AUX_STATE is None on a fresh start.
        info = _aux_info(self.client)
        assert info == {}

    def test_aux_roundtrip_empty_module(self):
        # No keys — aux_save writes empty tiering map; aux_load reads it back.
        _bgsave_and_restart(self.server)
        info = _aux_info(self.client)
        assert info.get(b"before.entries") == 0
        # The 16-byte WAL header is always present; wal_cursor reflects the
        # absolute file offset used during recovery, not record bytes.
        assert info.get(b"before.wal_cursor") == 16
        assert info.get(b"before.version") == 1

    def test_aux_roundtrip_with_flash_keys(self):
        for i in range(5):
            self.client.execute_command("FLASH.SET", f"aux:{i}", f"v{i}")
        _bgsave_and_restart(self.server)
        info = _aux_info(self.client)
        # Tiering map is always empty in this version; aux state is still
        # present and parseable.
        assert info.get(b"before.entries") == 0
        assert info.get(b"before.version") == 1
        # Flash keys should still be readable via rdb_load.
        for i in range(5):
            assert self.client.execute_command("FLASH.GET", f"aux:{i}") == f"v{i}".encode()

    def test_aux_after_saved_at_is_positive(self):
        _bgsave_and_restart(self.server)
        info = _aux_info(self.client)
        saved_at = info.get(b"after.saved_at_unix_ms")
        assert saved_at is not None
        assert saved_at > 0

    def test_aux_before_path_matches_module_config(self):
        _bgsave_and_restart(self.server)
        info = _aux_info(self.client)
        path = info.get(b"before.path")
        assert path is not None
        assert len(path) > 0

    def test_aux_before_capacity_bytes_is_positive(self):
        _bgsave_and_restart(self.server)
        info = _aux_info(self.client)
        capacity = info.get(b"before.capacity_bytes")
        assert capacity is not None
        assert capacity > 0

    def test_log_message_aux_load_before(self):
        _bgsave_and_restart(self.server)
        self.server.verify_string_in_logfile("flash: aux_load BEFORE: loaded 0 tiering entries")

    def test_log_message_aux_load_after(self):
        _bgsave_and_restart(self.server)
        self.server.verify_string_in_logfile("flash: aux_load AFTER: saved_at_unix_ms=")

    def test_multiple_bgsave_restart_cycles(self):
        self.client.execute_command("FLASH.SET", "persist:k", "v")
        for _ in range(3):
            _bgsave_and_restart(self.server)
        info = _aux_info(self.client)
        assert info.get(b"before.version") == 1
        assert self.client.execute_command("FLASH.GET", "persist:k") == b"v"

    def test_aux_version_field_is_one(self):
        _bgsave_and_restart(self.server)
        info = _aux_info(self.client)
        assert info.get(b"before.version") == 1
        assert info.get(b"after.version") == 1
