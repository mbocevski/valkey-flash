import pathlib

from valkey_flash_test_case import ValkeyFlashTestCase
from valkeytestframework.util.waiters import wait_for_equal


def _wal_path(test_case) -> pathlib.Path:
    """WAL sits next to the backing store; the test case owns both paths."""
    return pathlib.Path(test_case.flash_dir) / "flash.wal"


def _bgsave_and_restart(server):
    server.client.execute_command("BGSAVE")
    server.wait_for_save_done()
    server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
    assert server.is_alive()
    wait_for_equal(server.is_rdb_done_loading, True)


def _crash_restart(server):
    server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
    assert server.is_alive()
    wait_for_equal(server.is_rdb_done_loading, True)


class TestFlashDurability(ValkeyFlashTestCase):
    def test_set_grows_wal_file(self):
        wal = _wal_path(self)
        size_before = wal.stat().st_size
        self.client.execute_command("FLASH.SET", "k1", "v1")
        assert wal.stat().st_size > size_before

    def test_del_grows_wal_file(self):
        self.client.execute_command("FLASH.SET", "k1", "v1")
        wal = _wal_path(self)
        size_after_set = wal.stat().st_size
        self.client.execute_command("FLASH.DEL", "k1")
        assert wal.stat().st_size > size_after_set

    def test_set_wal_records_replayed_after_restart(self):
        # Establish a clean cursor baseline; 3 new SETs will land past it.
        _bgsave_and_restart(self.server)
        for i in range(3):
            self.client.execute_command("FLASH.SET", f"k{i}", f"v{i}")
        _crash_restart(self.server)
        self.server.verify_string_in_logfile("flash: recovery complete: 3 records applied")

    def test_del_wal_record_replayed_after_restart(self):
        # Clean cursor, then SET + DEL = 1 Put record + 1 Delete record.
        _bgsave_and_restart(self.server)
        self.client.execute_command("FLASH.SET", "k1", "v1")
        self.client.execute_command("FLASH.DEL", "k1")
        _crash_restart(self.server)
        self.server.verify_string_in_logfile("flash: recovery complete: 2 records applied")

    def test_state_ready_after_wal_replay(self):
        # Module reaches Ready state after replaying WAL records.
        _bgsave_and_restart(self.server)
        for i in range(5):
            self.client.execute_command("FLASH.SET", f"k{i}", f"v{i}")
        _crash_restart(self.server)
        assert self.client.execute_command("FLASH.DEBUG.STATE") == b"ready"

    def test_bgsave_cursor_skips_pre_save_records(self):
        # k1 is SET before BGSAVE — cursor advances past its WAL record.
        # k2 is SET after BGSAVE — its record is past the cursor.
        # After crash restart: only k2's record is replayed (1 record applied).
        _bgsave_and_restart(self.server)
        self.client.execute_command("FLASH.SET", "k1", "v1")
        _bgsave_and_restart(self.server)
        self.client.execute_command("FLASH.SET", "k2", "v2")
        _crash_restart(self.server)
        self.server.verify_string_in_logfile("flash: recovery complete: 1 records applied")
