import pathlib
import struct

from valkey_flash_test_case import ValkeyFlashTestCase
from valkeytestframework.util.waiters import wait_for_equal

try:
    import crc32c as _crc32c_pkg
except ImportError:
    _crc32c_pkg = None

# ── WAL helpers ───────────────────────────────────────────────────────────────

WAL_MAGIC = 0x5753_4C46  # "FLSW" LE
WAL_VERSION = 1
HEADER_SIZE = 16

OP_VER = 1
OP_PUT = 0x01
OP_DELETE = 0x02


def _wal_path(flash_path: str) -> pathlib.Path:
    """Derive WAL path from flash backing-store path (same dir, .wal extension)."""
    return pathlib.Path(flash_path).with_suffix(".wal")


def _default_flash_path(test_case=None) -> str:
    """Return the per-test backing-store path. The `ValkeyFlashTestCase` fixture
    creates a distinct `self.flash_dir` under `testdir` for every test; tests
    pass `self` so the helper can locate that dir."""
    if test_case is not None and hasattr(test_case, "flash_dir"):
        import os as _os
        return _os.path.join(test_case.flash_dir, "flash.bin")
    return "/tmp/valkey-flash.bin"


def _encode_put(key_hash: int, offset: int, value_hash: int) -> bytes:
    """Encode a Put payload (op_ver + op_code + 24-byte body)."""
    return struct.pack("<BBQQQ", OP_VER, OP_PUT, key_hash, offset, value_hash)


def _encode_delete(key_hash: int) -> bytes:
    """Encode a Delete payload (op_ver + op_code + 8-byte body)."""
    return struct.pack("<BBQ", OP_VER, OP_DELETE, key_hash)


def _crc32c(data: bytes) -> int:
    """Compute CRC32C (Castagnoli).  Uses the `crc32c` package when available,
    falls back to a pure-Python table for test environments that lack it."""
    if _crc32c_pkg is not None:
        return _crc32c_pkg.crc32c(data)
    # Pure-Python CRC32C (Castagnoli polynomial 0x1EDC6F41, reflected 0x82F63B78)
    crc = 0xFFFF_FFFF
    table = []
    for i in range(256):
        k = i
        for _ in range(8):
            if k & 1:
                k = (k >> 1) ^ 0x82F63B78
            else:
                k >>= 1
        table.append(k)
    for byte in data:
        crc = (crc >> 8) ^ table[(crc ^ byte) & 0xFF]
    return crc ^ 0xFFFF_FFFF


def _frame(payload: bytes) -> bytes:
    """Wrap payload in [u32 LE len][u32 LE CRC32C][payload]."""
    crc = _crc32c(payload)
    return struct.pack("<II", len(payload), crc) + payload


def _write_wal(path: pathlib.Path, payloads: list[bytes]) -> None:
    """Write a WAL file from scratch with the given encoded payloads."""
    header = struct.pack("<IB", WAL_MAGIC, WAL_VERSION) + b"\x00" * 11
    with open(path, "wb") as f:
        f.write(header)
        for p in payloads:
            f.write(_frame(p))


# ── Restart helper ────────────────────────────────────────────────────────────


def _bgsave_and_restart(server):
    server.client.execute_command("BGSAVE")
    server.wait_for_save_done()
    server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
    assert server.is_alive()
    wait_for_equal(server.is_rdb_done_loading, True)


# ── Recovery tests ────────────────────────────────────────────────────────────


class TestFlashRecovery(ValkeyFlashTestCase):
    def test_state_is_ready_on_fresh_start(self):
        result = self.client.execute_command("FLASH.DEBUG.STATE")
        assert result == b"ready"

    def test_recovery_complete_logged_on_fresh_start(self):
        # Module always logs "flash: recovery complete" during initialize().
        self.server.verify_string_in_logfile("flash: recovery complete:")

    def test_recovery_zero_records_on_fresh_start(self):
        self.server.verify_string_in_logfile("flash: recovery complete: 0 records applied")

    def test_state_ready_after_bgsave_restart(self):
        for i in range(3):
            self.client.execute_command("FLASH.SET", f"k{i}", f"v{i}")
        _bgsave_and_restart(self.server)
        assert self.client.execute_command("FLASH.DEBUG.STATE") == b"ready"

    def test_keys_survive_bgsave_restart(self):
        for i in range(3):
            self.client.execute_command("FLASH.SET", f"k{i}", f"v{i}")
        _bgsave_and_restart(self.server)
        for i in range(3):
            assert self.client.execute_command("FLASH.GET", f"k{i}") == f"v{i}".encode()

    def test_recovery_zero_records_after_bgsave_empty_wal(self):
        # BGSAVE with no WAL records → recovery replays 0 records.
        _bgsave_and_restart(self.server)
        self.server.verify_string_in_logfile("flash: recovery complete: 0 records applied")

    def _sigkill_server(self):
        """SIGKILL the server process and reset the framework's handle so
        `start()` will spawn a new process (instead of refusing with 'Server
        already started')."""
        import os as _os
        import signal as _signal
        _os.kill(self.server.pid(), _signal.SIGKILL)
        self.server.server.wait(timeout=5)
        # Let framework's start() know there is no running server.
        self.server.server = None
        if self.client is not None:
            try:
                self.client.close()
            except Exception:
                pass
        self.client = None

    def _write_corrupt_wal_offline(self):
        """Stop the server, overwrite its WAL with a corrupt frame, and leave the
        server stopped so the caller can restart into the tampered state.

        Writing the WAL while the server is still running races with the flusher
        thread and, on clean shutdown, gets silently rewritten — which defeats
        the test's purpose."""
        self._sigkill_server()
        wal_path = _wal_path(_default_flash_path(self))
        header = struct.pack("<IB", WAL_MAGIC, WAL_VERSION) + b"\x00" * 11
        corrupt_frame = struct.pack("<II", 10, 0xDEADBEEF) + b"\xff" * 10
        with open(wal_path, "wb") as f:
            f.write(header)
            f.write(corrupt_frame)
        return wal_path

    def test_corrupted_wal_module_loads_ready(self):
        # Write a WAL file with a corrupt frame; module should still load ready.
        self._write_corrupt_wal_offline()
        # Fresh start() after SIGKILL: no RDB was written, so
        # is_rdb_done_loading would never flip to True. start() already
        # waits for "Ready to accept connections" before returning.
        self.client = self.server.start(connect_client=True)
        assert self.server.is_alive()
        assert self.client.execute_command("FLASH.DEBUG.STATE") == b"ready"

    def test_corrupted_wal_warn_logged(self):
        self._write_corrupt_wal_offline()
        # Fresh start() after SIGKILL: no RDB was written, so
        # is_rdb_done_loading would never flip to True. start() already
        # waits for "Ready to accept connections" before returning.
        self.client = self.server.start(connect_client=True)
        assert self.server.is_alive()
        self.server.verify_string_in_logfile("flash: recovery: WAL corruption at offset")

    def test_corrupted_wal_truncated_to_header(self):
        wal_path = self._write_corrupt_wal_offline()
        # Fresh start() after SIGKILL: no RDB was written, so
        # is_rdb_done_loading would never flip to True. start() already
        # waits for "Ready to accept connections" before returning.
        self.client = self.server.start(connect_client=True)
        assert self.server.is_alive()
        # WAL should be truncated to just the 16-byte header.
        assert wal_path.stat().st_size == HEADER_SIZE

    def test_valid_wal_records_applied_on_recovery(self):
        # Write WAL records manually; recovery should count them.
        # SIGKILL the server first — otherwise clean shutdown rewrites the WAL.
        self._sigkill_server()

        wal_path = _wal_path(_default_flash_path(self))
        _write_wal(
            wal_path,
            [
                _encode_put(1, 0, 0xABCD),
                _encode_put(2, 4096, 0xEF01),
                _encode_delete(1),
            ],
        )

        # Fresh start() after SIGKILL: no RDB was written, so
        # is_rdb_done_loading would never flip to True. start() already
        # waits for "Ready to accept connections" before returning.
        self.client = self.server.start(connect_client=True)
        assert self.server.is_alive()
        assert self.client.execute_command("FLASH.DEBUG.STATE") == b"ready"
        self.server.verify_string_in_logfile("flash: recovery complete: 3 records applied")

    # NOTE: stale-cursor integration test (cursor skips records already in RDB)
    # is deferred to the WAL-wiring task (#58). The cursor-skipping logic is
    # verified by the unit test `recovery::tests::cursor_skips_earlier_records`.
    # Only once FLASH.SET/DEL write WAL records does `current_offset()` naturally
    # advance past them at BGSAVE time, making the integration assertion reliable.
