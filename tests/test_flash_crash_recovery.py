import os
import pathlib
import re
import signal
import struct
import threading

import pytest
from valkey_flash_test_case import ValkeyFlashTestCase
from valkeytestframework.util.waiters import wait_for_equal

try:
    import crc32c as _crc32c_lib
except ImportError:
    _crc32c_lib = None

# ── WAL layout constants ──────────────────────────────────────────────────────

WAL_MAGIC = 0x5753_4C46  # "FLSW" LE
WAL_VERSION = 1
HEADER_SIZE = 16  # [u32 magic][u8 version][11 pad]
PUT_PAYLOAD_SIZE = 26  # OP_VER(1)+OP_PUT(1)+key_hash(8)+offset(8)+value_hash(8)
FRAME_OVERHEAD = 8  # [u32 len][u32 crc]
PUT_FRAME_SIZE = FRAME_OVERHEAD + PUT_PAYLOAD_SIZE  # 34 bytes per Put record

OP_VER = 1
OP_PUT = 0x01
OP_DELETE = 0x02


def _wal_path(flash_dir: str | pathlib.Path | None = None) -> pathlib.Path:
    if flash_dir is None:
        return pathlib.Path("/tmp/valkey-flash.wal")
    return pathlib.Path(flash_dir) / "flash.wal"


def _fresh_wal_header() -> bytes:
    return struct.pack("<IB", WAL_MAGIC, WAL_VERSION) + b"\x00" * 11


def _crc32c(data: bytes) -> int:
    if _crc32c_lib is not None:
        return _crc32c_lib.crc32c(data)
    else:
        crc = 0xFFFF_FFFF
        table = []
        for i in range(256):
            k = i
            for _ in range(8):
                k = (k >> 1) ^ 0x82F63B78 if k & 1 else k >> 1
            table.append(k)
        for byte in data:
            crc = (crc >> 8) ^ table[(crc ^ byte) & 0xFF]
        return crc ^ 0xFFFF_FFFF


# ── Restart helpers ───────────────────────────────────────────────────────────


def _bgsave_and_restart(server):
    """BGSAVE + restart: advances the WAL cursor past all current records."""
    server.client.execute_command("BGSAVE")
    server.wait_for_save_done()
    server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
    assert server.is_alive()
    wait_for_equal(server.is_rdb_done_loading, True)


def _crash_restart(server):
    """Restart without BGSAVE (simulates crash): replays WAL from saved cursor."""
    server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
    assert server.is_alive()
    wait_for_equal(server.is_rdb_done_loading, True)


def _sigkill(server):
    """Hard-kill the server with SIGKILL. Does not allow any cleanup."""
    os.kill(server.pid(), signal.SIGKILL)
    server.server.wait(timeout=5)  # block until process is dead


def _recovery_count(server) -> int:
    """Return the 'N records applied' count from the most recent startup log."""
    log_path = os.path.join(server.cwd, server.args["logfile"])
    with open(log_path) as f:
        content = f.read()
    m = re.search(r"flash: recovery complete: (\d+) records applied", content)
    return int(m.group(1)) if m else -1


# ── Base class that customises the sync mode ──────────────────────────────────


class _CrashTestBase(ValkeyFlashTestCase):
    """ValkeyFlashTestCase that passes flash.sync=<_sync_mode> at server startup."""

    _sync_mode: str = "everysec"

    @pytest.fixture(autouse=True)
    def setup_test(self, setup):
        binaries_dir = (
            f"{os.path.dirname(os.path.realpath(__file__))}"
            f"/build/binaries/{os.environ['SERVER_VERSION']}"
        )
        server_path = os.path.join(binaries_dir, "valkey-server")
        existing = os.environ.get("LD_LIBRARY_PATH", "")
        os.environ["LD_LIBRARY_PATH"] = f"{binaries_dir}:{existing}" if existing else binaries_dir
        import tempfile

        self._flash_dir = tempfile.mkdtemp(prefix="flash_crash_", dir=self.testdir)
        flash_path = os.path.join(self._flash_dir, "flash.bin")
        args = {
            "enable-debug-command": "yes",
            "loadmodule": (
                f"{os.getenv('MODULE_PATH')} "
                f"path {flash_path} "
                f"capacity-bytes 16777216 "
                f"sync {self._sync_mode}"
            ),
        }
        self.server, self.client = self.create_server(
            testdir=self.testdir, server_path=server_path, args=args
        )
        yield
        import shutil

        shutil.rmtree(self._flash_dir, ignore_errors=True)


# ── Scenarios 1 & 2: always mode ─────────────────────────────────────────────


class TestFlashCrashAlwaysMode(_CrashTestBase):
    """Scenarios 1 & 2: flash.sync=always — WAL is fsynced before reply."""

    _sync_mode = "always"

    @pytest.mark.crash_recovery
    def test_sigkill_mid_write_bounded_loss(self):
        """Scenario 1: SIGKILL before reply → 0 or 1 records applied (bounded loss).

        The reply hasn't landed so the client can retry safely. The server must
        restart to a consistent, ready state regardless of whether the in-flight
        WAL write completed before the kill.
        """
        _bgsave_and_restart(self.server)  # establish clean cursor

        errors = []

        def do_set():
            try:
                self.client.execute_command("FLASH.SET", "mid_write_key", "v")
            except Exception as e:
                errors.append(e)  # connection reset is expected

        t = threading.Thread(target=do_set, daemon=True)
        t.start()
        _sigkill(self.server)  # kill before reply (racy by design)
        t.join(timeout=2)

        _crash_restart(self.server)
        assert self.client.execute_command("FLASH.DEBUG.STATE") == b"ready"
        # Bounded-loss guarantee: at most 1 in-flight record may be applied.
        count = _recovery_count(self.server)
        assert 0 <= count <= 1, f"Expected 0 or 1 records applied, got {count}"

    @pytest.mark.crash_recovery
    def test_sigkill_after_reply_record_is_durable(self):
        """Scenario 2: SIGKILL after +OK reply → WAL record durable, 1 record applied.

        With always mode, the WAL fsync completes before the reply is sent to
        the client. A hard kill after receiving the reply must not lose the record.
        """
        _bgsave_and_restart(self.server)  # clean cursor at P0

        self.client.execute_command("FLASH.SET", "durable_key", "durable_val")
        # Reply received: WAL Put was fsynced before this point.
        _sigkill(self.server)

        _crash_restart(self.server)
        assert self.client.execute_command("FLASH.DEBUG.STATE") == b"ready"
        assert _recovery_count(self.server) == 1


# ── Scenario 3: everysec mode ─────────────────────────────────────────────────


class TestFlashCrashEverysecMode(_CrashTestBase):
    """Scenario 3: flash.sync=everysec — fsync happens in background every second."""

    _sync_mode = "everysec"

    @pytest.mark.crash_recovery
    def test_sigkill_within_fsync_window_bounded_loss(self):
        """Scenario 3: SIGKILL immediately after reply in everysec mode.

        The WAL write may be in the OS buffer (not fsynced). Loss of up to 1s
        of writes is acceptable. Server must restart cleanly.
        """
        _bgsave_and_restart(self.server)  # clean cursor

        self.client.execute_command("FLASH.SET", "everysec_key", "v")
        _sigkill(self.server)  # kill before background fsync fires

        _crash_restart(self.server)
        assert self.client.execute_command("FLASH.DEBUG.STATE") == b"ready"
        # Key may or may not be in WAL depending on OS buffer flush timing.
        count = _recovery_count(self.server)
        assert 0 <= count <= 1, f"Expected 0 or 1 records applied, got {count}"


# ── Scenario 4: no-sync mode ──────────────────────────────────────────────────


class TestFlashCrashNoSyncMode(_CrashTestBase):
    """Scenario 4: flash.sync=no — writes are never fsynced proactively."""

    _sync_mode = "no"

    @pytest.mark.crash_recovery
    def test_sigkill_no_sync_restarts_cleanly(self):
        """Scenario 4: SIGKILL in no-sync mode — data loss is documented and expected.

        No fsync guarantee; server must still restart to a consistent ready state.
        """
        _bgsave_and_restart(self.server)

        self.client.execute_command("FLASH.SET", "nosync_key", "v")
        _sigkill(self.server)

        _crash_restart(self.server)
        assert self.client.execute_command("FLASH.DEBUG.STATE") == b"ready"
        # No assertion on record count — no-sync provides no durability guarantee.


# ── Scenarios 5–9: WAL file manipulation ─────────────────────────────────────


class TestFlashWalCorruption(_CrashTestBase):
    """Scenarios 5–9: inject WAL corruption, verify recovery truncates and continues."""

    _sync_mode = "always"  # always mode: records definitely reach disk before kill

    @pytest.mark.crash_recovery
    def test_torn_wal_frame_truncated_prior_records_applied(self):
        """Scenario 5: torn 4KiB write emulation — partial frame appended to WAL.

        Two complete PUT records precede the torn frame. Recovery must apply both,
        detect the partial payload (CRC can't be verified for incomplete data),
        truncate at the torn frame, and log a warning.
        """
        _bgsave_and_restart(self.server)  # clean cursor at P0

        self.client.execute_command("FLASH.SET", "k1", "v1")
        self.client.execute_command("FLASH.SET", "k2", "v2")
        _sigkill(self.server)

        wal = _wal_path(self._flash_dir)
        # Two complete PUT records: WAL should be HEADER_SIZE + 2*PUT_FRAME_SIZE
        expected_good_size = HEADER_SIZE + 2 * PUT_FRAME_SIZE

        # Append a torn frame: valid length header, truncated payload (5 of 10 bytes)
        with open(wal, "ab") as f:
            f.write(struct.pack("<II", 10, 0xDEADBEEF) + b"\xff" * 5)

        _crash_restart(self.server)
        assert self.client.execute_command("FLASH.DEBUG.STATE") == b"ready"
        assert _recovery_count(self.server) == 2
        self.server.verify_string_in_logfile("flash: recovery: WAL corruption at offset")
        # WAL truncated back to the two good records.
        assert wal.stat().st_size == expected_good_size

    @pytest.mark.crash_recovery
    def test_corrupt_wal_crc_truncates_at_corruption(self):
        """Scenario 6: CRC byte-flip mid-stream — recovery applies earlier records only.

        Three PUT records written; a byte in record 2's CRC is flipped, causing a
        CRC mismatch. Recovery applies record 1, truncates at record 2's offset, and
        logs a warning. Record 3 is also discarded (unreachable past the corruption).
        """
        _bgsave_and_restart(self.server)  # clean cursor

        self.client.execute_command("FLASH.SET", "k1", "v1")
        self.client.execute_command("FLASH.SET", "k2", "v2")
        self.client.execute_command("FLASH.SET", "k3", "v3")
        _sigkill(self.server)

        wal = _wal_path(self._flash_dir)
        # Record 2 CRC field starts at: HEADER_SIZE + PUT_FRAME_SIZE + 4
        # (frame overhead = 8: [u32 len][u32 crc]; CRC is bytes 4-7 of overhead)
        rec2_crc_offset = HEADER_SIZE + PUT_FRAME_SIZE + 4

        with open(wal, "r+b") as f:
            f.seek(rec2_crc_offset)
            original = f.read(1)
            f.seek(rec2_crc_offset)
            f.write(bytes([original[0] ^ 0xFF]))  # flip all bits → CRC mismatch

        _crash_restart(self.server)
        assert self.client.execute_command("FLASH.DEBUG.STATE") == b"ready"
        assert _recovery_count(self.server) == 1
        self.server.verify_string_in_logfile("flash: recovery: WAL corruption at offset")
        # WAL truncated at record 2's start.
        assert wal.stat().st_size == HEADER_SIZE + PUT_FRAME_SIZE

    @pytest.mark.crash_recovery
    def test_no_aux_replays_all_wal_records(self):
        """Scenario 7: WAL present, aux absent (RDB-free deployment or deleted RDB).

        Without an aux state the WAL cursor defaults to 0, so recovery replays
        every record in the file.
        """
        # Do NOT BGSAVE before kill — no aux state in the RDB.
        self.client.execute_command("FLASH.SET", "k1", "v1")
        self.client.execute_command("FLASH.SET", "k2", "v2")
        self.client.execute_command("FLASH.SET", "k3", "v3")
        _sigkill(self.server)

        # remove_rdb=True deletes the RDB → no aux on next load → cursor = 0.
        # Also: with no RDB, Valkey never logs "Done loading RDB", so we can't
        # wait for is_rdb_done_loading — restart() already blocks on the
        # server's "Ready to accept connections" log line.
        self.server.restart(remove_rdb=True, remove_nodes_conf=False, connect_client=True)
        assert self.server.is_alive()

        assert self.client.execute_command("FLASH.DEBUG.STATE") == b"ready"
        assert _recovery_count(self.server) == 3

    @pytest.mark.crash_recovery
    def test_wal_deleted_between_restarts_module_loads_ready(self):
        """Scenario 8: aux cursor present but WAL file deleted.

        Someone deleted the WAL between crashes. Recovery opens a fresh WAL
        (header only) and seeks past EOF — no records found. Module continues.
        """
        self.client.execute_command("FLASH.SET", "k1", "v1")
        _bgsave_and_restart(self.server)  # saves aux with cursor past k1's record

        _sigkill(self.server)

        # Delete the WAL — Wal::open recreates it with a fresh 16-byte header.
        wal = _wal_path(self._flash_dir)
        wal.unlink()

        _crash_restart(self.server)
        assert self.client.execute_command("FLASH.DEBUG.STATE") == b"ready"
        # Cursor from aux points past the (now absent) records → 0 applied.
        assert _recovery_count(self.server) == 0

    @pytest.mark.crash_recovery
    def test_corrupt_wal_magic_causes_module_load_failure(self):
        """Scenario 9: corrupt WAL magic — module refuses to load with a clear error.

        BadMagic is a fatal recovery error (unlike CRC corruption which is non-fatal).
        The server starts but the flash module fails to initialize; the log must
        contain 'flash: recovery failed' and 'bad magic'.
        """
        _sigkill(self.server)

        wal = _wal_path(self._flash_dir)
        with open(wal, "wb") as f:
            f.write(struct.pack("<IB", 0xDEAD_BEEF, WAL_VERSION) + b"\x00" * 11)

        # The module load failure causes Valkey to exit — start() will timeout.
        log_path = os.path.join(self.server.cwd, self.server.args["logfile"])
        with pytest.raises((RuntimeError, Exception)):
            self.server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)

        # Log written by the failed startup contains the recovery error.
        with open(log_path) as f:
            log_content = f.read()
        assert "flash: recovery failed" in log_content
        assert "bad magic" in log_content.lower()

        # Restore valid WAL so the test framework can tear down cleanly.
        with open(wal, "wb") as f:
            f.write(_fresh_wal_header())
        self.server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
        assert self.server.is_alive()
