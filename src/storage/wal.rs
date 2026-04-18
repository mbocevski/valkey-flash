use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

// ── Error type ────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum WalError {
    Io(std::io::Error),
    BadMagic,
    BadVersion(u8),
    Corruption { offset: u64, reason: &'static str },
}

impl std::fmt::Display for WalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WalError::Io(e) => write!(f, "WAL I/O error: {e}"),
            WalError::BadMagic => write!(f, "WAL bad magic number"),
            WalError::BadVersion(v) => write!(f, "WAL unsupported version: {v}"),
            WalError::Corruption { offset, reason } => {
                write!(f, "WAL corruption at offset {offset}: {reason}")
            }
        }
    }
}

impl std::error::Error for WalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        if let WalError::Io(e) = self {
            Some(e)
        } else {
            None
        }
    }
}

impl From<std::io::Error> for WalError {
    fn from(e: std::io::Error) -> Self {
        WalError::Io(e)
    }
}

pub type WalResult<T> = Result<T, WalError>;

// ── WAL header ────────────────────────────────────────────────────────────────

// "FLSW" in little-endian
const WAL_MAGIC: u32 = 0x5753_4C46;
const WAL_VERSION: u8 = 1;
// 16 bytes total: [u32 magic][u8 version][11 bytes padding]
const HEADER_SIZE: usize = 16;

// ── Record framing ────────────────────────────────────────────────────────────

// Each record: [u32 LE payload_len][u32 LE CRC32C over payload][payload]
const FRAME_OVERHEAD: usize = 8; // 4 + 4

// ── Op codes ──────────────────────────────────────────────────────────────────

const OP_VER: u8 = 1;
const OP_PUT: u8 = 0x01;
const OP_DELETE: u8 = 0x02;
const OP_CHECKPOINT: u8 = 0x03;

// ── WalOp ─────────────────────────────────────────────────────────────────────

#[derive(Debug, PartialEq, Clone)]
pub enum WalOp {
    /// A key was written to NVMe.
    Put {
        key_hash: u64,
        offset: u64,
        value_hash: u64,
    },
    /// A key was deleted from NVMe.
    Delete { key_hash: u64 },
    /// Durable checkpoint marker: all ops before `at` are stable.
    Checkpoint { at: u64 },
}

impl WalOp {
    fn encode(&self) -> Vec<u8> {
        match self {
            WalOp::Put {
                key_hash,
                offset,
                value_hash,
            } => {
                let mut v = vec![OP_VER, OP_PUT];
                v.extend_from_slice(&key_hash.to_le_bytes());
                v.extend_from_slice(&offset.to_le_bytes());
                v.extend_from_slice(&value_hash.to_le_bytes());
                v
            }
            WalOp::Delete { key_hash } => {
                let mut v = vec![OP_VER, OP_DELETE];
                v.extend_from_slice(&key_hash.to_le_bytes());
                v
            }
            WalOp::Checkpoint { at } => {
                let mut v = vec![OP_VER, OP_CHECKPOINT];
                v.extend_from_slice(&at.to_le_bytes());
                v
            }
        }
    }

    fn decode(payload: &[u8]) -> WalResult<Self> {
        if payload.len() < 2 {
            return Err(WalError::Corruption {
                offset: 0,
                reason: "payload too short for op header",
            });
        }
        // op_ver at payload[0] — reserved for future schema evolution
        let op_code = payload[1];
        let body = &payload[2..];
        match op_code {
            OP_PUT => {
                if body.len() < 24 {
                    return Err(WalError::Corruption {
                        offset: 0,
                        reason: "Put payload too short",
                    });
                }
                let key_hash = u64::from_le_bytes(body[0..8].try_into().map_err(|_| {
                    WalError::Corruption {
                        offset: 0,
                        reason: "Put key_hash slice",
                    }
                })?);
                let offset = u64::from_le_bytes(body[8..16].try_into().map_err(|_| {
                    WalError::Corruption {
                        offset: 0,
                        reason: "Put offset slice",
                    }
                })?);
                let value_hash = u64::from_le_bytes(body[16..24].try_into().map_err(|_| {
                    WalError::Corruption {
                        offset: 0,
                        reason: "Put value_hash slice",
                    }
                })?);
                Ok(WalOp::Put {
                    key_hash,
                    offset,
                    value_hash,
                })
            }
            OP_DELETE => {
                if body.len() < 8 {
                    return Err(WalError::Corruption {
                        offset: 0,
                        reason: "Delete payload too short",
                    });
                }
                let key_hash = u64::from_le_bytes(body[0..8].try_into().map_err(|_| {
                    WalError::Corruption {
                        offset: 0,
                        reason: "Delete key_hash slice",
                    }
                })?);
                Ok(WalOp::Delete { key_hash })
            }
            OP_CHECKPOINT => {
                if body.len() < 8 {
                    return Err(WalError::Corruption {
                        offset: 0,
                        reason: "Checkpoint payload too short",
                    });
                }
                let at = u64::from_le_bytes(body[0..8].try_into().map_err(|_| {
                    WalError::Corruption {
                        offset: 0,
                        reason: "Checkpoint at slice",
                    }
                })?);
                Ok(WalOp::Checkpoint { at })
            }
            _other => Err(WalError::Corruption {
                offset: 0,
                reason: "unknown op_code",
            }),
        }
    }
}

// ── Sync mode ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum WalSyncMode {
    Always,
    Everysec,
    No,
}

// ── Background everysec flusher ───────────────────────────────────────────────

fn spawn_everysec_flusher(
    inner: Arc<Mutex<WalInner>>,
    shutdown: Arc<AtomicBool>,
    signal: Arc<(Mutex<()>, Condvar)>,
) -> WalResult<thread::JoinHandle<()>> {
    thread::Builder::new()
        .name("wal-flusher".into())
        .spawn(move || loop {
            let (mu, cvar) = &*signal;
            let guard = mu.lock().unwrap_or_else(|e| e.into_inner());
            // Sleep up to 1 s; wakes immediately when shutdown is signalled.
            let _ = cvar.wait_timeout_while(guard, Duration::from_secs(1), |_| {
                !shutdown.load(Ordering::Relaxed)
            });
            if shutdown.load(Ordering::Relaxed) {
                // No final sync on exit: Everysec guarantees at-most-1s lag,
                // not a flush-before-close guarantee. Caller must use Always
                // mode if close-time durability is required.
                break;
            }
            let guard = match inner.lock() {
                Ok(g) => g,
                Err(_) => break,
            };
            let _ = guard.file.sync_data();
        })
        .map_err(WalError::Io)
}

// ── Inner state (held under Mutex) ────────────────────────────────────────────

struct WalInner {
    file: File,
}

// ── Wal ───────────────────────────────────────────────────────────────────────

/// `JoinHandle` doesn't implement `Debug`, so we implement it manually.
impl std::fmt::Debug for Wal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Wal")
            .field("path", &self.path)
            .field("sync_mode", &self.sync_mode)
            .finish_non_exhaustive()
    }
}

pub struct Wal {
    inner: Arc<Mutex<WalInner>>,
    sync_mode: WalSyncMode,
    path: PathBuf,
    shutdown: Arc<AtomicBool>,
    flusher_signal: Arc<(Mutex<()>, Condvar)>,
    flusher: Option<thread::JoinHandle<()>>,
}

impl Wal {
    /// Open (or create) a WAL file at `path` with the given sync mode.
    pub fn open(path: impl AsRef<Path>, sync_mode: WalSyncMode) -> WalResult<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;

        let metadata = file.metadata()?;
        if metadata.len() == 0 {
            // New file — write header.
            let mut f = &file;
            let mut header = [0u8; HEADER_SIZE];
            header[0..4].copy_from_slice(&WAL_MAGIC.to_le_bytes());
            header[4] = WAL_VERSION;
            // bytes 5–15 remain zero (padding)
            f.write_all(&header)?;
            f.sync_data()?;
        } else {
            // Existing file — validate header.
            let mut f = &file;
            f.seek(SeekFrom::Start(0))?;
            let mut header = [0u8; HEADER_SIZE];
            f.read_exact(&mut header)?;
            let magic = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
            if magic != WAL_MAGIC {
                return Err(WalError::BadMagic);
            }
            let ver = header[4];
            if ver != WAL_VERSION {
                return Err(WalError::BadVersion(ver));
            }
        }

        // Seek to end for appends.
        {
            let mut f = &file;
            f.seek(SeekFrom::End(0))?;
        }

        let inner = Arc::new(Mutex::new(WalInner { file }));
        let shutdown = Arc::new(AtomicBool::new(false));
        let flusher_signal = Arc::new((Mutex::new(()), Condvar::new()));

        let flusher = if sync_mode == WalSyncMode::Everysec {
            Some(spawn_everysec_flusher(
                Arc::clone(&inner),
                Arc::clone(&shutdown),
                Arc::clone(&flusher_signal),
            )?)
        } else {
            None
        };

        Ok(Wal {
            inner,
            sync_mode,
            path,
            shutdown,
            flusher_signal,
            flusher,
        })
    }

    /// Append a `WalOp` record, applying the configured sync policy.
    pub fn append(&self, op: WalOp) -> WalResult<()> {
        let payload = op.encode();
        let len = payload.len() as u32;
        let crc = crc32c::crc32c(&payload);

        let mut guard = self
            .inner
            .lock()
            .map_err(|_| WalError::Io(std::io::Error::other("WAL mutex poisoned")))?;

        guard.file.write_all(&len.to_le_bytes())?;
        guard.file.write_all(&crc.to_le_bytes())?;
        guard.file.write_all(&payload)?;

        match self.sync_mode {
            WalSyncMode::Always => guard.file.sync_data()?,
            WalSyncMode::Everysec | WalSyncMode::No => guard.file.flush()?,
        }

        Ok(())
    }

    /// Iterate over WAL records starting from the record area (after the header).
    /// On CRC failure or torn tail the iterator yields an error and stops.
    pub fn iter_records(&self) -> WalResult<RecordIter> {
        self.iter_records_from(HEADER_SIZE as u64)
    }

    /// Iterate over WAL records starting from `offset` (clamped to the record
    /// area start). Used by recovery to skip records already applied via RDB.
    pub fn iter_records_from(&self, offset: u64) -> WalResult<RecordIter> {
        let path = self.path.clone();
        let mut file = OpenOptions::new().read(true).open(&path)?;
        let start = offset.max(HEADER_SIZE as u64);
        file.seek(SeekFrom::Start(start))?;
        Ok(RecordIter {
            file,
            offset: start,
            done: false,
        })
    }

    /// Return the current write position (bytes from file start).
    /// Used by `aux_save` to snapshot the WAL cursor before an RDB save.
    pub fn current_offset(&self) -> WalResult<u64> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| WalError::Io(std::io::Error::other("WAL mutex poisoned")))?;
        Ok(guard.file.stream_position()?)
    }

    /// Truncate the WAL file to `offset` bytes and seek to the new end.
    /// Used by checkpoint / compaction.
    pub fn truncate_at(&self, offset: u64) -> WalResult<()> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| WalError::Io(std::io::Error::other("WAL mutex poisoned")))?;
        guard.file.set_len(offset)?;
        guard.file.seek(SeekFrom::Start(offset))?;
        Ok(())
    }
}

impl Drop for Wal {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        // Lock briefly then notify: ensures the flusher either (a) sees the flag
        // on its next condition check, or (b) wakes from wait_timeout_while early.
        {
            let (_g, cvar) = &*self.flusher_signal;
            drop(_g.lock().unwrap_or_else(|e| e.into_inner()));
            cvar.notify_all();
        }
        if let Some(handle) = self.flusher.take() {
            handle.join().unwrap_or(());
        }
    }
}

// ── Record iterator ───────────────────────────────────────────────────────────

pub struct RecordIter {
    file: File,
    offset: u64,
    done: bool,
}

impl Iterator for RecordIter {
    type Item = WalResult<WalOp>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        // Read frame header: [u32 len][u32 crc]
        let mut frame_hdr = [0u8; FRAME_OVERHEAD];
        match self.file.read_exact(&mut frame_hdr) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // Clean EOF — iteration complete.
                return None;
            }
            Err(e) => {
                self.done = true;
                return Some(Err(WalError::Io(e)));
            }
        }

        let payload_len =
            u32::from_le_bytes([frame_hdr[0], frame_hdr[1], frame_hdr[2], frame_hdr[3]]) as usize;
        let stored_crc =
            u32::from_le_bytes([frame_hdr[4], frame_hdr[5], frame_hdr[6], frame_hdr[7]]);

        // Guard against OOM from a fuzz-crafted or corrupted length field.
        // Largest valid WAL payload (Put) is 26 bytes; 4 KiB is a safe ceiling.
        const MAX_PAYLOAD: usize = 4096;
        if payload_len > MAX_PAYLOAD {
            self.done = true;
            return Some(Err(WalError::Corruption {
                offset: self.offset,
                reason: "payload_len exceeds maximum (possible corruption or truncated write)",
            }));
        }

        let mut payload = vec![0u8; payload_len];
        match self.file.read_exact(&mut payload) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // Torn tail — partial payload write; stop here.
                self.done = true;
                return Some(Err(WalError::Corruption {
                    offset: self.offset,
                    reason: "torn tail: partial payload",
                }));
            }
            Err(e) => {
                self.done = true;
                return Some(Err(WalError::Io(e)));
            }
        }

        let computed_crc = crc32c::crc32c(&payload);
        if computed_crc != stored_crc {
            self.done = true;
            return Some(Err(WalError::Corruption {
                offset: self.offset,
                reason: "CRC32C mismatch",
            }));
        }

        self.offset += (FRAME_OVERHEAD + payload_len) as u64;

        Some(WalOp::decode(&payload))
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn put_op(n: u64) -> WalOp {
        WalOp::Put {
            key_hash: n,
            offset: n * 4096,
            value_hash: n ^ 0xDEAD,
        }
    }

    fn delete_op(n: u64) -> WalOp {
        WalOp::Delete { key_hash: n }
    }

    fn checkpoint_op(at: u64) -> WalOp {
        WalOp::Checkpoint { at }
    }

    // ── Round-trip ────────────────────────────────────────────────────────────

    #[test]
    fn roundtrip_100_records() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");

        {
            let wal = Wal::open(&path, WalSyncMode::No).unwrap();
            for i in 0..100u64 {
                wal.append(put_op(i)).unwrap();
            }
        }

        let wal = Wal::open(&path, WalSyncMode::No).unwrap();
        let records: Vec<_> = wal
            .iter_records()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(records.len(), 100);
        for (i, rec) in records.iter().enumerate() {
            assert_eq!(*rec, put_op(i as u64));
        }
    }

    #[test]
    fn roundtrip_mixed_op_types() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("mixed.wal");
        let ops = vec![put_op(1), delete_op(2), checkpoint_op(42), put_op(3)];

        {
            let wal = Wal::open(&path, WalSyncMode::No).unwrap();
            for op in &ops {
                wal.append(op.clone()).unwrap();
            }
        }

        let wal = Wal::open(&path, WalSyncMode::No).unwrap();
        let restored: Vec<_> = wal
            .iter_records()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(restored, ops);
    }

    // ── CRC corruption ────────────────────────────────────────────────────────

    #[test]
    fn crc_corruption_detected() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("corrupt.wal");

        {
            let wal = Wal::open(&path, WalSyncMode::No).unwrap();
            wal.append(put_op(1)).unwrap();
            wal.append(put_op(2)).unwrap();
        }

        // Corrupt one byte inside the first record's payload area.
        // Layout: [16 header][4 len][4 crc][payload...] — byte 24 is payload[0]
        {
            use std::os::unix::fs::FileExt;
            let f = OpenOptions::new().write(true).open(&path).unwrap();
            f.write_at(&[0xFF], 24).unwrap();
        }

        let wal = Wal::open(&path, WalSyncMode::No).unwrap();
        let mut iter = wal.iter_records().unwrap();
        let first = iter.next().unwrap();
        assert!(
            matches!(first, Err(WalError::Corruption { .. })),
            "expected corruption error, got {first:?}"
        );
        // Iterator must stop after corruption — no subsequent records.
        assert!(iter.next().is_none());
    }

    // ── Torn tail ─────────────────────────────────────────────────────────────

    #[test]
    fn torn_tail_treated_as_end() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("torn.wal");

        {
            let wal = Wal::open(&path, WalSyncMode::No).unwrap();
            wal.append(put_op(1)).unwrap();
            wal.append(put_op(2)).unwrap();
        }

        // Truncate the file by 1 byte — creates a partial record at the tail.
        let len = std::fs::metadata(&path).unwrap().len();
        {
            let f = OpenOptions::new().write(true).open(&path).unwrap();
            f.set_len(len - 1).unwrap();
        }

        let wal = Wal::open(&path, WalSyncMode::No).unwrap();
        let mut iter = wal.iter_records().unwrap();
        // First record is intact.
        assert_eq!(iter.next().unwrap().unwrap(), put_op(1));
        // Second record is torn — must error.
        let second = iter.next().unwrap();
        assert!(
            matches!(second, Err(WalError::Corruption { .. })),
            "expected torn-tail error, got {second:?}"
        );
        assert!(iter.next().is_none());
    }

    // ── Sync modes ────────────────────────────────────────────────────────────

    #[test]
    fn everysec_flusher_joins_promptly_on_drop() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("drop_join.wal");
        let wal = Wal::open(&path, WalSyncMode::Everysec).unwrap();
        wal.append(put_op(1)).unwrap();

        let start = std::time::Instant::now();
        drop(wal);
        let elapsed = start.elapsed();

        assert!(
            elapsed < std::time::Duration::from_millis(100),
            "Wal::drop took {elapsed:?}, expected < 100 ms (flusher thread not joined promptly)"
        );
    }

    #[test]
    fn sync_mode_always_completes_without_error() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("always.wal");
        let wal = Wal::open(&path, WalSyncMode::Always).unwrap();
        wal.append(put_op(1)).unwrap();
        wal.append(put_op(2)).unwrap();
    }

    #[test]
    fn sync_mode_everysec_flusher_advances() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("everysec.wal");
        let wal = Wal::open(&path, WalSyncMode::Everysec).unwrap();
        wal.append(put_op(1)).unwrap();
        // Let the background flusher tick at least once.
        std::thread::sleep(Duration::from_millis(1200));
        // Data must be readable after flusher ran.
        let records: Vec<_> = wal
            .iter_records()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0], put_op(1));
    }

    #[test]
    fn sync_mode_no_completes_without_error() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("no.wal");
        let wal = Wal::open(&path, WalSyncMode::No).unwrap();
        for i in 0..50u64 {
            wal.append(put_op(i)).unwrap();
        }
    }

    // ── Header validation ─────────────────────────────────────────────────────

    #[test]
    fn bad_magic_refused() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("bad_magic.wal");

        // Write a file with wrong magic.
        {
            let mut f = File::create(&path).unwrap();
            f.write_all(&[
                0xDE,
                0xAD,
                0xBE,
                0xEF,
                WAL_VERSION,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
            ])
            .unwrap();
        }

        let result = Wal::open(&path, WalSyncMode::No);
        assert!(
            matches!(result, Err(WalError::BadMagic)),
            "expected BadMagic, got {result:?}"
        );
    }

    #[test]
    fn bad_version_refused() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("bad_ver.wal");

        {
            let mut f = File::create(&path).unwrap();
            // Correct magic, version = 99
            let mut header = [0u8; HEADER_SIZE];
            header[0..4].copy_from_slice(&WAL_MAGIC.to_le_bytes());
            header[4] = 99;
            f.write_all(&header).unwrap();
        }

        let result = Wal::open(&path, WalSyncMode::No);
        assert!(
            matches!(result, Err(WalError::BadVersion(99))),
            "expected BadVersion(99), got {result:?}"
        );
    }

    #[test]
    fn empty_file_gets_header_written() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("new.wal");
        let _wal = Wal::open(&path, WalSyncMode::No).unwrap();
        let meta = std::fs::metadata(&path).unwrap();
        assert_eq!(meta.len(), HEADER_SIZE as u64);
    }

    // ── truncate_at ───────────────────────────────────────────────────────────

    #[test]
    fn truncate_at_reduces_file_size() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("trunc.wal");
        let wal = Wal::open(&path, WalSyncMode::No).unwrap();
        wal.append(put_op(1)).unwrap();
        wal.append(put_op(2)).unwrap();
        let before = std::fs::metadata(&path).unwrap().len();
        // Truncate back to just the header — all records removed.
        wal.truncate_at(HEADER_SIZE as u64).unwrap();
        let after = std::fs::metadata(&path).unwrap().len();
        assert_eq!(after, HEADER_SIZE as u64);
        assert!(after < before);
    }

    #[test]
    fn truncate_then_append_works() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("trunc_append.wal");
        let wal = Wal::open(&path, WalSyncMode::No).unwrap();
        wal.append(put_op(1)).unwrap();
        // truncate_at seeks to the new end, so the next append writes there.
        wal.truncate_at(HEADER_SIZE as u64).unwrap();
        wal.append(put_op(99)).unwrap();
        let records: Vec<_> = wal
            .iter_records()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(records, vec![put_op(99)]);
    }

    // ── Thread safety ─────────────────────────────────────────────────────────

    #[test]
    fn concurrent_appends_are_safe() {
        use std::sync::Arc;

        let dir = tempdir().unwrap();
        let path = dir.path().join("concurrent.wal");
        let wal = Arc::new(Wal::open(&path, WalSyncMode::No).unwrap());

        let mut handles = Vec::new();
        for t in 0..8u64 {
            let wal = Arc::clone(&wal);
            handles.push(thread::spawn(move || {
                for i in 0..25u64 {
                    wal.append(put_op(t * 100 + i)).unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        // All 200 records must be readable and valid (no corrupted frames).
        let records: Vec<_> = wal
            .iter_records()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(records.len(), 200);
    }

    // ── iter_records_from ─────────────────────────────────────────────────────

    #[test]
    fn iter_from_zero_equals_iter_records() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("from_zero.wal");
        let wal = Wal::open(&path, WalSyncMode::No).unwrap();
        for i in 0..5u64 {
            wal.append(put_op(i)).unwrap();
        }
        let all: Vec<_> = wal
            .iter_records()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let from_zero: Vec<_> = wal
            .iter_records_from(0)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(all, from_zero);
    }

    #[test]
    fn iter_from_mid_skips_earlier_records() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("from_mid.wal");
        let wal = Wal::open(&path, WalSyncMode::No).unwrap();
        wal.append(put_op(1)).unwrap();
        wal.append(put_op(2)).unwrap();
        // Capture position after first record.
        let cursor = wal.current_offset().unwrap();
        wal.append(put_op(3)).unwrap();
        wal.append(put_op(4)).unwrap();

        let records: Vec<_> = wal
            .iter_records_from(cursor)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(records, vec![put_op(3), put_op(4)]);
    }

    #[test]
    fn current_offset_advances_after_appends() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("offset.wal");
        let wal = Wal::open(&path, WalSyncMode::No).unwrap();
        let before = wal.current_offset().unwrap();
        wal.append(put_op(1)).unwrap();
        let after = wal.current_offset().unwrap();
        assert!(after > before);
    }

    // ── Op encoding round-trips ───────────────────────────────────────────────

    #[test]
    fn op_put_encode_decode_roundtrip() {
        let op = put_op(42);
        let encoded = op.encode();
        assert_eq!(WalOp::decode(&encoded).unwrap(), op);
    }

    #[test]
    fn op_delete_encode_decode_roundtrip() {
        let op = delete_op(7);
        let encoded = op.encode();
        assert_eq!(WalOp::decode(&encoded).unwrap(), op);
    }

    #[test]
    fn op_checkpoint_encode_decode_roundtrip() {
        let op = checkpoint_op(1024);
        let encoded = op.encode();
        assert_eq!(WalOp::decode(&encoded).unwrap(), op);
    }
}
