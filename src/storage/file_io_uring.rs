use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::os::unix::io::AsRawFd;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use io_uring::{opcode, types, IoUring};
use libc::{c_void, fallocate, ftruncate64, posix_memalign};

use super::backend::{KvPair, StorageBackend, StorageError, StorageResult};

// ── Constants ─────────────────────────────────────────────────────────────────

const BLOCK_SIZE: usize = 4096;
const HEADER_BYTES: usize = 8; // u64 LE: value length prefix on each record

pub const DEFAULT_PATH: &str = "/tmp/valkey-flash.bin";
pub const DEFAULT_CAPACITY_BYTES: u64 = 1 << 30; // 1 GiB
pub const DEFAULT_IO_URING_ENTRIES: u32 = 256;

// ── AlignedBuf ────────────────────────────────────────────────────────────────

/// BLOCK_SIZE-aligned heap buffer allocated via posix_memalign for O_DIRECT.
struct AlignedBuf {
    ptr: *mut u8,
    size: usize,
}

impl AlignedBuf {
    fn new(size: usize) -> StorageResult<Self> {
        debug_assert!(size > 0, "AlignedBuf::new: size=0 is UB on some platforms");
        let mut ptr: *mut c_void = std::ptr::null_mut();
        // SAFETY: `ptr` is a valid out-param; BLOCK_SIZE is a power of two;
        // `size` > 0 (asserted above).  On success, ptr points to `size`
        // aligned bytes owned by this struct.
        let rc = unsafe { posix_memalign(&mut ptr, BLOCK_SIZE, size) };
        if rc != 0 {
            return Err(StorageError::Io(std::io::Error::from_raw_os_error(rc)));
        }
        // SAFETY: posix_memalign succeeded (rc == 0), so ptr is non-null,
        // properly aligned, and points to `size` writable bytes.
        unsafe { std::ptr::write_bytes(ptr as *mut u8, 0, size) };
        Ok(AlignedBuf {
            ptr: ptr as *mut u8,
            size,
        })
    }

    fn as_slice(&self) -> &[u8] {
        // SAFETY: ptr is a live allocation of exactly `self.size` bytes;
        // lifetime is tied to `&self` so the allocation cannot be freed.
        unsafe { std::slice::from_raw_parts(self.ptr, self.size) }
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: same as as_slice; `&mut self` ensures no aliasing.
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.size) }
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            // SAFETY: ptr was allocated by posix_memalign (libc-compatible
            // allocator); non-null check passed; Drop runs at most once.
            unsafe { libc::free(self.ptr as *mut c_void) };
        }
    }
}

// SAFETY: AlignedBuf is a plain owned heap allocation.  The pointer is never
// shared without synchronisation; the owning type gates all access via Mutex.
unsafe impl Send for AlignedBuf {}

// ── Index entry ───────────────────────────────────────────────────────────────

#[derive(Clone, Copy)]
struct IndexEntry {
    block_offset: u64, // first block index (not byte offset)
    value_len: u32,
    num_blocks: u32,
}

// ── FileIoUringBackend ────────────────────────────────────────────────────────

pub struct FileIoUringBackend {
    file: File,
    ring: Mutex<IoUring>,
    index: Mutex<HashMap<Vec<u8>, IndexEntry>>,
    next_block: AtomicU64,
    capacity_blocks: u64,
}

impl FileIoUringBackend {
    /// Open (or create) a backing file and return a ready backend.
    pub fn open(path: &str, capacity_bytes: u64, io_uring_entries: u32) -> StorageResult<Self> {
        let file = Self::open_backing_file(path)?;
        Self::preallocate(&file, capacity_bytes)?;

        let ring = IoUring::new(io_uring_entries).map_err(|e| {
            // ENOSYS means kernel < 5.1 — no io_uring support at all.
            StorageError::Other(format!(
                "io_uring unavailable (kernel too old or missing CONFIG_IO_URING): {e}"
            ))
        })?;

        let capacity_blocks = capacity_bytes / BLOCK_SIZE as u64;
        Ok(FileIoUringBackend {
            file,
            ring: Mutex::new(ring),
            index: Mutex::new(HashMap::new()),
            next_block: AtomicU64::new(0),
            capacity_blocks,
        })
    }

    /// Open the file with O_DIRECT; fall back to buffered I/O on EINVAL
    /// (e.g. tmpfs, some network filesystems) so tests on tmpfiles work.
    fn open_backing_file(path: &str) -> StorageResult<File> {
        use std::os::unix::fs::OpenOptionsExt;
        let result = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .custom_flags(libc::O_DIRECT)
            .open(path);
        match result {
            Ok(f) => Ok(f),
            Err(e) if e.raw_os_error() == Some(libc::EINVAL) => Ok(OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(path)?),
            Err(e) => Err(StorageError::Io(e)),
        }
    }

    /// Pre-allocate `capacity_bytes` in the file; fall back to ftruncate on
    /// EOPNOTSUPP / ENOSYS (tmpfs, older kernels without fallocate support).
    fn preallocate(file: &File, capacity_bytes: u64) -> StorageResult<()> {
        let fd = file.as_raw_fd();
        let rc = unsafe { fallocate(fd, 0, 0, capacity_bytes as libc::off_t) };
        if rc == 0 {
            return Ok(());
        }
        let errno = unsafe { *libc::__errno_location() };
        if errno == libc::EOPNOTSUPP || errno == libc::ENOSYS || errno == libc::EINVAL {
            let rc2 = unsafe { ftruncate64(fd, capacity_bytes as libc::off64_t) };
            if rc2 != 0 {
                return Err(StorageError::Io(std::io::Error::last_os_error()));
            }
            return Ok(());
        }
        Err(StorageError::Io(std::io::Error::from_raw_os_error(errno)))
    }

    pub fn blocks_needed(value_len: usize) -> u32 {
        let total = HEADER_BYTES + value_len;
        total.div_ceil(BLOCK_SIZE) as u32
    }

    fn alloc_blocks(&self, n: u32) -> StorageResult<u64> {
        let start = self.next_block.fetch_add(n as u64, Ordering::Relaxed);
        if start + n as u64 > self.capacity_blocks {
            return Err(StorageError::Other("storage capacity exhausted".into()));
        }
        Ok(start)
    }

    /// Submit one sqe and wait for its cqe; return `cqe.result()`.
    fn submit_and_await(ring: &mut IoUring, entry: io_uring::squeue::Entry) -> StorageResult<i32> {
        // SAFETY: the buffer referenced by `entry` must outlive this call.
        // All callers keep the AlignedBuf alive for the duration.
        unsafe { ring.submission().push(&entry) }
            .map_err(|e| StorageError::Other(format!("sqe push failed: {e}")))?;
        ring.submit_and_wait(1)
            .map_err(|e| StorageError::Other(format!("io_uring submit: {e}")))?;
        let cqe = ring
            .completion()
            .next()
            .ok_or_else(|| StorageError::Other("no cqe returned".into()))?;
        Ok(cqe.result())
    }

    fn do_read(&self, byte_offset: u64, buf: &mut AlignedBuf) -> StorageResult<()> {
        let fd = types::Fd(self.file.as_raw_fd());
        let entry = opcode::Read::new(fd, buf.ptr, buf.size as u32)
            .offset(byte_offset)
            .build()
            .user_data(0);
        let mut ring = self
            .ring
            .lock()
            .map_err(|e| StorageError::Other(format!("ring lock poisoned: {e}")))?;
        let result = Self::submit_and_await(&mut ring, entry)?;
        if result < 0 {
            return Err(StorageError::Io(std::io::Error::from_raw_os_error(-result)));
        }
        Ok(())
    }

    fn do_write(&self, byte_offset: u64, buf: &AlignedBuf) -> StorageResult<()> {
        let fd = types::Fd(self.file.as_raw_fd());
        // SAFETY: buf lives for the duration of submit_and_await.
        let entry = opcode::Write::new(fd, buf.ptr, buf.size as u32)
            .offset(byte_offset)
            .build()
            .user_data(0);
        let mut ring = self
            .ring
            .lock()
            .map_err(|e| StorageError::Other(format!("ring lock poisoned: {e}")))?;
        let result = Self::submit_and_await(&mut ring, entry)?;
        if result < 0 {
            return Err(StorageError::Io(std::io::Error::from_raw_os_error(-result)));
        }
        Ok(())
    }

    /// Encode and write `value` at block `block_offset`.
    fn write_value_at(&self, block_offset: u64, value: &[u8]) -> StorageResult<()> {
        let n_blocks = Self::blocks_needed(value.len()) as usize;
        let buf_size = n_blocks * BLOCK_SIZE;
        let mut buf = AlignedBuf::new(buf_size)?;
        let slice = buf.as_mut_slice();
        // Header: 8-byte LE value length.
        slice[..8].copy_from_slice(&(value.len() as u64).to_le_bytes());
        slice[8..8 + value.len()].copy_from_slice(value);
        let byte_offset = block_offset * BLOCK_SIZE as u64;
        self.do_write(byte_offset, &buf)
    }

    /// Read value stored at `entry`.
    fn read_value_at(&self, entry: &IndexEntry) -> StorageResult<Vec<u8>> {
        let buf_size = entry.num_blocks as usize * BLOCK_SIZE;
        let mut buf = AlignedBuf::new(buf_size)?;
        let byte_offset = entry.block_offset * BLOCK_SIZE as u64;
        self.do_read(byte_offset, &mut buf)?;
        let slice = buf.as_slice();
        let len_bytes: [u8; 8] = slice[..8]
            .try_into()
            .map_err(|_| StorageError::Other("corrupt record: header too short".into()))?;
        let stored_len = u64::from_le_bytes(len_bytes) as usize;
        if stored_len != entry.value_len as usize {
            return Err(StorageError::Other(format!(
                "header len {stored_len} != index len {}",
                entry.value_len
            )));
        }
        Ok(slice[8..8 + stored_len].to_vec())
    }
}

impl StorageBackend for FileIoUringBackend {
    fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        let entry = {
            let index = self
                .index
                .lock()
                .map_err(|e| StorageError::Other(format!("index lock poisoned: {e}")))?;
            index.get(key).copied()
        };
        match entry {
            None => Ok(None),
            Some(e) => self.read_value_at(&e).map(Some),
        }
    }

    fn put(&self, key: &[u8], value: &[u8]) -> StorageResult<()> {
        let n_blocks = Self::blocks_needed(value.len());
        let block_offset = self.alloc_blocks(n_blocks)?;
        self.write_value_at(block_offset, value)?;
        let entry = IndexEntry {
            block_offset,
            value_len: value.len() as u32,
            num_blocks: n_blocks,
        };
        let mut index = self
            .index
            .lock()
            .map_err(|e| StorageError::Other(format!("index lock poisoned: {e}")))?;
        index.insert(key.to_vec(), entry);
        Ok(())
    }

    fn delete(&self, key: &[u8]) -> StorageResult<()> {
        let mut index = self
            .index
            .lock()
            .map_err(|e| StorageError::Other(format!("index lock poisoned: {e}")))?;
        index.remove(key);
        // Blocks are leaked until compaction (task #50).
        Ok(())
    }

    fn iter(&self) -> Box<dyn Iterator<Item = StorageResult<KvPair>> + '_> {
        // Snapshot the index under lock; reads happen outside the lock.
        let snapshot = match self.index.lock() {
            Err(e) => {
                let err = StorageError::Other(format!("index lock poisoned: {e}"));
                return Box::new(std::iter::once(Err(err)));
            }
            Ok(guard) => guard
                .iter()
                .map(|(k, v)| (k.clone(), *v))
                .collect::<Vec<_>>(),
        };
        let results: Vec<StorageResult<KvPair>> = snapshot
            .into_iter()
            .map(|(key, entry)| self.read_value_at(&entry).map(|val| (key, val)))
            .collect();
        Box::new(results.into_iter())
    }

    fn flush(&self) -> StorageResult<()> {
        self.file.sync_data().map_err(StorageError::Io)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    fn open_backend() -> (FileIoUringBackend, NamedTempFile) {
        let tmp = NamedTempFile::new().expect("tempfile");
        let path = tmp.path().to_str().unwrap().to_owned();
        let backend = FileIoUringBackend::open(&path, 4 * 1024 * 1024, 32).expect("open_backend");
        (backend, tmp)
    }

    #[test]
    fn open_succeeds() {
        open_backend();
    }

    #[test]
    fn get_missing_returns_none() {
        let (b, _tmp) = open_backend();
        assert!(b.get(b"absent").unwrap().is_none());
    }

    #[test]
    fn put_then_get_roundtrip() {
        let (b, _tmp) = open_backend();
        b.put(b"key", b"value").unwrap();
        assert_eq!(b.get(b"key").unwrap(), Some(b"value".to_vec()));
    }

    #[test]
    fn put_overwrite_returns_new_value() {
        let (b, _tmp) = open_backend();
        b.put(b"k", b"v1").unwrap();
        b.put(b"k", b"v2").unwrap();
        assert_eq!(b.get(b"k").unwrap(), Some(b"v2".to_vec()));
    }

    #[test]
    fn delete_removes_key() {
        let (b, _tmp) = open_backend();
        b.put(b"k", b"v").unwrap();
        b.delete(b"k").unwrap();
        assert!(b.get(b"k").unwrap().is_none());
    }

    #[test]
    fn delete_missing_is_noop() {
        let (b, _tmp) = open_backend();
        b.delete(b"nope").unwrap();
    }

    #[test]
    fn iter_empty() {
        let (b, _tmp) = open_backend();
        assert_eq!(b.iter().count(), 0);
    }

    #[test]
    fn iter_populated() {
        let (b, _tmp) = open_backend();
        b.put(b"a", b"1").unwrap();
        b.put(b"b", b"2").unwrap();
        b.put(b"c", b"3").unwrap();
        let mut pairs: Vec<KvPair> = b.iter().map(|r| r.expect("iter item")).collect();
        pairs.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            pairs,
            vec![
                (b"a".to_vec(), b"1".to_vec()),
                (b"b".to_vec(), b"2".to_vec()),
                (b"c".to_vec(), b"3".to_vec()),
            ]
        );
    }

    #[test]
    fn flush_succeeds_and_data_intact() {
        let (b, _tmp) = open_backend();
        b.put(b"k", b"v").unwrap();
        b.flush().unwrap();
        assert_eq!(b.get(b"k").unwrap(), Some(b"v".to_vec()));
    }

    #[test]
    fn large_value_multi_block() {
        let (b, _tmp) = open_backend();
        let big: Vec<u8> = (0u8..=255).cycle().take(12_000).collect();
        b.put(b"big", &big).unwrap();
        assert_eq!(b.get(b"big").unwrap(), Some(big));
    }

    #[test]
    fn empty_value_roundtrip() {
        let (b, _tmp) = open_backend();
        b.put(b"empty", b"").unwrap();
        assert_eq!(b.get(b"empty").unwrap(), Some(b"".to_vec()));
    }

    #[test]
    fn multiple_keys_independent() {
        let (b, _tmp) = open_backend();
        b.put(b"x", b"hello").unwrap();
        b.put(b"y", b"world").unwrap();
        assert_eq!(b.get(b"x").unwrap(), Some(b"hello".to_vec()));
        assert_eq!(b.get(b"y").unwrap(), Some(b"world".to_vec()));
    }

    #[test]
    fn blocks_needed_unit() {
        assert_eq!(FileIoUringBackend::blocks_needed(0), 1); // header only → 8 bytes → 1 block
        assert_eq!(FileIoUringBackend::blocks_needed(4088), 1); // 8+4088=4096 → 1 block
        assert_eq!(FileIoUringBackend::blocks_needed(4089), 2); // 8+4089=4097 → 2 blocks
        assert_eq!(FileIoUringBackend::blocks_needed(8184), 2); // 8+8184=8192 → exactly 2 blocks
        assert_eq!(FileIoUringBackend::blocks_needed(8185), 3); // 8+8185=8193 → 3 blocks
    }

    #[test]
    #[ignore]
    fn bench_4k_read() {
        let (b, _tmp) = open_backend();
        let payload: Vec<u8> = vec![0xAB; 4000];
        b.put(b"bench", &payload).unwrap();
        let n = 10_000u32;
        let start = std::time::Instant::now();
        for _ in 0..n {
            let _ = b.get(b"bench").unwrap();
        }
        let elapsed = start.elapsed();
        println!(
            "bench_4k_read: {n} reads in {:?} ({:.0} µs/op)",
            elapsed,
            elapsed.as_micros() as f64 / n as f64
        );
    }
}
