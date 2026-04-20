use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::os::unix::io::AsRawFd;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use io_uring::{IoUring, opcode, types};
use libc::{c_void, fallocate, ftruncate64, posix_memalign};

use super::BlockRange;
use super::backend::{KvPair, StorageBackend, StorageError, StorageResult};

// ── Compaction metrics ────────────────────────────────────────────────────────

/// Total number of compaction ticks that have run since module load.
pub static COMPACTION_RUNS: AtomicU64 = AtomicU64::new(0);

/// Cumulative NVMe bytes returned to the free-list (delete + overwrite).
pub static BYTES_RECLAIMED: AtomicU64 = AtomicU64::new(0);

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
    free_list: Mutex<Vec<BlockRange>>,
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
            free_list: Mutex::new(Vec::new()),
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
        // First-fit from free-list: reuse previously freed blocks before bumping.
        {
            let mut fl = self
                .free_list
                .lock()
                .map_err(|e| StorageError::Other(format!("free_list lock poisoned: {e}")))?;
            if let Some(pos) = fl.iter().position(|r| r.len >= n) {
                let range = fl[pos];
                if range.len == n {
                    fl.swap_remove(pos);
                } else {
                    fl[pos].start += n as u64;
                    fl[pos].len -= n;
                }
                return Ok(range.start);
            }
        }
        // Fall back to bump allocator for fresh blocks.
        let start = self.next_block.fetch_add(n as u64, Ordering::Relaxed);
        if start + n as u64 > self.capacity_blocks {
            return Err(StorageError::Other("storage capacity exhausted".into()));
        }
        Ok(start)
    }

    fn push_free_range(&self, start: u64, len: u32) {
        if let Ok(mut fl) = self.free_list.lock() {
            fl.push(BlockRange { start, len });
        }
    }

    // ── Free-list management (public for compaction command + aux persistence) ──

    /// Run one compaction tick: coalesce adjacent/overlapping free ranges.
    ///
    /// Sorts and merges the range vec *outside* the `free_list` mutex so
    /// `alloc_blocks` / `release_cold_blocks` on the main thread don't block
    /// on the O(N log N) sort. The vec is swapped out under the lock (cheap —
    /// just a pointer move), coalesced on the compaction worker, then merged
    /// back with any ranges released during the unlock window.
    pub fn run_compaction_tick(&self) {
        let mut ranges = match self.free_list.lock() {
            Ok(mut fl) => std::mem::take(&mut *fl),
            Err(_) => return,
        };
        coalesce_ranges(&mut ranges);
        if let Ok(mut fl) = self.free_list.lock() {
            // `fl` may contain ranges released while the compaction worker was
            // sorting. Merge them in and re-coalesce — cheap because the
            // unlock window was short, so |fl| is typically small.
            ranges.append(&mut fl);
            coalesce_ranges(&mut ranges);
            *fl = ranges;
        }
        COMPACTION_RUNS.fetch_add(1, Ordering::Relaxed);
    }

    /// Snapshot the free-list for aux persistence.
    pub fn free_list_snapshot(&self) -> Vec<BlockRange> {
        self.free_list
            .lock()
            .map(|fl| fl.clone())
            .unwrap_or_default()
    }

    /// Current value of the bump-allocator pointer, for aux persistence.
    pub fn next_block_snapshot(&self) -> u64 {
        self.next_block.load(Ordering::Relaxed)
    }

    /// Configured storage capacity in bytes.
    pub fn capacity_bytes(&self) -> u64 {
        self.capacity_blocks * BLOCK_SIZE as u64
    }

    /// Number of free NVMe 4 KiB blocks tracked in the free-list.
    pub fn free_block_count(&self) -> u64 {
        self.free_list
            .lock()
            .map(|fl| fl.iter().map(|r| r.len as u64).sum())
            .unwrap_or(0)
    }

    /// Restore allocator state from aux (called once after `open()` on restart).
    pub fn restore_state(&self, next_block: u64, free_blocks: Vec<BlockRange>) {
        self.next_block.store(next_block, Ordering::Relaxed);
        if let Ok(mut fl) = self.free_list.lock() {
            *fl = free_blocks;
        }
    }

    /// Release cold-tier NVMe blocks into the free-list.
    /// Called from the `free()` type callback on TTL expiry for cold keys.
    /// Does NOT touch the STORAGE index — ownership already left the index at demotion time.
    pub fn release_cold_blocks(&self, backend_offset: u64, num_blocks: u32) {
        let block_start = backend_offset / BLOCK_SIZE as u64;
        self.push_free_range(block_start, num_blocks);
        BYTES_RECLAIMED.fetch_add(num_blocks as u64 * BLOCK_SIZE as u64, Ordering::Relaxed);
    }

    /// Remove a key from the STORAGE index without freeing its blocks.
    /// Used by demotion: after `put()` writes the value, ownership transfers to
    /// `Tier::Cold`, so the index entry must be removed to prevent double-free on
    /// TTL expiry (which reclaims via `release_cold_blocks`).
    pub fn remove_from_index(&self, key: &[u8]) {
        if let Ok(mut idx) = self.index.lock() {
            idx.remove(key);
        }
    }

    /// Fork-safe cousin of [`read_at_offset`]: reads via `pread(2)` against
    /// the file descriptor directly, bypassing the io_uring ring.
    ///
    /// Use this from any path that runs in a forked child — `rdb_save` and
    /// `aof_rewrite` in particular. The parent's io_uring ring is not fork-
    /// safe (SQ/CQ buffers are kernel-shared and the `ring` mutex, if locked
    /// at fork time, is inherited in a poisoned state). `pread` touches only
    /// the inherited fd and process-local memory, so it is always safe.
    ///
    /// Advises `POSIX_FADV_WILLNEED` on the block range before the read so
    /// the kernel can pre-populate the page cache. The buffer alignment
    /// guarantees satisfy the `O_DIRECT` requirement (same `AlignedBuf`
    /// used by the io_uring path). Slower than io_uring — which is fine,
    /// as persistence-child throughput isn't on the hot path.
    ///
    /// Callers on the parent process should keep using [`read_at_offset`].
    pub fn pread_at_offset(&self, backend_offset: u64, value_len: u32) -> StorageResult<Vec<u8>> {
        let num_blocks = Self::blocks_needed(value_len as usize);
        let buf_size = num_blocks as usize * BLOCK_SIZE;
        let buf = AlignedBuf::new(buf_size)?;
        let fd = self.file.as_raw_fd();

        // Hint the kernel; advice failures are non-fatal.
        unsafe {
            libc::posix_fadvise(
                fd,
                backend_offset as libc::off_t,
                buf_size as libc::off_t,
                libc::POSIX_FADV_WILLNEED,
            );
        }

        // SAFETY: buf.ptr is a live `buf_size`-byte allocation, BLOCK_SIZE-aligned
        // (so the O_DIRECT constraints on the fd are satisfied); backend_offset
        // is already block-aligned by the allocator; fd is the module's own
        // owned NVMe file descriptor inherited into the forked child.
        let ret = unsafe {
            libc::pread(
                fd,
                buf.ptr as *mut c_void,
                buf_size,
                backend_offset as libc::off_t,
            )
        };
        if ret < 0 {
            return Err(StorageError::Io(std::io::Error::last_os_error()));
        }
        if (ret as usize) < buf_size {
            return Err(StorageError::Other(format!(
                "pread short read: {ret} < {buf_size}"
            )));
        }

        let slice = buf.as_slice();
        let len_bytes: [u8; 8] = slice[..8]
            .try_into()
            .map_err(|_| StorageError::Other("corrupt record: header too short".into()))?;
        let stored_len = u64::from_le_bytes(len_bytes) as usize;
        if stored_len != value_len as usize {
            return Err(StorageError::Other(format!(
                "header len {stored_len} != expected len {value_len}"
            )));
        }
        Ok(slice[8..8 + stored_len].to_vec())
    }

    /// Read `value_len` bytes from NVMe starting at byte offset `backend_offset`.
    /// Used by the cold-tier read path (FLASH.GET) and rdb_save for cold objects.
    pub fn read_at_offset(&self, backend_offset: u64, value_len: u32) -> StorageResult<Vec<u8>> {
        let num_blocks = Self::blocks_needed(value_len as usize);
        let buf_size = num_blocks as usize * BLOCK_SIZE;
        let mut buf = AlignedBuf::new(buf_size)?;
        self.do_read(backend_offset, &mut buf)?;
        let slice = buf.as_slice();
        let len_bytes: [u8; 8] = slice[..8]
            .try_into()
            .map_err(|_| StorageError::Other("corrupt record: header too short".into()))?;
        let stored_len = u64::from_le_bytes(len_bytes) as usize;
        if stored_len != value_len as usize {
            return Err(StorageError::Other(format!(
                "header len {stored_len} != expected len {value_len}"
            )));
        }
        Ok(slice[8..8 + stored_len].to_vec())
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

// ── Free-list coalescing ──────────────────────────────────────────────────────

/// Sort ranges by start, then merge adjacent or overlapping ranges in-place.
/// After coalescing, no two ranges share or overlap any block.
pub fn coalesce_ranges(ranges: &mut Vec<BlockRange>) {
    if ranges.len() < 2 {
        return;
    }
    ranges.sort_unstable_by_key(|r| r.start);
    let mut out: Vec<BlockRange> = Vec::with_capacity(ranges.len());
    for &r in ranges.iter() {
        if let Some(last) = out.last_mut() {
            let last_end = last.start + last.len as u64;
            if r.start <= last_end {
                // Adjacent or overlapping: extend the last range if needed.
                let new_end = r.start + r.len as u64;
                if new_end > last_end {
                    last.len = (new_end - last.start) as u32;
                }
                continue;
            }
        }
        out.push(r);
    }
    *ranges = out;
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

    fn put(&self, key: &[u8], value: &[u8]) -> StorageResult<u64> {
        let n_blocks = Self::blocks_needed(value.len());
        let block_offset = self.alloc_blocks(n_blocks)?;
        self.write_value_at(block_offset, value)?;
        let new_entry = IndexEntry {
            block_offset,
            value_len: value.len() as u32,
            num_blocks: n_blocks,
        };
        // Atomically swap new entry in and capture the old one (if overwrite).
        let old_entry = {
            let mut index = self
                .index
                .lock()
                .map_err(|e| StorageError::Other(format!("index lock poisoned: {e}")))?;
            index.insert(key.to_vec(), new_entry)
        };
        // Free old blocks outside the index lock.
        if let Some(old) = old_entry {
            self.push_free_range(old.block_offset, old.num_blocks);
            BYTES_RECLAIMED.fetch_add(old.num_blocks as u64 * BLOCK_SIZE as u64, Ordering::Relaxed);
        }
        Ok(block_offset * BLOCK_SIZE as u64)
    }

    fn delete(&self, key: &[u8]) -> StorageResult<()> {
        let old_entry = {
            let mut index = self
                .index
                .lock()
                .map_err(|e| StorageError::Other(format!("index lock poisoned: {e}")))?;
            index.remove(key)
        };
        if let Some(old) = old_entry {
            self.push_free_range(old.block_offset, old.num_blocks);
            BYTES_RECLAIMED.fetch_add(old.num_blocks as u64 * BLOCK_SIZE as u64, Ordering::Relaxed);
        }
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

    // ── pread_at_offset ────────────────────────────────────────────────────────
    //
    // pread is the fork-safe cousin of read_at_offset used by rdb_save /
    // aof_rewrite inside the BGSAVE/BGREWRITEAOF child. These tests verify
    // it recovers the same bytes as the io_uring path.

    #[test]
    fn pread_at_offset_roundtrip_matches_put() {
        let (b, _tmp) = open_backend();
        let payload = b"hello pread world";
        let offset = b.put(b"k", payload).unwrap();
        let recovered = b
            .pread_at_offset(offset, payload.len() as u32)
            .expect("pread must succeed");
        assert_eq!(recovered, payload);
    }

    #[test]
    fn pread_at_offset_matches_read_at_offset() {
        let (b, _tmp) = open_backend();
        let payload = b"matching-both-read-paths";
        let offset = b.put(b"k", payload).unwrap();
        let via_pread = b
            .pread_at_offset(offset, payload.len() as u32)
            .expect("pread path");
        let via_ring = b
            .read_at_offset(offset, payload.len() as u32)
            .expect("ring path");
        assert_eq!(
            via_pread, via_ring,
            "pread and io_uring must return identical bytes"
        );
        assert_eq!(via_pread, payload);
    }

    #[test]
    fn pread_at_offset_multi_block_value() {
        // Force a multi-block payload (8 KiB > 4 KiB block size) so the
        // pread call covers more than one NVMe block.
        let (b, _tmp) = open_backend();
        let payload: Vec<u8> = (0..8192u32).map(|i| (i % 251) as u8).collect();
        let offset = b.put(b"multi", &payload).unwrap();
        let recovered = b.pread_at_offset(offset, payload.len() as u32).unwrap();
        assert_eq!(recovered, payload);
    }

    #[test]
    fn blocks_needed_unit() {
        assert_eq!(FileIoUringBackend::blocks_needed(0), 1); // header only → 8 bytes → 1 block
        assert_eq!(FileIoUringBackend::blocks_needed(4088), 1); // 8+4088=4096 → 1 block
        assert_eq!(FileIoUringBackend::blocks_needed(4089), 2); // 8+4089=4097 → 2 blocks
        assert_eq!(FileIoUringBackend::blocks_needed(8184), 2); // 8+8184=8192 → exactly 2 blocks
        assert_eq!(FileIoUringBackend::blocks_needed(8185), 3); // 8+8185=8193 → 3 blocks
    }

    // ── Free-list + coalescing tests ──────────────────────────────────────────

    #[test]
    fn delete_frees_blocks_visible_in_free_list() {
        let (b, _tmp) = open_backend();
        b.put(b"k", b"v").unwrap();
        assert_eq!(b.free_block_count(), 0);
        b.delete(b"k").unwrap();
        assert_eq!(b.free_block_count(), 1); // 1 block freed (small value)
    }

    #[test]
    fn overwrite_frees_old_blocks() {
        let (b, _tmp) = open_backend();
        b.put(b"k", b"v1").unwrap();
        b.put(b"k", b"v2").unwrap();
        // Old allocation freed; free_list has 1 block from v1's allocation.
        assert_eq!(b.free_block_count(), 1);
    }

    #[test]
    fn freed_blocks_reused_by_next_put() {
        let (b, _tmp) = open_backend();
        b.put(b"k1", b"hello").unwrap();
        let next_before_delete = b.next_block_snapshot();
        b.delete(b"k1").unwrap();
        // next_block has not advanced; the freed block is in the free-list.
        b.put(b"k2", b"world").unwrap();
        // next_block should still equal next_before_delete (reused freed block).
        assert_eq!(
            b.next_block_snapshot(),
            next_before_delete,
            "freed block should be reused, not bump-allocated"
        );
    }

    #[test]
    fn compaction_tick_coalesces_adjacent_ranges() {
        let (b, _tmp) = open_backend();
        // Write 3 small values, each takes 1 block; they land at blocks 0, 1, 2.
        b.put(b"a", b"v").unwrap();
        b.put(b"b", b"v").unwrap();
        b.put(b"c", b"v").unwrap();
        b.delete(b"a").unwrap();
        b.delete(b"b").unwrap();
        b.delete(b"c").unwrap();
        // 3 separate 1-block ranges in the free-list.
        {
            let fl = b.free_list.lock().unwrap();
            assert_eq!(fl.len(), 3);
        }
        b.run_compaction_tick();
        // After coalesce: 1 merged range covering all 3 blocks.
        {
            let fl = b.free_list.lock().unwrap();
            assert_eq!(fl.len(), 1);
            assert_eq!(fl[0].len, 3);
        }
        assert_eq!(COMPACTION_RUNS.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn restore_state_sets_next_block_and_free_list() {
        let (b, _tmp) = open_backend();
        let ranges = vec![
            BlockRange { start: 5, len: 2 },
            BlockRange { start: 10, len: 3 },
        ];
        b.restore_state(20, ranges.clone());
        assert_eq!(b.next_block_snapshot(), 20);
        assert_eq!(b.free_list_snapshot(), ranges);
    }

    #[test]
    fn bytes_reclaimed_incremented_on_delete() {
        let before = BYTES_RECLAIMED.load(Ordering::Relaxed);
        let (b, _tmp) = open_backend();
        b.put(b"k", b"v").unwrap();
        b.delete(b"k").unwrap();
        let after = BYTES_RECLAIMED.load(Ordering::Relaxed);
        let delta = after - before;
        // Our delete freed ≥1 block; concurrent parallel tests may free additional blocks,
        // so check delta >= BLOCK_SIZE and block-aligned rather than exact equality.
        assert!(
            delta >= BLOCK_SIZE as u64 && delta.is_multiple_of(BLOCK_SIZE as u64),
            "expected BYTES_RECLAIMED delta >= BLOCK_SIZE and block-aligned; got {delta}",
        );
    }

    #[test]
    fn coalesce_empty_is_noop() {
        let mut v: Vec<BlockRange> = Vec::new();
        coalesce_ranges(&mut v);
        assert!(v.is_empty());
    }

    #[test]
    fn coalesce_single_is_noop() {
        let mut v = vec![BlockRange { start: 3, len: 2 }];
        coalesce_ranges(&mut v);
        assert_eq!(v, vec![BlockRange { start: 3, len: 2 }]);
    }

    #[test]
    fn coalesce_adjacent_merged() {
        let mut v = vec![
            BlockRange { start: 5, len: 3 }, // blocks 5,6,7
            BlockRange { start: 8, len: 2 }, // blocks 8,9 — adjacent
        ];
        coalesce_ranges(&mut v);
        assert_eq!(v, vec![BlockRange { start: 5, len: 5 }]);
    }

    #[test]
    fn coalesce_overlapping_merged() {
        let mut v = vec![
            BlockRange { start: 5, len: 4 }, // blocks 5-8
            BlockRange { start: 7, len: 4 }, // blocks 7-10 — overlaps
        ];
        coalesce_ranges(&mut v);
        assert_eq!(v, vec![BlockRange { start: 5, len: 6 }]);
    }

    #[test]
    fn coalesce_non_adjacent_kept_separate() {
        let mut v = vec![
            BlockRange { start: 0, len: 2 }, // blocks 0,1
            BlockRange { start: 5, len: 2 }, // blocks 5,6 — gap at 2,3,4
        ];
        coalesce_ranges(&mut v);
        assert_eq!(v.len(), 2);
    }

    #[test]
    fn coalesce_unsorted_input_sorted_first() {
        let mut v = vec![
            BlockRange { start: 10, len: 2 }, // blocks 10,11
            BlockRange { start: 0, len: 5 },  // blocks 0-4
            BlockRange { start: 5, len: 5 },  // blocks 5-9 (adjacent to above)
        ];
        coalesce_ranges(&mut v);
        // 5+5+2 = 12 blocks merged into one range starting at 0.
        assert_eq!(v, vec![BlockRange { start: 0, len: 12 }]);
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
