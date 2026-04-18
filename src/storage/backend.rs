use std::fmt;

// ── Error type ────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum StorageError {
    Io(std::io::Error),
    KeyTooLarge(usize),
    ValueTooLarge(usize),
    Closed,
    Other(String),
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorageError::Io(e) => write!(f, "I/O error: {e}"),
            StorageError::KeyTooLarge(n) => write!(f, "key too large: {n} bytes"),
            StorageError::ValueTooLarge(n) => write!(f, "value too large: {n} bytes"),
            StorageError::Closed => write!(f, "storage is closed"),
            StorageError::Other(s) => write!(f, "{s}"),
        }
    }
}

impl std::error::Error for StorageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            StorageError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for StorageError {
    fn from(e: std::io::Error) -> Self {
        StorageError::Io(e)
    }
}

pub type StorageResult<T> = Result<T, StorageError>;

/// Item type yielded by [`StorageBackend::iter`].
pub type KvPair = (Vec<u8>, Vec<u8>);

// ── Trait ─────────────────────────────────────────────────────────────────────

/// Backing store for the cold NVMe tier. All methods are synchronous from the
/// caller's perspective; async I/O lives behind the implementation.
pub trait StorageBackend: Send + Sync {
    /// Look up `key`. Returns `None` if the key is absent.
    fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>>;

    /// Insert or overwrite `key` with `value`.
    /// Returns the NVMe byte offset at which the value was written.
    fn put(&self, key: &[u8], value: &[u8]) -> StorageResult<u64>;

    /// Remove `key`. Succeeds (no-op) if the key is absent.
    fn delete(&self, key: &[u8]) -> StorageResult<()>;

    /// Iterate all key-value pairs in undefined order. Each item is wrapped in
    /// `StorageResult` so mid-iteration I/O failures (e.g. a bad NVMe read
    /// during `rdb_save`) are surfaced rather than panicked or silently dropped.
    /// The `'_` lifetime ties the iterator to the backend borrow.
    fn iter(&self) -> Box<dyn Iterator<Item = StorageResult<KvPair>> + '_>;

    /// Flush any pending writes to durable storage. May be a no-op for
    /// backends that write synchronously.
    fn flush(&self) -> StorageResult<()>;
}

// ── MockStorage ───────────────────────────────────────────────────────────────

#[cfg(test)]
pub struct MockStorage {
    data: std::sync::Mutex<std::collections::HashMap<Vec<u8>, Vec<u8>>>,
}

#[cfg(test)]
impl MockStorage {
    pub fn new() -> Self {
        MockStorage {
            data: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }
}

#[cfg(test)]
impl Default for MockStorage {
    fn default() -> Self {
        MockStorage::new()
    }
}

#[cfg(test)]
impl StorageBackend for MockStorage {
    fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        let data = self
            .data
            .lock()
            .map_err(|e| StorageError::Other(format!("lock poisoned: {e}")))?;
        Ok(data.get(key).cloned())
    }

    fn put(&self, key: &[u8], value: &[u8]) -> StorageResult<u64> {
        let mut data = self
            .data
            .lock()
            .map_err(|e| StorageError::Other(format!("lock poisoned: {e}")))?;
        data.insert(key.to_vec(), value.to_vec());
        Ok(0) // mock has no real NVMe offset
    }

    fn delete(&self, key: &[u8]) -> StorageResult<()> {
        let mut data = self
            .data
            .lock()
            .map_err(|e| StorageError::Other(format!("lock poisoned: {e}")))?;
        data.remove(key);
        Ok(())
    }

    fn iter(&self) -> Box<dyn Iterator<Item = StorageResult<KvPair>> + '_> {
        // Collect a snapshot to avoid holding the lock across the iterator's lifetime.
        let data = self.data.lock().expect("MockStorage::iter: lock poisoned");
        let snapshot: Vec<StorageResult<KvPair>> = data
            .iter()
            .map(|(k, v)| Ok((k.clone(), v.clone())))
            .collect();
        Box::new(snapshot.into_iter())
    }

    fn flush(&self) -> StorageResult<()> {
        Ok(())
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> MockStorage {
        MockStorage::new()
    }

    #[test]
    fn get_missing_returns_none() {
        let s = store();
        assert!(s.get(b"absent").unwrap().is_none());
    }

    #[test]
    fn put_then_get_returns_value() {
        let s = store();
        s.put(b"k", b"v").unwrap();
        assert_eq!(s.get(b"k").unwrap(), Some(b"v".to_vec()));
    }

    #[test]
    fn put_overwrite_returns_new_value() {
        let s = store();
        s.put(b"k", b"v1").unwrap();
        s.put(b"k", b"v2").unwrap();
        assert_eq!(s.get(b"k").unwrap(), Some(b"v2".to_vec()));
    }

    #[test]
    fn delete_present_key() {
        let s = store();
        s.put(b"k", b"v").unwrap();
        s.delete(b"k").unwrap();
        assert!(s.get(b"k").unwrap().is_none());
    }

    #[test]
    fn delete_missing_key_is_noop() {
        let s = store();
        s.delete(b"absent").unwrap();
    }

    #[test]
    fn iter_empty_storage() {
        let s = store();
        assert_eq!(s.iter().count(), 0);
    }

    #[test]
    fn iter_populated_storage() {
        let s = store();
        s.put(b"a", b"1").unwrap();
        s.put(b"b", b"2").unwrap();
        s.put(b"c", b"3").unwrap();
        let mut pairs: Vec<(Vec<u8>, Vec<u8>)> = s
            .iter()
            .map(|r| r.expect("mock iter never errors"))
            .collect();
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
    fn flush_is_noop_on_mock() {
        let s = store();
        s.put(b"k", b"v").unwrap();
        assert!(s.flush().is_ok());
        assert_eq!(s.get(b"k").unwrap(), Some(b"v".to_vec()));
    }

    #[test]
    fn storage_error_display_variants() {
        assert!(!StorageError::Closed.to_string().is_empty());
        assert!(!StorageError::KeyTooLarge(1024).to_string().is_empty());
        assert!(!StorageError::ValueTooLarge(4096).to_string().is_empty());
        assert!(!StorageError::Other("oops".into()).to_string().is_empty());
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        assert!(StorageError::Io(io_err).to_string().contains("I/O error"));
    }

    #[test]
    fn storage_error_io_source_is_some() {
        let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied");
        let err = StorageError::Io(io_err);
        assert!(std::error::Error::source(&err).is_some());
    }

    #[test]
    fn storage_error_non_io_source_is_none() {
        assert!(std::error::Error::source(&StorageError::Closed).is_none());
    }

    #[test]
    fn from_io_error_conversion() {
        let io_err = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "broken");
        let storage_err: StorageError = io_err.into();
        assert!(matches!(storage_err, StorageError::Io(_)));
    }
}
