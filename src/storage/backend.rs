use std::collections::HashMap;
use std::fmt;
use std::sync::Mutex;

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

// ── Trait ─────────────────────────────────────────────────────────────────────

/// Backing store for the cold NVMe tier. All methods are synchronous from the
/// caller's perspective; async I/O lives behind the implementation.
///
/// The `'_` lifetime on `iter` ties the iterator to the backend borrow so
/// implementations can hand out references into internal buffers without
/// requiring an owned copy of every key/value pair.
pub trait StorageBackend: Send + Sync {
    /// Look up `key`. Returns `None` if the key is absent.
    fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>>;

    /// Insert or overwrite `key` with `value`.
    fn put(&self, key: &[u8], value: &[u8]) -> StorageResult<()>;

    /// Remove `key`. Succeeds (no-op) if the key is absent.
    fn delete(&self, key: &[u8]) -> StorageResult<()>;

    /// Iterate all key-value pairs in undefined order.
    fn iter(&self) -> Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + '_>;

    /// Flush any pending writes to durable storage. May be a no-op for
    /// backends that write synchronously.
    fn flush(&self) -> StorageResult<()>;
}

// ── MockStorage ───────────────────────────────────────────────────────────────

/// In-memory storage backend used by unit tests. Not for production use.
pub struct MockStorage {
    data: Mutex<HashMap<Vec<u8>, Vec<u8>>>,
}

impl MockStorage {
    pub fn new() -> Self {
        MockStorage {
            data: Mutex::new(HashMap::new()),
        }
    }
}

impl Default for MockStorage {
    fn default() -> Self {
        MockStorage::new()
    }
}

impl StorageBackend for MockStorage {
    fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        let data = self
            .data
            .lock()
            .map_err(|e| StorageError::Other(format!("lock poisoned: {e}")))?;
        Ok(data.get(key).cloned())
    }

    fn put(&self, key: &[u8], value: &[u8]) -> StorageResult<()> {
        let mut data = self
            .data
            .lock()
            .map_err(|e| StorageError::Other(format!("lock poisoned: {e}")))?;
        data.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    fn delete(&self, key: &[u8]) -> StorageResult<()> {
        let mut data = self
            .data
            .lock()
            .map_err(|e| StorageError::Other(format!("lock poisoned: {e}")))?;
        data.remove(key);
        Ok(())
    }

    fn iter(&self) -> Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + '_> {
        // Collect a snapshot to avoid holding the lock across the iterator's
        // lifetime, which would require the iterator to hold the MutexGuard.
        let data = self.data.lock().expect("MockStorage::iter: lock poisoned");
        let snapshot: Vec<(Vec<u8>, Vec<u8>)> =
            data.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
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
        let mut pairs: Vec<(Vec<u8>, Vec<u8>)> = s.iter().collect();
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
    fn storage_error_display() {
        assert!(!StorageError::Closed.to_string().is_empty());
        assert!(!StorageError::KeyTooLarge(1024).to_string().is_empty());
        assert!(!StorageError::ValueTooLarge(4096).to_string().is_empty());
        assert!(!StorageError::Other("oops".into()).to_string().is_empty());
    }
}
