//! In-memory storage backend for testing.
//!
//! Provides a HashMap-based storage that implements the Storage trait.
//! Used for `memory://` URI scheme in tests to avoid file I/O.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use emsqrt_mem::Storage;
use emsqrt_mem::error::{Error as MemError, Result as MemResult};

/// Thread-safe in-memory storage using a HashMap.
#[derive(Clone)]
pub struct MemoryStorage {
    data: Arc<Mutex<HashMap<String, Vec<u8>>>>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self {
            data: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Pre-populate data for a path (used by tests)
    pub fn insert(&self, path: String, bytes: Vec<u8>) {
        let mut data = self.data.lock().unwrap();
        data.insert(path, bytes);
    }

    /// Check if a path exists
    pub fn contains(&self, path: &str) -> bool {
        let data = self.data.lock().unwrap();
        data.contains_key(path)
    }

    /// Get the number of stored objects
    pub fn len(&self) -> usize {
        let data = self.data.lock().unwrap();
        data.len()
    }

    /// Clear all stored data
    pub fn clear(&self) {
        let mut data = self.data.lock().unwrap();
        data.clear();
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl Storage for MemoryStorage {
    fn write(&self, path: &str, bytes: &[u8]) -> MemResult<()> {
        let mut data = self.data.lock().unwrap();
        data.insert(path.to_string(), bytes.to_vec());
        Ok(())
    }

    fn read_range(&self, path: &str, offset: u64, len: usize) -> MemResult<Vec<u8>> {
        let data = self.data.lock().unwrap();
        let bytes = data
            .get(path)
            .ok_or_else(|| MemError::Storage(format!("path not found: {}", path)))?;

        let start = offset as usize;
        let end = (start + len).min(bytes.len());

        if start >= bytes.len() {
            return Err(MemError::Storage(format!(
                "offset {} exceeds size {}",
                offset,
                bytes.len()
            )));
        }

        Ok(bytes[start..end].to_vec())
    }

    fn delete(&self, path: &str) -> MemResult<()> {
        let mut data = self.data.lock().unwrap();
        data.remove(path);
        Ok(())
    }

    fn list(&self, prefix: &str) -> MemResult<Vec<String>> {
        let data = self.data.lock().unwrap();
        let mut result: Vec<String> = data
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect();
        result.sort();
        Ok(result)
    }

    fn size(&self, path: &str) -> MemResult<u64> {
        let data = self.data.lock().unwrap();
        let bytes = data
            .get(path)
            .ok_or_else(|| MemError::Storage(format!("path not found: {}", path)))?;
        Ok(bytes.len() as u64)
    }

    fn etag(&self, path: &str) -> MemResult<Option<String>> {
        // For in-memory storage, use a simple hash of the path as etag
        let data = self.data.lock().unwrap();
        if data.contains_key(path) {
            Ok(Some(format!("mem-{}", path.len())))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_storage_write_read() {
        let storage = MemoryStorage::new();
        let path = "test/file.txt";
        let data = b"hello world";

        storage.write(path, data).unwrap();
        let read_data = storage.read_range(path, 0, data.len()).unwrap();

        assert_eq!(read_data, data);
    }

    #[test]
    fn test_memory_storage_read_range() {
        let storage = MemoryStorage::new();
        let path = "test/file.txt";
        let data = b"hello world";

        storage.write(path, data).unwrap();

        // Read partial range
        let partial = storage.read_range(path, 6, 5).unwrap();
        assert_eq!(partial, b"world");
    }

    #[test]
    fn test_memory_storage_delete() {
        let storage = MemoryStorage::new();
        let path = "test/file.txt";

        storage.write(path, b"data").unwrap();
        assert!(storage.contains(path));

        storage.delete(path).unwrap();
        assert!(!storage.contains(path));
    }

    #[test]
    fn test_memory_storage_list() {
        let storage = MemoryStorage::new();

        storage.write("dir/file1.txt", b"1").unwrap();
        storage.write("dir/file2.txt", b"2").unwrap();
        storage.write("other/file3.txt", b"3").unwrap();

        let files = storage.list("dir/").unwrap();
        assert_eq!(files.len(), 2);
        assert!(files.contains(&"dir/file1.txt".to_string()));
        assert!(files.contains(&"dir/file2.txt".to_string()));
    }

    #[test]
    fn test_memory_storage_size() {
        let storage = MemoryStorage::new();
        let path = "test/file.txt";
        let data = b"hello world";

        storage.write(path, data).unwrap();
        let size = storage.size(path).unwrap();

        assert_eq!(size, data.len() as u64);
    }
}

