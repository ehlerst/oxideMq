use bytes::Bytes;
use oxidemq_core::error::{OxideMqError, Result};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Object storage abstraction for S3 / RustStack S3.
pub trait ObjectStorage: Send + Sync {
    /// Uploads an immutable object to the specified key.
    fn put_object(&self, key: &str, data: Bytes) -> Result<()>;

    /// Retrieves an entire object from storage.
    fn get_object(&self, key: &str) -> Result<Bytes>;

    /// Retrieves a byte range `[start, end]` (inclusive) from the specified object.
    fn get_object_range(&self, key: &str, start: u64, end: u64) -> Result<Bytes>;

    /// Deletes a single object from storage.
    fn delete_object(&self, key: &str) -> Result<()>;

    /// Deletes multiple objects in a single batch.
    fn delete_objects(&self, keys: &[String]) -> Result<()>;

    /// Lists object keys matching the given prefix.
    fn list_objects(&self, prefix: &str) -> Result<Vec<String>>;
}

/// A high-performance, in-memory object store for Tier 1 tests, RustStack emulation,
/// and nanosecond microbenchmarks.
#[derive(Debug, Default)]
pub struct MemoryObjectStorage {
    objects: Arc<RwLock<HashMap<String, Bytes>>>,
    put_count: AtomicU64,
    get_count: AtomicU64,
    bytes_stored: AtomicU64,
}

impl MemoryObjectStorage {
    pub fn new() -> Self {
        Self {
            objects: Arc::new(RwLock::new(HashMap::new())),
            put_count: AtomicU64::new(0),
            get_count: AtomicU64::new(0),
            bytes_stored: AtomicU64::new(0),
        }
    }

    pub fn put_count(&self) -> u64 {
        self.put_count.load(Ordering::Relaxed)
    }

    pub fn get_count(&self) -> u64 {
        self.get_count.load(Ordering::Relaxed)
    }

    pub fn total_bytes(&self) -> u64 {
        self.bytes_stored.load(Ordering::Relaxed)
    }

    pub fn clear(&self) {
        let mut obs = self.objects.write();
        obs.clear();
        self.bytes_stored.store(0, Ordering::Relaxed);
    }
}

impl ObjectStorage for MemoryObjectStorage {
    fn put_object(&self, key: &str, data: Bytes) -> Result<()> {
        let len = data.len() as u64;
        let mut obs = self.objects.write();
        if let Some(old) = obs.insert(key.to_string(), data) {
            self.bytes_stored
                .fetch_sub(old.len() as u64, Ordering::Relaxed);
        }
        self.bytes_stored.fetch_add(len, Ordering::Relaxed);
        self.put_count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn get_object(&self, key: &str) -> Result<Bytes> {
        let obs = self.objects.read();
        self.get_count.fetch_add(1, Ordering::Relaxed);
        obs.get(key)
            .cloned()
            .ok_or_else(|| OxideMqError::Storage(format!("Object not found: {}", key)))
    }

    fn get_object_range(&self, key: &str, start: u64, end: u64) -> Result<Bytes> {
        let obs = self.objects.read();
        self.get_count.fetch_add(1, Ordering::Relaxed);
        let obj = obs
            .get(key)
            .ok_or_else(|| OxideMqError::Storage(format!("Object not found: {}", key)))?;

        let obj_len = obj.len() as u64;
        if start >= obj_len {
            return Ok(Bytes::new());
        }

        let actual_end = (end + 1).min(obj_len);
        let slice = &obj[start as usize..actual_end as usize];
        Ok(Bytes::copy_from_slice(slice))
    }

    fn delete_object(&self, key: &str) -> Result<()> {
        let mut obs = self.objects.write();
        if let Some(data) = obs.remove(key) {
            self.bytes_stored
                .fetch_sub(data.len() as u64, Ordering::Relaxed);
        }
        Ok(())
    }

    fn delete_objects(&self, keys: &[String]) -> Result<()> {
        let mut obs = self.objects.write();
        for key in keys {
            if let Some(data) = obs.remove(key) {
                self.bytes_stored
                    .fetch_sub(data.len() as u64, Ordering::Relaxed);
            }
        }
        Ok(())
    }

    fn list_objects(&self, prefix: &str) -> Result<Vec<String>> {
        let obs = self.objects.read();
        let mut matches: Vec<String> = obs
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect();
        matches.sort();
        Ok(matches)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_object_storage() {
        let storage = MemoryObjectStorage::new();
        let key = "streams/stream-1/00000000000000000000.data";
        let payload = Bytes::from_static(b"abcdefghijklmnopqrstuvwxyz");

        storage.put_object(key, payload.clone()).unwrap();
        assert_eq!(storage.put_count(), 1);

        // Full get
        let retrieved = storage.get_object(key).unwrap();
        assert_eq!(retrieved, payload);

        // Range get: bytes=4-9 ("efghij")
        let range = storage.get_object_range(key, 4, 9).unwrap();
        assert_eq!(range.as_ref(), b"efghij");

        // List objects
        let list = storage.list_objects("streams/stream-1").unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0], key);

        // Delete
        storage.delete_object(key).unwrap();
        assert!(storage.get_object(key).is_err());
    }
}
