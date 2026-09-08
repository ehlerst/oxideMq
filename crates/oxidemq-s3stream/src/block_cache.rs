use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BlockKey {
    pub object_key: String,
    pub stream_id: u64,
    pub start_offset: i64,
}

impl BlockKey {
    pub fn new(object_key: impl Into<String>, stream_id: u64, start_offset: i64) -> Self {
        Self {
            object_key: object_key.into(),
            stream_id,
            start_offset,
        }
    }
}

/// Tier 2 high-performance LRU cache storing decoded S3 data blocks for historical readers.
#[derive(Debug)]
pub struct BlockCache {
    max_bytes: usize,
    current_bytes: Arc<RwLock<usize>>,
    entries: Arc<RwLock<HashMap<BlockKey, Bytes>>>,
    lru_order: Arc<RwLock<VecDeque<BlockKey>>>,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl BlockCache {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            current_bytes: Arc::new(RwLock::new(0)),
            entries: Arc::new(RwLock::new(HashMap::new())),
            lru_order: Arc::new(RwLock::new(VecDeque::new())),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Retrieves a block from the cache, updating its LRU position.
    pub fn get(&self, key: &BlockKey) -> Option<Bytes> {
        let entries = self.entries.read();
        if let Some(data) = entries.get(key) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            let val = data.clone();
            drop(entries);

            // Update LRU order
            let mut lru = self.lru_order.write();
            if let Some(pos) = lru.iter().position(|k| k == key) {
                lru.remove(pos);
            }
            lru.push_back(key.clone());

            Some(val)
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Stores a block in the cache, evicting the least recently used blocks if capacity is exceeded.
    pub fn put(&self, key: BlockKey, data: Bytes) {
        let data_len = data.len();
        let mut entries = self.entries.write();
        let mut lru = self.lru_order.write();
        let mut cur_bytes = self.current_bytes.write();

        // Evict LRU entries if capacity exceeded
        while *cur_bytes + data_len > self.max_bytes && !lru.is_empty() {
            if let Some(old_key) = lru.pop_front() {
                if let Some(old_data) = entries.remove(&old_key) {
                    *cur_bytes = cur_bytes.saturating_sub(old_data.len());
                }
            }
        }

        if let Some(old) = entries.insert(key.clone(), data) {
            *cur_bytes = cur_bytes.saturating_sub(old.len());
            if let Some(pos) = lru.iter().position(|k| k == &key) {
                lru.remove(pos);
            }
        }

        *cur_bytes += data_len;
        lru.push_back(key);
    }

    pub fn current_size_bytes(&self) -> usize {
        *self.current_bytes.read()
    }

    pub fn hit_count(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    pub fn miss_count(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    pub fn clear(&self) {
        let mut entries = self.entries.write();
        let mut lru = self.lru_order.write();
        let mut cur_bytes = self.current_bytes.write();
        entries.clear();
        lru.clear();
        *cur_bytes = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_cache_lru() {
        let cache = BlockCache::new(50);
        let k1 = BlockKey::new("obj1", 1, 0);
        let k2 = BlockKey::new("obj1", 1, 10);
        let k3 = BlockKey::new("obj1", 1, 20);

        let d1 = Bytes::from(vec![1u8; 25]);
        let d2 = Bytes::from(vec![2u8; 25]);
        let d3 = Bytes::from(vec![3u8; 25]);

        cache.put(k1.clone(), d1.clone());
        cache.put(k2.clone(), d2.clone());

        // Access k1 to make it most recently used
        assert_eq!(cache.get(&k1), Some(d1));

        // Adding k3 should evict k2 (since k1 was just accessed)
        cache.put(k3.clone(), d3);

        assert_eq!(cache.get(&k2), None);
        assert!(cache.get(&k1).is_some());
        assert!(cache.get(&k3).is_some());

        assert!(cache.hit_count() > 0);
        assert!(cache.miss_count() > 0);
        assert!(cache.current_size_bytes() <= 50);

        // Overwriting existing key
        cache.put(k1.clone(), Bytes::from(vec![9u8; 20]));
        assert_eq!(cache.get(&k1).unwrap().len(), 20);

        // Oversized entry evicts previous entries
        let huge = Bytes::from(vec![0u8; 100]);
        cache.put(BlockKey::new("huge", 1, 0), huge);
        assert!(cache.get(&BlockKey::new("huge", 1, 0)).is_some());
        assert_eq!(cache.get(&k1), None);
        assert_eq!(cache.get(&k3), None);

        // Clear
        cache.clear();
        assert_eq!(cache.current_size_bytes(), 0);
        assert_eq!(cache.get(&k1), None);
    }
}
