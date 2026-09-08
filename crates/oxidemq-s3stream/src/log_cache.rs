use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// High-speed Tier 1 in-memory ring buffer holding recently committed records.
/// Delivers sub-millisecond latency (< 200 µs) for tailing consumers.
#[derive(Debug)]
pub struct LogCache {
    max_bytes: usize,
    current_bytes: Arc<RwLock<usize>>,
    // stream_id -> (start_offset, BTreeMap<offset, Bytes>)
    streams: Arc<RwLock<HashMap<u64, BTreeMap<i64, Bytes>>>>,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl LogCache {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            current_bytes: Arc::new(RwLock::new(0)),
            streams: Arc::new(RwLock::new(HashMap::new())),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Stores a record batch in the cache. Evicts oldest entries if capacity is exceeded.
    pub fn put(&self, stream_id: u64, offset: i64, data: Bytes) {
        let data_len = data.len();
        let mut streams = self.streams.write();
        let mut cur_bytes = self.current_bytes.write();

        // Evict if we exceed max_bytes
        while *cur_bytes + data_len > self.max_bytes && !streams.is_empty() {
            // Find stream with oldest record
            let mut oldest_stream = None;
            let mut oldest_offset = i64::MAX;

            for (&s_id, map) in streams.iter() {
                if let Some((&first_off, _)) = map.iter().next() {
                    if first_off < oldest_offset {
                        oldest_offset = first_off;
                        oldest_stream = Some(s_id);
                    }
                }
            }

            if let Some(s_id) = oldest_stream {
                let map = streams.get_mut(&s_id).unwrap();
                if let Some((_, evicted)) = map.pop_first() {
                    *cur_bytes -= evicted.len();
                }
                if map.is_empty() {
                    streams.remove(&s_id);
                }
            } else {
                break;
            }
        }

        let stream_map = streams.entry(stream_id).or_default();
        if let Some(old) = stream_map.insert(offset, data) {
            *cur_bytes -= old.len();
        }
        *cur_bytes += data_len;
    }

    /// Retrieves contiguous records starting at `start_offset` up to `max_bytes`.
    /// If the start offset is not in cache, records a miss.
    pub fn read_range(
        &self,
        stream_id: u64,
        start_offset: i64,
        max_bytes: usize,
    ) -> Option<Vec<(i64, Bytes)>> {
        let streams = self.streams.read();
        let stream_map = match streams.get(&stream_id) {
            Some(map) => map,
            None => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                return None;
            }
        };

        // Check if start_offset is present
        if !stream_map.contains_key(&start_offset) {
            self.misses.fetch_add(1, Ordering::Relaxed);
            return None;
        }

        let mut result = Vec::new();
        let mut total_bytes = 0;
        let mut expected_offset = start_offset;

        for (&off, payload) in stream_map.range(start_offset..) {
            if off != expected_offset {
                // Gap in cache
                break;
            }
            result.push((off, payload.clone()));
            total_bytes += payload.len();
            expected_offset += 1;

            if total_bytes >= max_bytes {
                break;
            }
        }

        if result.is_empty() {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        } else {
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some(result)
        }
    }

    /// Evicts records up to `up_to_offset` for a given stream (e.g. after compaction or S3 upload).
    pub fn trim(&self, stream_id: u64, up_to_offset: i64) {
        let mut streams = self.streams.write();
        let mut cur_bytes = self.current_bytes.write();

        if let Some(map) = streams.get_mut(&stream_id) {
            while let Some((&first_off, _)) = map.iter().next() {
                if first_off < up_to_offset {
                    let (_, evicted) = map.pop_first().unwrap();
                    *cur_bytes = cur_bytes.saturating_sub(evicted.len());
                } else {
                    break;
                }
            }
        }
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_cache_put_read_hit() {
        let cache = LogCache::new(1024 * 1024);
        cache.put(1, 0, Bytes::from_static(b"rec-0"));
        cache.put(1, 1, Bytes::from_static(b"rec-1"));
        cache.put(1, 2, Bytes::from_static(b"rec-2"));

        let res = cache.read_range(1, 0, 1024).expect("Cache hit");
        assert_eq!(res.len(), 3);
        assert_eq!(res[0].0, 0);
        assert_eq!(res[0].1.as_ref(), b"rec-0");
        assert_eq!(res[2].0, 2);
        assert_eq!(res[2].1.as_ref(), b"rec-2");
        assert_eq!(cache.hit_count(), 1);
        assert_eq!(cache.miss_count(), 0);
    }

    #[test]
    fn test_log_cache_miss() {
        let cache = LogCache::new(1024);
        assert!(cache.read_range(99, 0, 1024).is_none());
        assert_eq!(cache.miss_count(), 1);
    }

    #[test]
    fn test_log_cache_eviction() {
        let cache = LogCache::new(50); // small 50-byte capacity
        let p1 = Bytes::from(vec![0xAA; 25]);
        let p2 = Bytes::from(vec![0xBB; 25]);
        let p3 = Bytes::from(vec![0xCC; 25]);

        cache.put(1, 0, p1);
        cache.put(1, 1, p2);
        assert_eq!(cache.current_size_bytes(), 50);

        // Put third record, should evict p1
        cache.put(1, 2, p3);
        assert!(cache.read_range(1, 0, 100).is_none()); // offset 0 evicted
        let hit = cache.read_range(1, 1, 100).expect("Offset 1 should remain");
        assert_eq!(hit.len(), 2);
    }
}
