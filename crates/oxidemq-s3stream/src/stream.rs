use crate::block_cache::{BlockCache, BlockKey};
use crate::client::ObjectStorage;
use crate::format::{S3DataBlock, S3ObjectCodec};
use crate::log_cache::LogCache;
use crate::S3StreamStorage;
use bytes::Bytes;
use oxidemq_core::error::{OxideMqError, Result};
use oxidemq_wal::WalEngine;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

/// Tracks metadata of an uploaded S3 object associated with a stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3ObjectMeta {
    pub key: String,
    pub start_offset: i64,
    pub end_offset: i64,
    pub record_count: u32,
    pub size_bytes: usize,
}

/// A decoupled, cloud-native streaming storage stream backed by WAL and S3.
pub struct S3Stream {
    pub(crate) stream_id: u64,
    pub(crate) start_offset: AtomicI64,
    pub(crate) next_offset: AtomicI64,
    pub(crate) wal: Arc<dyn WalEngine>,
    pub(crate) storage: Arc<dyn ObjectStorage>,
    pub(crate) log_cache: Arc<LogCache>,
    pub(crate) block_cache: Arc<BlockCache>,
    pub(crate) s3_objects: Arc<RwLock<Vec<S3ObjectMeta>>>,
}

impl S3Stream {
    pub fn stream_id(&self) -> u64 {
        self.stream_id
    }

    pub fn start_offset(&self) -> i64 {
        self.start_offset.load(Ordering::Relaxed)
    }

    pub fn next_offset(&self) -> i64 {
        self.next_offset.load(Ordering::Relaxed)
    }
    pub fn new(
        stream_id: u64,
        start_offset: i64,
        wal: Arc<dyn WalEngine>,
        storage: Arc<dyn ObjectStorage>,
        log_cache: Arc<LogCache>,
        block_cache: Arc<BlockCache>,
    ) -> Self {
        Self {
            stream_id,
            start_offset: AtomicI64::new(start_offset),
            next_offset: AtomicI64::new(start_offset),
            wal,
            storage,
            log_cache,
            block_cache,
            s3_objects: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Appends a payload to the stream, advancing the watermark by `record_count`.
    pub fn append_with_count(&self, payload: Bytes, record_count: i64) -> Result<i64> {
        let count = record_count.max(1);
        let offset = self.next_offset.fetch_add(count, Ordering::SeqCst);

        // Durability: write to WAL
        self.wal.append(self.stream_id, offset, &payload)?;

        // Caching: store in Tier 1 LogCache for zero-latency tailing reads
        self.log_cache.put(self.stream_id, offset, payload);

        Ok(offset)
    }

    /// Appends a payload to the stream.
    /// Acknowledged as soon as it is committed to the WAL, and placed in the LogCache for instant tail reads.
    pub fn append(&self, payload: Bytes) -> Result<i64> {
        self.append_with_count(payload, 1)
    }

    /// Fetches records from the stream starting at `start_offset` up to `max_bytes`.
    /// 1. Tries Tier 1 LogCache (< 200 µs fast path).
    /// 2. If missed, tries Tier 2 BlockCache.
    /// 3. If missed, fetches object from S3 / ObjectStorage and populates BlockCache.
    pub fn fetch(&self, start_offset: i64, max_bytes: usize) -> Result<Vec<(i64, Bytes)>> {
        let current_start = self.start_offset.load(Ordering::Relaxed);
        let next_off = self.next_offset.load(Ordering::Relaxed);

        if start_offset < current_start || start_offset >= next_off {
            return Ok(Vec::new());
        }

        // Fast path 1: Tier 1 LogCache
        if let Some(cached_records) =
            self.log_cache
                .read_range(self.stream_id, start_offset, max_bytes)
        {
            return Ok(cached_records);
        }

        // Path 2 & 3: Historical read from S3 / BlockCache
        let s3_objs = self.s3_objects.read();
        for meta in s3_objs.iter() {
            if start_offset >= meta.start_offset && start_offset <= meta.end_offset {
                let block_key = BlockKey::new(&meta.key, self.stream_id, meta.start_offset);

                // Check Tier 2 BlockCache
                let block_data = if let Some(cached_bytes) = self.block_cache.get(&block_key) {
                    cached_bytes
                } else {
                    // Fetch from ObjectStorage (S3)
                    let raw_obj = self.storage.get_object(&meta.key)?;
                    let decoded_blocks = S3ObjectCodec::decode_all(&raw_obj)?;

                    let mut found_data = None;
                    for b in decoded_blocks {
                        let key = BlockKey::new(&meta.key, b.stream_id, b.start_offset);
                        self.block_cache.put(key, b.data.clone());
                        if b.stream_id == self.stream_id && b.start_offset == meta.start_offset {
                            found_data = Some(b.data);
                        }
                    }

                    found_data.ok_or_else(|| {
                        OxideMqError::Storage(format!("Block not found in S3 object {}", meta.key))
                    })?
                };

                return Ok(vec![(meta.start_offset, block_data)]);
            }
        }

        Ok(Vec::new())
    }

    /// Uploads an accumulated batch of stream records to S3, creating an immutable S3 Data Object.
    pub fn upload_batch(
        &self,
        start_offset: i64,
        end_offset: i64,
        records: Vec<Bytes>,
    ) -> Result<String> {
        if records.is_empty() {
            return Ok(String::new());
        }

        let record_count = records.len() as u32;
        let mut combined_payload = Vec::new();
        for r in records {
            combined_payload.extend_from_slice(&r);
        }

        let block = S3DataBlock::new(
            self.stream_id,
            start_offset,
            end_offset,
            record_count,
            Bytes::from(combined_payload),
        );

        let object_payload = S3ObjectCodec::encode(&[block]);
        let object_key = format!(
            "streams/{}/{:020}_{:020}.data",
            self.stream_id, start_offset, end_offset
        );

        let size_bytes = object_payload.len();
        self.storage.put_object(&object_key, object_payload)?;

        {
            let mut s3_objs = self.s3_objects.write();
            s3_objs.push(S3ObjectMeta {
                key: object_key.clone(),
                start_offset,
                end_offset,
                record_count,
                size_bytes,
            });
        }

        // Durability confirmed in S3: Trim WAL up to start_offset
        let _ = self.wal.trim(self.stream_id, start_offset);

        Ok(object_key)
    }

    /// Returns the number of immutable S3 objects registered for this stream.
    pub fn s3_object_count(&self) -> usize {
        self.s3_objects.read().len()
    }
}

impl S3StreamStorage for S3Stream {
    fn stream_id(&self) -> u64 {
        self.stream_id
    }

    fn start_offset(&self) -> i64 {
        self.start_offset.load(Ordering::Relaxed)
    }

    fn next_offset(&self) -> i64 {
        self.next_offset.load(Ordering::Relaxed)
    }

    fn trim(&self, new_start_offset: i64) -> Result<()> {
        self.start_offset.store(new_start_offset, Ordering::SeqCst);
        self.log_cache.trim(self.stream_id, new_start_offset);
        self.wal.trim(self.stream_id, new_start_offset)?;

        // Prune obsolete S3 objects
        let mut s3_objs = self.s3_objects.write();
        let mut retained = Vec::new();
        let mut to_delete = Vec::new();

        for meta in s3_objs.drain(..) {
            if meta.end_offset < new_start_offset {
                to_delete.push(meta.key);
            } else {
                retained.push(meta);
            }
        }

        *s3_objs = retained;
        if !to_delete.is_empty() {
            self.storage.delete_objects(&to_delete)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::MemoryObjectStorage;
    use crate::S3StreamStorage;
    use oxidemq_wal::memory::MemoryWal;

    #[test]
    fn test_s3_stream_append_and_tail_fetch() {
        let wal = Arc::new(MemoryWal::new());
        let storage = Arc::new(MemoryObjectStorage::new());
        let log_cache = Arc::new(LogCache::new(1024 * 1024));
        let block_cache = Arc::new(BlockCache::new(1024 * 1024));

        let stream = S3Stream::new(100, 0, wal, storage, log_cache, block_cache);

        let off0 = stream.append(Bytes::from_static(b"message-0")).unwrap();
        let off1 = stream.append(Bytes::from_static(b"message-1")).unwrap();

        assert_eq!(off0, 0);
        assert_eq!(off1, 1);
        assert_eq!(stream.next_offset(), 2);

        // Fetch from LogCache (tailing read)
        let fetched = stream.fetch(0, 1024).unwrap();
        assert_eq!(fetched.len(), 2);
        assert_eq!(fetched[0].1.as_ref(), b"message-0");
        assert_eq!(fetched[1].1.as_ref(), b"message-1");
    }

    #[test]
    fn test_s3_stream_upload_and_cold_fetch() {
        let wal = Arc::new(MemoryWal::new());
        let storage = Arc::new(MemoryObjectStorage::new());
        let log_cache = Arc::new(LogCache::new(1024 * 1024));
        let block_cache = Arc::new(BlockCache::new(1024 * 1024));

        let stream = S3Stream::new(200, 0, wal, storage.clone(), log_cache.clone(), block_cache);

        // Upload batch to S3
        let records = vec![Bytes::from_static(b"cold-data-payload")];
        let obj_key = stream.upload_batch(0, 0, records).unwrap();
        assert!(!obj_key.is_empty());
        assert_eq!(storage.put_count(), 1);

        // Evict LogCache to force cold S3 read
        log_cache.trim(200, 100);

        // Fetch should now hit S3 and populate BlockCache
        stream.next_offset.store(1, Ordering::SeqCst);
        let fetched = stream.fetch(0, 1024).unwrap();
        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0].1.as_ref(), b"cold-data-payload");
        assert_eq!(storage.get_count(), 1);

        // Second fetch hits BlockCache without calling S3 get
        let fetched2 = stream.fetch(0, 1024).unwrap();
        assert_eq!(fetched2.len(), 1);
        assert_eq!(storage.get_count(), 1); // No new S3 get!

        // Test stream accessors
        assert_eq!(stream.stream_id(), 200);
        assert_eq!(stream.start_offset(), 0);

        // Fetch out of range (greater than next_offset)
        let empty_fetch = stream.fetch(999, 1024).unwrap();
        assert!(empty_fetch.is_empty());

        // Test empty upload_batch
        let empty_key = stream.upload_batch(10, 10, vec![]).unwrap();
        assert!(empty_key.is_empty());

        // Test trim
        stream.trim(1).unwrap();
        assert_eq!(stream.start_offset(), 1);
    }

    #[test]
    fn test_s3_stream_append_with_count() {
        let wal = Arc::new(MemoryWal::new());
        let storage = Arc::new(MemoryObjectStorage::new());
        let log_cache = Arc::new(LogCache::new(1024 * 1024));
        let block_cache = Arc::new(BlockCache::new(1024 * 1024));

        let stream = S3Stream::new(300, 0, wal, storage, log_cache, block_cache);
        let off = stream
            .append_with_count(Bytes::from_static(b"batch-of-5"), 5)
            .unwrap();
        assert_eq!(off, 0);
        assert_eq!(stream.next_offset(), 5);
    }
}
