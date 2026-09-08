use crate::stream::S3Stream;
use bytes::Bytes;
use oxidemq_core::error::Result;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing::info;

/// Coordinates the asynchronous background offloading of committed WAL batches into S3.
pub struct BatchUploader {
    total_bytes_uploaded: AtomicU64,
    total_batches_uploaded: AtomicU64,
}

impl Default for BatchUploader {
    fn default() -> Self {
        Self::new()
    }
}

impl BatchUploader {
    pub fn new() -> Self {
        Self {
            total_bytes_uploaded: AtomicU64::new(0),
            total_batches_uploaded: AtomicU64::new(0),
        }
    }

    /// Uploads an accumulated batch of records for a stream into an immutable S3 object.
    pub fn upload(
        &self,
        stream: &Arc<S3Stream>,
        start_offset: i64,
        end_offset: i64,
        records: Vec<Bytes>,
    ) -> Result<String> {
        let count = records.len();
        let bytes: usize = records.iter().map(|r| r.len()).sum();

        let object_key = stream.upload_batch(start_offset, end_offset, records)?;

        self.total_bytes_uploaded
            .fetch_add(bytes as u64, Ordering::Relaxed);
        self.total_batches_uploaded.fetch_add(1, Ordering::Relaxed);

        info!(
            "Uploaded batch to S3 ({:?}, {} records, {} bytes) for stream {}",
            object_key,
            count,
            bytes,
            stream.stream_id()
        );

        Ok(object_key)
    }

    pub fn total_bytes_uploaded(&self) -> u64 {
        self.total_bytes_uploaded.load(Ordering::Relaxed)
    }

    pub fn total_batches_uploaded(&self) -> u64 {
        self.total_batches_uploaded.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_cache::BlockCache;
    use crate::client::MemoryObjectStorage;
    use crate::log_cache::LogCache;
    use oxidemq_wal::memory::MemoryWal;

    #[test]
    fn test_batch_uploader() {
        let storage = Arc::new(MemoryObjectStorage::new());
        let wal = Arc::new(MemoryWal::new());
        let log_cache = Arc::new(LogCache::new(1024 * 1024));
        let block_cache = Arc::new(BlockCache::new(1024 * 1024));

        let stream = Arc::new(S3Stream::new(42, 0, wal, storage, log_cache, block_cache));
        let uploader = BatchUploader::new();

        let records = vec![Bytes::from_static(b"r1"), Bytes::from_static(b"r2")];
        let key = uploader.upload(&stream, 0, 1, records).unwrap();

        assert!(!key.is_empty());
        assert_eq!(uploader.total_batches_uploaded(), 1);
        assert_eq!(uploader.total_bytes_uploaded(), 4);
    }
}
