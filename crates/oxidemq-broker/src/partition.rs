use bytes::Bytes;
use oxidemq_core::error::Result;
use oxidemq_core::types::TopicPartition;
use oxidemq_s3stream::stream::S3Stream;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Represents a single topic partition backed by an underlying cloud `S3Stream`.
#[derive(Clone)]
pub struct Partition {
    pub topic_partition: TopicPartition,
    pub stream: Arc<S3Stream>,
}

impl Partition {
    pub fn new(topic_partition: TopicPartition, stream: Arc<S3Stream>) -> Self {
        Self {
            topic_partition,
            stream,
        }
    }

    /// Appends incoming batch records to the partition's underlying S3Stream.
    /// Returns `(base_offset, log_append_time_ms)`.
    pub fn append_records(&self, records: Bytes) -> Result<(i64, i64)> {
        let base_offset = self.stream.append(records)?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        Ok((base_offset, now))
    }

    /// Fetches records starting at `fetch_offset` up to `max_bytes`.
    pub fn read_records(&self, fetch_offset: i64, max_bytes: usize) -> Result<Vec<(i64, Bytes)>> {
        self.stream.fetch(fetch_offset, max_bytes)
    }

    pub fn high_watermark(&self) -> i64 {
        self.stream.next_offset()
    }

    pub fn log_start_offset(&self) -> i64 {
        self.stream.start_offset()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxidemq_s3stream::block_cache::BlockCache;
    use oxidemq_s3stream::client::MemoryObjectStorage;
    use oxidemq_s3stream::log_cache::LogCache;
    use oxidemq_wal::memory::MemoryWal;

    #[test]
    fn test_partition_append_fetch() {
        let wal = Arc::new(MemoryWal::new());
        let storage = Arc::new(MemoryObjectStorage::new());
        let log_cache = Arc::new(LogCache::new(1024 * 1024));
        let block_cache = Arc::new(BlockCache::new(1024 * 1024));
        let stream = Arc::new(S3Stream::new(1, 0, wal, storage, log_cache, block_cache));

        let tp = TopicPartition::new("orders", 0);
        let part = Partition::new(tp, stream);

        let (base_off, _) = part.append_records(Bytes::from_static(b"order-1")).unwrap();
        assert_eq!(base_off, 0);
        assert_eq!(part.high_watermark(), 1);

        let records = part.read_records(0, 1024).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].1.as_ref(), b"order-1");
    }
}
