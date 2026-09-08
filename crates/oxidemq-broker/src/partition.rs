use bytes::{Bytes, BytesMut};
use oxidemq_core::error::Result;
use oxidemq_core::types::TopicPartition;
use oxidemq_s3stream::stream::S3Stream;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

pub type ProducerEntry = (i16, i32, i64);
pub type ProducerStateMap = Arc<RwLock<HashMap<i64, ProducerEntry>>>;

/// Represents a single topic partition backed by an underlying cloud `S3Stream`.
#[derive(Clone)]
pub struct Partition {
    pub topic_partition: TopicPartition,
    pub stream: Arc<S3Stream>,
    producer_states: ProducerStateMap,
    append_lock: Arc<Mutex<()>>,
}

fn patch_record_batches(records: Bytes, base_offset: i64) -> (Bytes, i64) {
    if records.len() < 21 {
        return (records, 1);
    }
    let mut buf = BytesMut::from(records.as_ref());
    let mut cursor = 0;
    let mut current_offset = base_offset;

    while cursor + 21 <= buf.len() {
        if buf[cursor + 16] == 2 {
            // Kafka RecordBatch v2
            let batch_len =
                i32::from_be_bytes(buf[cursor + 8..cursor + 12].try_into().unwrap()) as usize;
            let total_batch_size = 12 + batch_len;
            if cursor + total_batch_size > buf.len() {
                break;
            }

            // Patch base offset
            buf[cursor..cursor + 8].copy_from_slice(&current_offset.to_be_bytes());

            // Recompute CRC32C over attributes to end of batch (byte 21 to total_batch_size)
            let crc = oxidemq_core::compute_crc32c(&buf[cursor + 21..cursor + total_batch_size]);
            buf[cursor + 17..cursor + 21].copy_from_slice(&crc.to_be_bytes());

            // Count records from last_offset_delta
            let count = if cursor + 27 <= buf.len() {
                let delta = i32::from_be_bytes(buf[cursor + 23..cursor + 27].try_into().unwrap());
                (delta as i64).max(0) + 1
            } else {
                1
            };
            current_offset += count;
            cursor += total_batch_size;
        } else {
            break;
        }
    }

    let total_records = (current_offset - base_offset).max(1);
    (buf.freeze(), total_records)
}

impl Partition {
    pub fn new(topic_partition: TopicPartition, stream: Arc<S3Stream>) -> Self {
        Self {
            topic_partition,
            stream,
            producer_states: Arc::new(RwLock::new(HashMap::new())),
            append_lock: Arc::new(Mutex::new(())),
        }
    }

    /// Appends incoming batch records to the partition's underlying S3Stream.
    /// Returns `(base_offset, log_append_time_ms)`.
    pub fn append_records(&self, records: Bytes) -> Result<(i64, i64)> {
        let _guard = self.append_lock.lock();
        let base_offset = self.stream.next_offset();
        let (patched_records, count) = patch_record_batches(records, base_offset);
        self.stream.append_with_count(patched_records, count)?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        Ok((base_offset, now))
    }

    /// Appends incoming batch records with Kafka idempotent producer semantics.
    /// Deduplicates previously processed sequence numbers and verifies strict ordering.
    /// Returns `(base_offset, log_append_time_ms, is_duplicate)`.
    pub fn append_idempotent(
        &self,
        producer_id: i64,
        producer_epoch: i16,
        sequence: i32,
        records: Bytes,
    ) -> Result<(i64, i64, bool)> {
        let mut states = self.producer_states.write();
        if let Some(&(last_epoch, last_seq, last_offset)) = states.get(&producer_id) {
            if producer_epoch < last_epoch {
                return Err(oxidemq_core::error::OxideMqError::Protocol(format!(
                    "Fenced producer {}: epoch {} < active {}",
                    producer_id, producer_epoch, last_epoch
                )));
            }

            if producer_epoch == last_epoch {
                if sequence <= last_seq {
                    // Duplicate batch re-sent by client: return existing offset without re-appending
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_millis() as i64)
                        .unwrap_or(0);
                    return Ok((last_offset, now, true));
                }

                if sequence > last_seq + 1 {
                    return Err(oxidemq_core::error::OxideMqError::Protocol(format!(
                        "Out of order sequence for producer {}: expected {}, got {}",
                        producer_id,
                        last_seq + 1,
                        sequence
                    )));
                }
            }
        }

        // Valid next sequence or new producer
        let (base_offset, now) = self.append_records(records)?;
        states.insert(producer_id, (producer_epoch, sequence, base_offset));
        Ok((base_offset, now, false))
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

    #[test]
    fn test_idempotent_producer_deduplication() {
        let wal = Arc::new(MemoryWal::new());
        let storage = Arc::new(MemoryObjectStorage::new());
        let log_cache = Arc::new(LogCache::new(1024 * 1024));
        let block_cache = Arc::new(BlockCache::new(1024 * 1024));
        let stream = Arc::new(S3Stream::new(1, 0, wal, storage, log_cache, block_cache));

        let tp = TopicPartition::new("idempotent-topic", 0);
        let part = Partition::new(tp, stream);

        let pid = 10001i64;
        let epoch = 1i16;

        // 1. Initial sequence 0: must succeed and append
        let (off0, _, is_dup0) = part
            .append_idempotent(pid, epoch, 0, Bytes::from_static(b"msg-0"))
            .unwrap();
        assert_eq!(off0, 0);
        assert!(!is_dup0);
        assert_eq!(part.high_watermark(), 1);

        // 2. Duplicate sequence 0 retry: must succeed, return duplicate flag true, without advancing watermark
        let (dup_off0, _, is_dup_retry) = part
            .append_idempotent(pid, epoch, 0, Bytes::from_static(b"msg-0-retry"))
            .unwrap();
        assert_eq!(dup_off0, 0);
        assert!(is_dup_retry, "Should be flagged as duplicate");
        assert_eq!(
            part.high_watermark(),
            1,
            "Watermark must not advance on duplicate"
        );

        // 3. Next contiguous sequence 1: must succeed
        let (off1, _, is_dup1) = part
            .append_idempotent(pid, epoch, 1, Bytes::from_static(b"msg-1"))
            .unwrap();
        assert_eq!(off1, 1);
        assert!(!is_dup1);
        assert_eq!(part.high_watermark(), 2);

        // 4. Out-of-order sequence (skip 2, send 5): must fail with out of order error
        let err = part.append_idempotent(pid, epoch, 5, Bytes::from_static(b"msg-5"));
        assert!(err.is_err(), "Out of order sequence must be rejected");

        // 5. Stale producer epoch (send epoch 0 when active is 1): must be rejected
        let stale_err = part.append_idempotent(pid, 0, 2, Bytes::from_static(b"msg-stale"));
        assert!(stale_err.is_err(), "Stale producer epoch must be fenced");

        assert_eq!(part.log_start_offset(), 0);
    }

    #[test]
    fn test_patch_record_batches_v2() {
        use bytes::BufMut;
        let mut raw = BytesMut::new();
        let batch_payload_len = 49; // from byte 12 to 61
        raw.put_i64(0); // base_offset
        raw.put_i32(batch_payload_len); // batch_len
        raw.put_i32(0); // leader epoch
        raw.put_u8(2); // magic = 2
        raw.put_u32(0); // crc
        raw.put_i16(0); // attributes
        raw.put_i32(2); // last_offset_delta (meaning 3 records: delta 2)
        raw.put_i64(1000); // base_ts
        raw.put_i64(1000); // max_ts
        raw.put_i64(-1); // producer_id
        raw.put_i16(-1); // producer_epoch
        raw.put_i32(-1); // base_seq
        raw.put_i32(3); // count

        let (patched, count) = patch_record_batches(raw.freeze(), 100);
        assert_eq!(count, 3);
        let base_off = i64::from_be_bytes(patched[0..8].try_into().unwrap());
        assert_eq!(base_off, 100);
        let crc = u32::from_be_bytes(patched[17..21].try_into().unwrap());
        assert_ne!(crc, 0);
    }
}
