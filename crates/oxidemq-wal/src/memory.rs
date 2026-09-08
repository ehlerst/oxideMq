use crate::frame::WalRecord;
use crate::WalEngine;
use bytes::Bytes;
use oxidemq_core::error::Result;
use parking_lot::RwLock;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// A blazing-fast, pure in-memory WAL implementation for Tier 1 testing and microsecond latency simulation.
#[derive(Debug, Default)]
pub struct MemoryWal {
    next_seq: AtomicU64,
    records: Arc<RwLock<BTreeMap<u64, WalRecord>>>,
    stream_indexes: Arc<RwLock<HashMap<u64, Vec<u64>>>>, // stream_id -> [seq]
}

impl MemoryWal {
    pub fn new() -> Self {
        Self {
            next_seq: AtomicU64::new(1),
            records: Arc::new(RwLock::new(BTreeMap::new())),
            stream_indexes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Read records for a given stream starting from `start_offset` up to `max_bytes`.
    pub fn read_stream(
        &self,
        stream_id: u64,
        start_offset: i64,
        max_bytes: usize,
    ) -> Vec<WalRecord> {
        let stream_idx = self.stream_indexes.read();
        let records = self.records.read();

        let mut result = Vec::new();
        let mut total_bytes = 0;

        if let Some(seqs) = stream_idx.get(&stream_id) {
            for &seq in seqs {
                if let Some(rec) = records.get(&seq) {
                    if rec.offset >= start_offset {
                        total_bytes += rec.payload.len();
                        result.push(rec.clone());
                        if total_bytes >= max_bytes {
                            break;
                        }
                    }
                }
            }
        }

        result
    }

    /// Returns the total count of records stored in memory.
    pub fn len(&self) -> usize {
        self.records.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.read().is_empty()
    }
}

impl WalEngine for MemoryWal {
    fn append(&self, stream_id: u64, offset: i64, data: &[u8]) -> Result<u64> {
        let seq = self.next_seq.fetch_add(1, Ordering::SeqCst);
        let record = WalRecord::new(seq, stream_id, offset, Bytes::copy_from_slice(data));

        {
            let mut recs = self.records.write();
            recs.insert(seq, record);
        }

        {
            let mut s_idx = self.stream_indexes.write();
            s_idx.entry(stream_id).or_default().push(seq);
        }

        Ok(seq)
    }

    fn flush(&self) -> Result<()> {
        // Memory WAL is always immediately durable in RAM
        Ok(())
    }

    fn trim(&self, stream_id: u64, up_to_offset: i64) -> Result<()> {
        let mut s_idx = self.stream_indexes.write();
        let mut recs = self.records.write();

        if let Some(seqs) = s_idx.get_mut(&stream_id) {
            let mut retained_seqs = Vec::new();
            for &seq in seqs.iter() {
                if let Some(rec) = recs.get(&seq) {
                    if rec.offset < up_to_offset {
                        recs.remove(&seq);
                    } else {
                        retained_seqs.push(seq);
                    }
                }
            }
            *seqs = retained_seqs;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_wal_lifecycle() {
        let wal = MemoryWal::new();
        let seq1 = wal.append(1, 0, b"first").unwrap();
        let seq2 = wal.append(1, 1, b"second").unwrap();
        let seq3 = wal.append(2, 0, b"stream2-item").unwrap();

        assert_eq!(seq1, 1);
        assert_eq!(seq2, 2);
        assert_eq!(seq3, 3);
        assert_eq!(wal.len(), 3);

        let stream1_records = wal.read_stream(1, 0, 1024);
        assert_eq!(stream1_records.len(), 2);
        assert_eq!(stream1_records[0].payload.as_ref(), b"first");
        assert_eq!(stream1_records[1].payload.as_ref(), b"second");

        // Trim offset 0
        wal.trim(1, 1).unwrap();
        let remaining = wal.read_stream(1, 0, 1024);
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].offset, 1);
    }
}
