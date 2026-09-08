use crate::client::ObjectStorage;
use crate::format::{S3DataBlock, S3ObjectCodec};
use crate::stream::{S3ObjectMeta, S3Stream};
use oxidemq_core::error::Result;
use std::sync::Arc;
use tracing::info;

/// Background compactor that merges small S3 data objects into larger, cost-optimized objects.
pub struct StreamCompactor {
    storage: Arc<dyn ObjectStorage>,
    target_object_size: usize,
}

impl StreamCompactor {
    pub fn new(storage: Arc<dyn ObjectStorage>, target_object_size: usize) -> Self {
        Self {
            storage,
            target_object_size,
        }
    }

    /// Compacts small S3 objects for a stream if there are 2 or more eligible objects.
    pub fn compact_stream(&self, stream: &S3Stream) -> Result<Option<S3ObjectMeta>> {
        let objects: Vec<S3ObjectMeta> = {
            // Check if stream has multiple objects smaller than target
            let s3_objs = stream.s3_objects.read();
            s3_objs
                .iter()
                .filter(|m| m.size_bytes < self.target_object_size)
                .cloned()
                .collect()
        };

        if objects.len() < 2 {
            return Ok(None);
        }

        let mut merged_blocks: Vec<S3DataBlock> = Vec::new();
        let mut keys_to_delete = Vec::new();
        let mut min_offset = i64::MAX;
        let mut max_offset = i64::MIN;
        let mut total_records = 0;

        for meta in &objects {
            let raw_data = self.storage.get_object(&meta.key)?;
            let blocks = S3ObjectCodec::decode_all(&raw_data)?;

            for block in blocks {
                if block.start_offset < min_offset {
                    min_offset = block.start_offset;
                }
                if block.end_offset > max_offset {
                    max_offset = block.end_offset;
                }
                total_records += block.record_count;
                merged_blocks.push(block);
            }

            keys_to_delete.push(meta.key.clone());
        }

        if merged_blocks.is_empty() {
            return Ok(None);
        }

        // Encode merged object
        let merged_bytes = S3ObjectCodec::encode(&merged_blocks);
        let compacted_key = format!(
            "streams/{}/{:020}_{:020}.compacted.data",
            stream.stream_id(),
            min_offset,
            max_offset
        );

        let size_bytes = merged_bytes.len();
        self.storage.put_object(&compacted_key, merged_bytes)?;

        let compacted_meta = S3ObjectMeta {
            key: compacted_key,
            start_offset: min_offset,
            end_offset: max_offset,
            record_count: total_records,
            size_bytes,
        };

        // Update stream metadata: replace old small objects with compacted object
        {
            let mut s3_objs = stream.s3_objects.write();
            s3_objs.retain(|m| !keys_to_delete.contains(&m.key));
            s3_objs.push(compacted_meta.clone());
            s3_objs.sort_by_key(|k| k.start_offset);
        }

        // Delete obsolete small objects from S3
        self.storage.delete_objects(&keys_to_delete)?;

        info!(
            "Compacted {} small S3 objects into {:?} ({} bytes, {} records) for stream {}",
            objects.len(),
            compacted_meta.key,
            compacted_meta.size_bytes,
            compacted_meta.record_count,
            stream.stream_id()
        );

        Ok(Some(compacted_meta))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_cache::BlockCache;
    use crate::client::MemoryObjectStorage;
    use crate::log_cache::LogCache;
    use bytes::Bytes;
    use oxidemq_wal::memory::MemoryWal;

    #[test]
    fn test_stream_compactor() {
        let storage = Arc::new(MemoryObjectStorage::new());
        let wal = Arc::new(MemoryWal::new());
        let log_cache = Arc::new(LogCache::new(1024 * 1024));
        let block_cache = Arc::new(BlockCache::new(1024 * 1024));

        let stream = S3Stream::new(1, 0, wal, storage.clone(), log_cache, block_cache);

        // Upload two small batches
        stream
            .upload_batch(0, 4, vec![Bytes::from_static(b"data-0-4")])
            .unwrap();
        stream
            .upload_batch(5, 9, vec![Bytes::from_static(b"data-5-9")])
            .unwrap();

        assert_eq!(stream.s3_object_count(), 2);
        assert_eq!(storage.list_objects("streams/1").unwrap().len(), 2);

        let compactor = StreamCompactor::new(storage.clone(), 64 * 1024 * 1024);
        let compacted = compactor
            .compact_stream(&stream)
            .unwrap()
            .expect("Compacted");

        assert_eq!(compacted.start_offset, 0);
        assert_eq!(compacted.end_offset, 9);
        assert_eq!(stream.s3_object_count(), 1);

        // Old small objects deleted from S3, 1 compacted object remains
        let remaining_objects = storage.list_objects("streams/1").unwrap();
        assert_eq!(remaining_objects.len(), 1);
        assert!(remaining_objects[0].contains("compacted"));
    }
}
