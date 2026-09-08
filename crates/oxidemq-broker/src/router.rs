use crate::partition::Partition;
use oxidemq_core::types::TopicPartition;
use oxidemq_protocol::messages::{
    BrokerMetadata, MetadataResponse, PartitionMetadata, TopicMetadata,
};
use oxidemq_protocol::KafkaErrorCode;
use oxidemq_s3stream::block_cache::BlockCache;
use oxidemq_s3stream::client::ObjectStorage;
use oxidemq_s3stream::log_cache::LogCache;
use oxidemq_s3stream::stream::S3Stream;
use oxidemq_wal::WalEngine;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Central broker cluster state tracking all active topics, partitions, and S3 streams.
pub struct ClusterState {
    node_id: i32,
    host: String,
    port: i32,
    cluster_id: String,
    partitions: Arc<RwLock<HashMap<TopicPartition, Arc<Partition>>>>,
    wal: Arc<dyn WalEngine>,
    storage: Arc<dyn ObjectStorage>,
    log_cache: Arc<LogCache>,
    block_cache: Arc<BlockCache>,
    next_stream_id: AtomicU64,
}

impl ClusterState {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: i32,
        host: impl Into<String>,
        port: i32,
        cluster_id: impl Into<String>,
        wal: Arc<dyn WalEngine>,
        storage: Arc<dyn ObjectStorage>,
        log_cache: Arc<LogCache>,
        block_cache: Arc<BlockCache>,
    ) -> Self {
        Self {
            node_id,
            host: host.into(),
            port,
            cluster_id: cluster_id.into(),
            partitions: Arc::new(RwLock::new(HashMap::new())),
            wal,
            storage,
            log_cache,
            block_cache,
            next_stream_id: AtomicU64::new(100),
        }
    }

    /// Retrieves an existing partition or automatically provisions one backed by a new S3Stream.
    pub fn get_or_create_partition(&self, tp: &TopicPartition) -> Arc<Partition> {
        let read_guard = self.partitions.read();
        if let Some(p) = read_guard.get(tp) {
            return Arc::clone(p);
        }
        drop(read_guard);

        let mut write_guard = self.partitions.write();
        if let Some(p) = write_guard.get(tp) {
            return Arc::clone(p);
        }

        let stream_id = self.next_stream_id.fetch_add(1, Ordering::SeqCst);
        let stream = Arc::new(S3Stream::new(
            stream_id,
            0,
            Arc::clone(&self.wal),
            Arc::clone(&self.storage),
            Arc::clone(&self.log_cache),
            Arc::clone(&self.block_cache),
        ));

        let partition = Arc::new(Partition::new(tp.clone(), stream));
        write_guard.insert(tp.clone(), Arc::clone(&partition));
        partition
    }

    pub fn get_partition(&self, tp: &TopicPartition) -> Option<Arc<Partition>> {
        self.partitions.read().get(tp).cloned()
    }

    pub fn node_id(&self) -> i32 {
        self.node_id
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self) -> i32 {
        self.port
    }

    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    pub fn partition_count(&self) -> usize {
        self.partitions.read().len()
    }

    /// Constructs standard Kafka `MetadataResponse`.
    pub fn build_metadata(&self, requested_topics: Option<&[String]>) -> MetadataResponse {
        let brokers = vec![BrokerMetadata {
            node_id: self.node_id,
            host: self.host.clone(),
            port: self.port,
            rack: None,
        }];

        let partitions = self.partitions.read();
        let mut topic_map: HashMap<String, Vec<i32>> = HashMap::new();

        for tp in partitions.keys() {
            let should_include = match requested_topics {
                Some(req_t) => req_t.contains(&tp.topic),
                None => true,
            };
            if should_include {
                topic_map
                    .entry(tp.topic.clone())
                    .or_default()
                    .push(tp.partition);
            }
        }

        // If specific topics were requested that don't exist yet, auto-create partition 0
        if let Some(req_t) = requested_topics {
            for t in req_t {
                if !topic_map.contains_key(t) {
                    topic_map.insert(t.clone(), vec![0]);
                }
            }
        }

        let mut topics = Vec::new();
        for (topic_name, p_indices) in topic_map {
            let mut part_metas = Vec::new();
            for p_idx in p_indices {
                part_metas.push(PartitionMetadata {
                    error_code: KafkaErrorCode::None,
                    partition_index: p_idx,
                    leader_id: self.node_id,
                    leader_epoch: 0,
                    replica_nodes: vec![self.node_id],
                    isr_nodes: vec![self.node_id],
                });
            }

            topics.push(TopicMetadata {
                error_code: KafkaErrorCode::None,
                name: topic_name,
                is_internal: false,
                partitions: part_metas,
            });
        }

        MetadataResponse {
            throttle_time_ms: 0,
            brokers,
            cluster_id: Some(self.cluster_id.clone()),
            controller_id: self.node_id,
            topics,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxidemq_s3stream::client::MemoryObjectStorage;
    use oxidemq_wal::memory::MemoryWal;

    #[test]
    fn test_cluster_state_auto_create_partition() {
        let wal = Arc::new(MemoryWal::new());
        let storage = Arc::new(MemoryObjectStorage::new());
        let log_cache = Arc::new(LogCache::new(1024 * 1024));
        let block_cache = Arc::new(BlockCache::new(1024 * 1024));

        let state = ClusterState::new(
            1,
            "127.0.0.1",
            9092,
            "test-cluster",
            wal,
            storage,
            log_cache,
            block_cache,
        );

        let tp = TopicPartition::new("telemetry", 0);
        let p1 = state.get_or_create_partition(&tp);
        let p2 = state.get_or_create_partition(&tp);

        assert_eq!(p1.topic_partition, tp);
        assert_eq!(p1.stream.stream_id(), p2.stream.stream_id());

        let meta = state.build_metadata(Some(&["telemetry".to_string()]));
        assert_eq!(meta.brokers.len(), 1);
        assert_eq!(meta.topics.len(), 1);
        assert_eq!(meta.topics[0].name, "telemetry");
        assert_eq!(meta.topics[0].partitions.len(), 1);
    }
}
