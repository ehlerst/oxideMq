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
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};
use std::sync::Arc;

/// Configuration and metadata registration for an explicitly created topic.
#[derive(Debug, Clone)]
pub struct TopicRegistration {
    pub name: String,
    pub num_partitions: i32,
    pub replication_factor: i16,
    pub configs: HashMap<String, String>,
}

/// Central broker cluster state tracking all active topics, partitions, and S3 streams.
pub struct ClusterState {
    node_id: i32,
    host: RwLock<String>,
    port: AtomicI32,
    cluster_id: String,
    partitions: Arc<RwLock<HashMap<TopicPartition, Arc<Partition>>>>,
    topics: Arc<RwLock<HashMap<String, TopicRegistration>>>,
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
            host: RwLock::new(host.into()),
            port: AtomicI32::new(port),
            cluster_id: cluster_id.into(),
            partitions: Arc::new(RwLock::new(HashMap::new())),
            topics: Arc::new(RwLock::new(HashMap::new())),
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

    /// Dynamically provisions a new topic with the specified partition count, replication factor, and configs.
    pub fn create_topic(
        &self,
        name: &str,
        num_partitions: i32,
        replication_factor: i16,
        configs: HashMap<String, String>,
    ) -> std::result::Result<(), KafkaErrorCode> {
        // 1. Topic name validation
        if name.is_empty() || name == "." || name == ".." || name.len() > 249 {
            return Err(KafkaErrorCode::InvalidTopicException);
        }
        if !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-')
        {
            return Err(KafkaErrorCode::InvalidTopicException);
        }

        // 2. Partition count validation
        let parts = if num_partitions == -1 {
            1
        } else {
            num_partitions
        };
        if parts <= 0 || parts > 10000 {
            return Err(KafkaErrorCode::InvalidPartitions);
        }

        // 3. Replication factor validation
        let rf = if replication_factor == -1 {
            1
        } else {
            replication_factor
        };
        if rf <= 0 {
            return Err(KafkaErrorCode::InvalidReplicationFactor);
        }

        // 4. Duplicate topic check
        {
            let t_guard = self.topics.read();
            if t_guard.contains_key(name) {
                return Err(KafkaErrorCode::TopicAlreadyExists);
            }
            let p_guard = self.partitions.read();
            if p_guard.keys().any(|tp| tp.topic == name) {
                return Err(KafkaErrorCode::TopicAlreadyExists);
            }
        }

        // 5. Pre-provision requested partitions
        for p in 0..parts {
            let tp = TopicPartition::new(name, p);
            let _ = self.get_or_create_partition(&tp);
        }

        // 6. Record topic registration
        self.topics.write().insert(
            name.to_string(),
            TopicRegistration {
                name: name.to_string(),
                num_partitions: parts,
                replication_factor: rf,
                configs,
            },
        );

        Ok(())
    }

    /// Deletes a topic and removes all associated partitions.
    pub fn delete_topic(&self, name: &str) -> std::result::Result<(), KafkaErrorCode> {
        let mut topics_guard = self.topics.write();
        let mut partitions_guard = self.partitions.write();

        let existed_in_topics = topics_guard.remove(name).is_some();
        let mut removed_partitions = false;
        partitions_guard.retain(|tp, _| {
            if tp.topic == name {
                removed_partitions = true;
                false
            } else {
                true
            }
        });

        if !existed_in_topics && !removed_partitions {
            return Err(KafkaErrorCode::UnknownTopicOrPartition);
        }

        Ok(())
    }

    /// Checks if a topic currently exists in the cluster state.
    pub fn has_topic(&self, name: &str) -> bool {
        self.topics.read().contains_key(name)
            || self.partitions.read().keys().any(|tp| tp.topic == name)
    }

    /// Retrieves registered topic metadata if present.
    pub fn get_topic(&self, name: &str) -> Option<TopicRegistration> {
        self.topics.read().get(name).cloned()
    }

    /// Returns a list of all active topic names.
    pub fn list_topics(&self) -> Vec<String> {
        let mut set = std::collections::BTreeSet::new();
        for k in self.topics.read().keys() {
            set.insert(k.clone());
        }
        for tp in self.partitions.read().keys() {
            set.insert(tp.topic.clone());
        }
        set.into_iter().collect()
    }

    pub fn node_id(&self) -> i32 {
        self.node_id
    }

    pub fn host(&self) -> String {
        self.host.read().clone()
    }

    pub fn port(&self) -> i32 {
        self.port.load(Ordering::Relaxed)
    }

    pub fn set_advertised_host(&self, host: impl Into<String>) {
        *self.host.write() = host.into();
    }

    pub fn set_advertised_port(&self, port: i32) {
        self.port.store(port, Ordering::SeqCst);
    }

    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    pub fn partition_count(&self) -> usize {
        self.partitions.read().len()
    }

    pub fn reset(&self) {
        self.topics.write().clear();
        self.partitions.write().clear();
        self.next_stream_id.store(100, Ordering::SeqCst);
    }

    pub fn dump_partition_snapshots(&self) -> Vec<crate::state::PartitionSnapshot> {
        let partitions = self.partitions.read();
        let mut snapshots = Vec::with_capacity(partitions.len());
        for (tp, part) in partitions.iter() {
            snapshots.push(crate::state::PartitionSnapshot {
                topic: tp.topic.clone(),
                partition: tp.partition,
                stream_id: part.stream.stream_id(),
                high_watermark: part.high_watermark(),
                log_start_offset: part.log_start_offset(),
            });
        }
        snapshots
    }

    /// Constructs standard Kafka `MetadataResponse` with default auto-creation enabled.
    pub fn build_metadata(&self, requested_topics: Option<&[String]>) -> MetadataResponse {
        self.build_metadata_with_auto_create(requested_topics, true)
    }

    /// Constructs standard Kafka `MetadataResponse` allowing fine-grained control over auto-creation.
    pub fn build_metadata_with_auto_create(
        &self,
        requested_topics: Option<&[String]>,
        allow_auto_create: bool,
    ) -> MetadataResponse {
        let brokers = vec![BrokerMetadata {
            node_id: self.node_id,
            host: self.host(),
            port: self.port(),
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
        drop(partitions);

        let mut not_found_topics = Vec::new();
        if let Some(req_t) = requested_topics {
            for t in req_t {
                if !topic_map.contains_key(t) {
                    if allow_auto_create {
                        let tp = TopicPartition::new(t, 0);
                        let _ = self.get_or_create_partition(&tp);
                        topic_map.insert(t.clone(), vec![0]);
                    } else {
                        not_found_topics.push(t.clone());
                    }
                }
            }
        }

        let mut topics = Vec::new();
        for (topic_name, mut p_indices) in topic_map {
            p_indices.sort_unstable();
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

        for nf_topic in not_found_topics {
            topics.push(TopicMetadata {
                error_code: KafkaErrorCode::UnknownTopicOrPartition,
                name: nf_topic,
                is_internal: false,
                partitions: Vec::new(),
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

        assert!(state.get_partition(&tp).is_some());
        assert!(state
            .get_partition(&TopicPartition::new("non-existent", 0))
            .is_none());

        assert_eq!(state.node_id(), 1);
        assert_eq!(state.host(), "127.0.0.1");
        assert_eq!(state.port(), 9092);
        assert_eq!(state.cluster_id(), "test-cluster");
        assert_eq!(state.partition_count(), 1);

        state.set_advertised_host("broker-new.com");
        state.set_advertised_port(9095);
        assert_eq!(state.host(), "broker-new.com");
        assert_eq!(state.port(), 9095);

        let snapshots = state.dump_partition_snapshots();
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].topic, "telemetry");

        let all_meta = state.build_metadata(None);
        assert_eq!(all_meta.topics.len(), 1);

        state.reset();
        assert_eq!(state.partition_count(), 0);
    }

    #[test]
    fn test_dynamic_topic_create_delete_and_metadata() {
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

        // 1. Create topic with 3 partitions and configs
        let mut configs = HashMap::new();
        configs.insert("retention.ms".into(), "60000".into());
        let res = state.create_topic("orders", 3, 1, configs);
        assert_eq!(res, Ok(()));
        assert!(state.has_topic("orders"));
        assert_eq!(state.partition_count(), 3);

        let topic_meta = state.get_topic("orders").unwrap();
        assert_eq!(topic_meta.num_partitions, 3);
        assert_eq!(topic_meta.replication_factor, 1);
        assert_eq!(
            topic_meta.configs.get("retention.ms").map(String::as_str),
            Some("60000")
        );

        // 2. Query metadata for "orders"
        let meta = state.build_metadata(Some(&["orders".to_string()]));
        assert_eq!(meta.topics.len(), 1);
        assert_eq!(meta.topics[0].name, "orders");
        assert_eq!(meta.topics[0].error_code, KafkaErrorCode::None);
        assert_eq!(meta.topics[0].partitions.len(), 3);
        assert_eq!(meta.topics[0].partitions[0].partition_index, 0);
        assert_eq!(meta.topics[0].partitions[1].partition_index, 1);
        assert_eq!(meta.topics[0].partitions[2].partition_index, 2);

        // 3. Validation errors
        assert_eq!(
            state.create_topic("", 1, 1, HashMap::new()),
            Err(KafkaErrorCode::InvalidTopicException)
        );
        assert_eq!(
            state.create_topic(".", 1, 1, HashMap::new()),
            Err(KafkaErrorCode::InvalidTopicException)
        );
        assert_eq!(
            state.create_topic("invalid topic spaces!", 1, 1, HashMap::new()),
            Err(KafkaErrorCode::InvalidTopicException)
        );
        assert_eq!(
            state.create_topic("orders", 1, 1, HashMap::new()),
            Err(KafkaErrorCode::TopicAlreadyExists)
        );
        assert_eq!(
            state.create_topic("new-topic", 0, 1, HashMap::new()),
            Err(KafkaErrorCode::InvalidPartitions)
        );
        assert_eq!(
            state.create_topic("new-topic", 1, 0, HashMap::new()),
            Err(KafkaErrorCode::InvalidReplicationFactor)
        );

        // 4. Query metadata without auto-create for non-existent topic
        let non_existent_meta =
            state.build_metadata_with_auto_create(Some(&["nonexistent".to_string()]), false);
        assert_eq!(non_existent_meta.topics.len(), 1);
        assert_eq!(non_existent_meta.topics[0].name, "nonexistent");
        assert_eq!(
            non_existent_meta.topics[0].error_code,
            KafkaErrorCode::UnknownTopicOrPartition
        );
        assert_eq!(non_existent_meta.topics[0].partitions.len(), 0);

        // 5. Delete topic
        let del_res = state.delete_topic("orders");
        assert_eq!(del_res, Ok(()));
        assert!(!state.has_topic("orders"));
        assert_eq!(state.partition_count(), 0);

        // 6. Second delete should fail with UnknownTopicOrPartition
        let del_res2 = state.delete_topic("orders");
        assert_eq!(del_res2, Err(KafkaErrorCode::UnknownTopicOrPartition));
    }
}
