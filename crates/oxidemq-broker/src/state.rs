use crate::coordinator::GroupCoordinator;
use crate::router::ClusterState;
use oxidemq_core::error::Result;
use oxidemq_core::types::TopicPartition;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionSnapshot {
    pub topic: String,
    pub partition: i32,
    pub stream_id: u64,
    pub high_watermark: i64,
    pub log_start_offset: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroupSnapshot {
    pub group_id: String,
    pub state: String,
    pub generation_id: i32,
    pub leader_id: Option<String>,
    pub offsets: HashMap<String, i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterStateSnapshot {
    pub node_id: i32,
    pub cluster_id: String,
    pub partitions: Vec<PartitionSnapshot>,
    pub consumer_groups: Vec<ConsumerGroupSnapshot>,
    pub timestamp_ms: u64,
}

impl ClusterStateSnapshot {
    pub fn capture(cluster: &ClusterState, coordinator: &GroupCoordinator) -> Self {
        let partitions = cluster.dump_partition_snapshots();
        let consumer_groups = coordinator.dump_group_snapshots();
        let timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        Self {
            node_id: cluster.node_id(),
            cluster_id: cluster.cluster_id().to_string(),
            partitions,
            consumer_groups,
            timestamp_ms,
        }
    }

    pub fn apply(&self, cluster: &ClusterState, coordinator: &GroupCoordinator) -> Result<()> {
        cluster.reset();
        coordinator.reset();

        for p_snap in &self.partitions {
            let tp = TopicPartition::new(&p_snap.topic, p_snap.partition);
            let _ = cluster.get_or_create_partition(&tp);
        }

        for g_snap in &self.consumer_groups {
            for (tp_str, &offset) in &g_snap.offsets {
                let parts: Vec<&str> = tp_str.split(':').collect();
                if parts.len() == 2 {
                    if let Ok(part_num) = parts[1].parse::<i32>() {
                        let tp = TopicPartition::new(parts[0], part_num);
                        coordinator.commit_offset(&g_snap.group_id, tp, offset);
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxidemq_s3stream::block_cache::BlockCache;
    use oxidemq_s3stream::client::MemoryObjectStorage;
    use oxidemq_s3stream::log_cache::LogCache;
    use oxidemq_wal::memory::MemoryWal;
    use std::sync::Arc;

    #[test]
    fn test_cluster_state_snapshot_roundtrip() {
        let wal = Arc::new(MemoryWal::new());
        let storage = Arc::new(MemoryObjectStorage::new());
        let log_cache = Arc::new(LogCache::new(1024 * 1024));
        let block_cache = Arc::new(BlockCache::new(1024 * 1024));
        let cluster = ClusterState::new(
            1,
            "127.0.0.1",
            9092,
            "test-cluster",
            wal,
            storage,
            log_cache,
            block_cache,
        );
        let coordinator = GroupCoordinator::new();

        let tp = TopicPartition::new("my-topic", 0);
        cluster.get_or_create_partition(&tp);
        coordinator.commit_offset("group-1", tp.clone(), 42);

        let snap = ClusterStateSnapshot::capture(&cluster, &coordinator);
        assert_eq!(snap.partitions.len(), 1);
        assert_eq!(snap.consumer_groups.len(), 1);

        // Apply to a clean cluster
        snap.apply(&cluster, &coordinator).unwrap();
        assert_eq!(cluster.partition_count(), 1);
        assert_eq!(coordinator.fetch_offset("group-1", &tp), Some(42));
    }
}
