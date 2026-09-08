//! # oxideMq Broker
//!
//! Stateless Kafka broker engine mapping Kafka topic partitions to underlying S3Streams
//! and managing consumer group coordination.

use oxidemq_core::types::TopicPartition;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// Broker partition router mapping topic partitions to stream IDs.
#[derive(Debug, Default)]
pub struct PartitionRouter {
    routes: Arc<RwLock<HashMap<TopicPartition, u64>>>,
}

impl PartitionRouter {
    pub fn new() -> Self {
        Self {
            routes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn register(&self, tp: TopicPartition, stream_id: u64) {
        self.routes.write().insert(tp, stream_id);
    }

    pub fn get_stream_id(&self, tp: &TopicPartition) -> Option<u64> {
        self.routes.read().get(tp).copied()
    }
}
