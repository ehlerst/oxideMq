//! # oxideMq Broker
//!
//! Stateless Kafka broker engine mapping Kafka topic partitions to underlying S3Streams
//! and managing consumer group coordination.

pub mod chaos;
pub mod coordinator;
pub mod handler;
pub mod partition;
pub mod router;
pub mod state;
pub mod transaction;

pub use chaos::{ChaosEngine, ChaosRule, FaultTarget};
pub use coordinator::{ConsumerGroup, GroupCoordinator, GroupMember, GroupState};
pub use handler::BrokerEngine;
pub use partition::Partition;
pub use router::ClusterState;
pub use state::{ClusterStateSnapshot, ConsumerGroupSnapshot, PartitionSnapshot};
pub use transaction::{TransactionCoordinator, TransactionMetadata, TransactionState};
