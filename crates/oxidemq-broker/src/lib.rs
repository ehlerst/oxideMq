//! # oxideMq Broker
//!
//! Stateless Kafka broker engine mapping Kafka topic partitions to underlying S3Streams
//! and managing consumer group coordination.

pub mod acl;
pub mod chaos;
pub mod coordinator;
pub mod handler;
pub mod partition;
pub mod router;
pub mod sasl;
pub mod schema_registry;
pub mod state;
pub mod transaction;

pub use acl::{AclAuthorizer, AclBinding};

pub use chaos::{ChaosEngine, ChaosRule, FaultTarget};
pub use coordinator::{ConsumerGroup, GroupCoordinator, GroupMember, GroupState};
pub use handler::BrokerEngine;
pub use partition::Partition;
pub use router::ClusterState;
pub use sasl::{ConnectionAuthState, SaslAuthenticator, SaslMechanism, ScramServerSession};
pub use schema_registry::{
    CompatibilityLevel, SchemaEntry, SchemaReference, SchemaRegistry, SchemaRegistryError,
    SchemaType,
};
pub use state::{ClusterStateSnapshot, ConsumerGroupSnapshot, PartitionSnapshot};
pub use transaction::{TransactionCoordinator, TransactionMetadata, TransactionState};
