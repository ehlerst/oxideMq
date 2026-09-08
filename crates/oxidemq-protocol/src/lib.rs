//! # oxideMq Protocol
//!
//! Pure-Rust, zero-allocation binary Kafka wire protocol parser and serializer.

pub mod codec;
pub mod error_code;
pub mod header;
pub mod messages;
pub mod parser;

pub use codec::KafkaFrameCodec;
pub use error_code::KafkaErrorCode;
pub use header::{RequestHeader, ResponseHeader};
pub use messages::{
    ApiVersionKey, ApiVersionsRequest, ApiVersionsResponse, BrokerMetadata, FetchPartition,
    FetchPartitionResponse, FetchRequest, FetchResponse, FetchTopic, FetchTopicResponse,
    FindCoordinatorRequest, FindCoordinatorResponse, HeartbeatRequest, HeartbeatResponse,
    LeaveGroupRequest, LeaveGroupResponse, ListOffsetsPartition, ListOffsetsPartitionResponse,
    ListOffsetsRequest, ListOffsetsResponse, ListOffsetsTopic, ListOffsetsTopicResponse,
    MetadataRequest, MetadataResponse, OffsetCommitPartition, OffsetCommitPartitionResponse,
    OffsetCommitRequest, OffsetCommitResponse, OffsetCommitTopic, OffsetCommitTopicResponse,
    OffsetFetchPartitionResponse, OffsetFetchRequest, OffsetFetchResponse, OffsetFetchTopic,
    OffsetFetchTopicResponse, PartitionMetadata, PartitionProduceData, PartitionProduceResponse,
    ProduceRequest, ProduceResponse, TopicMetadata, TopicProduceData, TopicProduceResponse,
};
pub use parser::{KafkaDecoder, KafkaEncoder};

/// Kafka API Keys according to the Apache Kafka wire protocol specification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i16)]
pub enum ApiKey {
    Produce = 0,
    Fetch = 1,
    ListOffsets = 2,
    Metadata = 3,
    OffsetCommit = 8,
    OffsetFetch = 9,
    FindCoordinator = 10,
    JoinGroup = 11,
    Heartbeat = 12,
    LeaveGroup = 13,
    SyncGroup = 14,
    ApiVersions = 18,
    CreateTopics = 19,
    DeleteTopics = 20,
}

impl ApiKey {
    pub fn from_i16(val: i16) -> Option<Self> {
        match val {
            0 => Some(Self::Produce),
            1 => Some(Self::Fetch),
            2 => Some(Self::ListOffsets),
            3 => Some(Self::Metadata),
            8 => Some(Self::OffsetCommit),
            9 => Some(Self::OffsetFetch),
            10 => Some(Self::FindCoordinator),
            11 => Some(Self::JoinGroup),
            12 => Some(Self::Heartbeat),
            13 => Some(Self::LeaveGroup),
            14 => Some(Self::SyncGroup),
            18 => Some(Self::ApiVersions),
            19 => Some(Self::CreateTopics),
            20 => Some(Self::DeleteTopics),
            _ => None,
        }
    }
}
