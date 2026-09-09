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
    compress_record_batch, decompress_record_batch, encode_control_batch, encode_record_batch_v2,
    parse_record_batch_records, AddOffsetsToTxnRequest, AddOffsetsToTxnResponse,
    AddPartitionsToTxnPartitionResult, AddPartitionsToTxnRequest, AddPartitionsToTxnResponse,
    AddPartitionsToTxnTopic, AddPartitionsToTxnTopicResult, ApiVersionKey, ApiVersionsRequest,
    ApiVersionsResponse, BrokerMetadata, EndTxnRequest, EndTxnResponse, FetchPartition,
    FetchPartitionResponse, FetchRequest, FetchResponse, FetchTopic, FetchTopicResponse,
    FindCoordinatorRequest, FindCoordinatorResponse, HeartbeatRequest, HeartbeatResponse,
    InitProducerIdRequest, InitProducerIdResponse, LeaveGroupRequest, LeaveGroupResponse,
    ListOffsetsPartition, ListOffsetsPartitionResponse, ListOffsetsRequest, ListOffsetsResponse,
    ListOffsetsTopic, ListOffsetsTopicResponse, MetadataRequest, MetadataResponse,
    OffsetCommitPartition, OffsetCommitPartitionResponse, OffsetCommitRequest,
    OffsetCommitResponse, OffsetCommitTopic, OffsetCommitTopicResponse,
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
    InitProducerId = 22,
    AddPartitionsToTxn = 24,
    AddOffsetsToTxn = 25,
    EndTxn = 26,
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
            22 => Some(Self::InitProducerId),
            24 => Some(Self::AddPartitionsToTxn),
            25 => Some(Self::AddOffsetsToTxn),
            26 => Some(Self::EndTxn),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_api_key_from_i16() {
        assert_eq!(ApiKey::from_i16(0), Some(ApiKey::Produce));
        assert_eq!(ApiKey::from_i16(1), Some(ApiKey::Fetch));
        assert_eq!(ApiKey::from_i16(2), Some(ApiKey::ListOffsets));
        assert_eq!(ApiKey::from_i16(3), Some(ApiKey::Metadata));
        assert_eq!(ApiKey::from_i16(8), Some(ApiKey::OffsetCommit));
        assert_eq!(ApiKey::from_i16(9), Some(ApiKey::OffsetFetch));
        assert_eq!(ApiKey::from_i16(10), Some(ApiKey::FindCoordinator));
        assert_eq!(ApiKey::from_i16(11), Some(ApiKey::JoinGroup));
        assert_eq!(ApiKey::from_i16(12), Some(ApiKey::Heartbeat));
        assert_eq!(ApiKey::from_i16(13), Some(ApiKey::LeaveGroup));
        assert_eq!(ApiKey::from_i16(14), Some(ApiKey::SyncGroup));
        assert_eq!(ApiKey::from_i16(18), Some(ApiKey::ApiVersions));
        assert_eq!(ApiKey::from_i16(19), Some(ApiKey::CreateTopics));
        assert_eq!(ApiKey::from_i16(20), Some(ApiKey::DeleteTopics));
        assert_eq!(ApiKey::from_i16(22), Some(ApiKey::InitProducerId));
        assert_eq!(ApiKey::from_i16(24), Some(ApiKey::AddPartitionsToTxn));
        assert_eq!(ApiKey::from_i16(25), Some(ApiKey::AddOffsetsToTxn));
        assert_eq!(ApiKey::from_i16(26), Some(ApiKey::EndTxn));
        assert_eq!(ApiKey::from_i16(99), None);
    }
}
