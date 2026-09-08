use crate::error_code::KafkaErrorCode;
use crate::parser::{KafkaDecoder, KafkaEncoder};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use oxidemq_core::error::Result;

// ==========================================
// ApiVersions (Key 18)
// ==========================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiVersionKey {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ApiVersionsRequest {
    pub client_software_name: Option<String>,
    pub client_software_version: Option<String>,
}

impl ApiVersionsRequest {
    pub fn decode(src: &mut Bytes, version: i16) -> Result<Self> {
        let mut req = Self::default();
        if version >= 3 {
            req.client_software_name = KafkaDecoder::read_compact_string(src)?;
            req.client_software_version = KafkaDecoder::read_compact_string(src)?;
            let _tag_count = KafkaDecoder::read_unsigned_varint(src)?;
        }
        Ok(req)
    }

    pub fn encode(&self, dst: &mut BytesMut, version: i16) {
        if version >= 3 {
            KafkaEncoder::write_compact_string(dst, self.client_software_name.as_deref());
            KafkaEncoder::write_compact_string(dst, self.client_software_version.as_deref());
            KafkaEncoder::write_unsigned_varint(dst, 0); // tagged fields
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiVersionsResponse {
    pub error_code: KafkaErrorCode,
    pub api_keys: Vec<ApiVersionKey>,
    pub throttle_time_ms: i32,
}

impl ApiVersionsResponse {
    pub fn new(error_code: KafkaErrorCode, api_keys: Vec<ApiVersionKey>) -> Self {
        Self {
            error_code,
            api_keys,
            throttle_time_ms: 0,
        }
    }

    pub fn default_supported() -> Self {
        let keys = vec![
            ApiVersionKey {
                api_key: 0,
                min_version: 0,
                max_version: 7,
            }, // Produce
            ApiVersionKey {
                api_key: 1,
                min_version: 0,
                max_version: 6,
            }, // Fetch
            ApiVersionKey {
                api_key: 2,
                min_version: 0,
                max_version: 5,
            }, // ListOffsets
            ApiVersionKey {
                api_key: 3,
                min_version: 0,
                max_version: 7,
            }, // Metadata
            ApiVersionKey {
                api_key: 8,
                min_version: 0,
                max_version: 2,
            }, // OffsetCommit
            ApiVersionKey {
                api_key: 9,
                min_version: 0,
                max_version: 5,
            }, // OffsetFetch
            ApiVersionKey {
                api_key: 10,
                min_version: 0,
                max_version: 2,
            }, // FindCoordinator
            ApiVersionKey {
                api_key: 11,
                min_version: 0,
                max_version: 5,
            }, // JoinGroup
            ApiVersionKey {
                api_key: 12,
                min_version: 0,
                max_version: 3,
            }, // Heartbeat
            ApiVersionKey {
                api_key: 13,
                min_version: 0,
                max_version: 3,
            }, // LeaveGroup
            ApiVersionKey {
                api_key: 14,
                min_version: 0,
                max_version: 3,
            }, // SyncGroup
            ApiVersionKey {
                api_key: 18,
                min_version: 0,
                max_version: 3,
            }, // ApiVersions
            ApiVersionKey {
                api_key: 19,
                min_version: 0,
                max_version: 4,
            }, // CreateTopics
            ApiVersionKey {
                api_key: 20,
                min_version: 0,
                max_version: 3,
            }, // DeleteTopics
        ];
        Self::new(KafkaErrorCode::None, keys)
    }

    pub fn encode(&self, dst: &mut BytesMut, version: i16) {
        dst.put_i16(self.error_code.code());
        if version >= 3 {
            KafkaEncoder::write_unsigned_varint(dst, (self.api_keys.len() + 1) as u64);
            for k in &self.api_keys {
                dst.put_i16(k.api_key);
                dst.put_i16(k.min_version);
                dst.put_i16(k.max_version);
                KafkaEncoder::write_unsigned_varint(dst, 0); // tagged fields
            }
            dst.put_i32(self.throttle_time_ms);
            KafkaEncoder::write_unsigned_varint(dst, 0); // tagged fields
        } else {
            dst.put_i32(self.api_keys.len() as i32);
            for k in &self.api_keys {
                dst.put_i16(k.api_key);
                dst.put_i16(k.min_version);
                dst.put_i16(k.max_version);
            }
            if version >= 1 {
                dst.put_i32(self.throttle_time_ms);
            }
        }
    }

    pub fn decode(src: &mut Bytes, version: i16) -> Result<Self> {
        let error_code = KafkaErrorCode::from_i16(src.get_i16());
        let mut api_keys = Vec::new();

        if version >= 3 {
            let count = KafkaDecoder::read_unsigned_varint(src)? as usize;
            if count > 0 {
                let actual_count = count - 1;
                for _ in 0..actual_count {
                    let api_key = src.get_i16();
                    let min_version = src.get_i16();
                    let max_version = src.get_i16();
                    let _ = KafkaDecoder::read_unsigned_varint(src)?;
                    api_keys.push(ApiVersionKey {
                        api_key,
                        min_version,
                        max_version,
                    });
                }
            }
            let throttle_time_ms = src.get_i32();
            let _ = KafkaDecoder::read_unsigned_varint(src)?;
            Ok(Self {
                error_code,
                api_keys,
                throttle_time_ms,
            })
        } else {
            let count = src.get_i32() as usize;
            for _ in 0..count {
                let api_key = src.get_i16();
                let min_version = src.get_i16();
                let max_version = src.get_i16();
                api_keys.push(ApiVersionKey {
                    api_key,
                    min_version,
                    max_version,
                });
            }
            let throttle_time_ms = if version >= 1 { src.get_i32() } else { 0 };
            Ok(Self {
                error_code,
                api_keys,
                throttle_time_ms,
            })
        }
    }
}

// ==========================================
// Metadata (Key 3)
// ==========================================

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MetadataRequest {
    pub topics: Option<Vec<String>>,
    pub allow_auto_topic_creation: bool,
}

impl MetadataRequest {
    pub fn decode(src: &mut Bytes, _version: i16) -> Result<Self> {
        if src.len() < 4 {
            return Ok(Self::default());
        }
        let count = src.get_i32();
        let topics = if count < 0 {
            None
        } else {
            let mut t_vec = Vec::with_capacity(count as usize);
            for _ in 0..count {
                if let Some(t) = KafkaDecoder::read_string(src)? {
                    t_vec.push(t);
                }
            }
            Some(t_vec)
        };
        let allow_auto = if src.has_remaining() {
            src.get_u8() != 0
        } else {
            true
        };
        Ok(Self {
            topics,
            allow_auto_topic_creation: allow_auto,
        })
    }

    pub fn encode(&self, dst: &mut BytesMut, _version: i16) {
        match &self.topics {
            Some(t_vec) => {
                dst.put_i32(t_vec.len() as i32);
                for t in t_vec {
                    KafkaEncoder::write_string(dst, Some(t));
                }
            }
            None => {
                dst.put_i32(-1);
            }
        }
        dst.put_u8(self.allow_auto_topic_creation as u8);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerMetadata {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
    pub rack: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionMetadata {
    pub error_code: KafkaErrorCode,
    pub partition_index: i32,
    pub leader_id: i32,
    pub leader_epoch: i32,
    pub replica_nodes: Vec<i32>,
    pub isr_nodes: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicMetadata {
    pub error_code: KafkaErrorCode,
    pub name: String,
    pub is_internal: bool,
    pub partitions: Vec<PartitionMetadata>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataResponse {
    pub throttle_time_ms: i32,
    pub brokers: Vec<BrokerMetadata>,
    pub cluster_id: Option<String>,
    pub controller_id: i32,
    pub topics: Vec<TopicMetadata>,
}

impl MetadataResponse {
    pub fn encode(&self, dst: &mut BytesMut, version: i16) {
        if version >= 3 {
            dst.put_i32(self.throttle_time_ms);
        }
        // Brokers
        dst.put_i32(self.brokers.len() as i32);
        for b in &self.brokers {
            dst.put_i32(b.node_id);
            KafkaEncoder::write_string(dst, Some(&b.host));
            dst.put_i32(b.port);
            if version >= 1 {
                KafkaEncoder::write_string(dst, b.rack.as_deref());
            }
        }
        if version >= 2 {
            KafkaEncoder::write_string(dst, self.cluster_id.as_deref());
        }
        if version >= 1 {
            dst.put_i32(self.controller_id);
        }
        // Topics
        dst.put_i32(self.topics.len() as i32);
        for t in &self.topics {
            dst.put_i16(t.error_code.code());
            KafkaEncoder::write_string(dst, Some(&t.name));
            if version >= 1 {
                dst.put_u8(t.is_internal as u8);
            }
            dst.put_i32(t.partitions.len() as i32);
            for p in &t.partitions {
                dst.put_i16(p.error_code.code());
                dst.put_i32(p.partition_index);
                dst.put_i32(p.leader_id);
                if version >= 7 {
                    dst.put_i32(p.leader_epoch);
                }
                dst.put_i32(p.replica_nodes.len() as i32);
                for r in &p.replica_nodes {
                    dst.put_i32(*r);
                }
                dst.put_i32(p.isr_nodes.len() as i32);
                for isr in &p.isr_nodes {
                    dst.put_i32(*isr);
                }
                if version >= 5 {
                    dst.put_i32(0); // offline_replicas count = 0
                }
            }
        }
    }

    pub fn decode(src: &mut Bytes, version: i16) -> Result<Self> {
        let throttle_time_ms = if version >= 3 { src.get_i32() } else { 0 };

        let broker_count = src.get_i32() as usize;
        let mut brokers = Vec::with_capacity(broker_count);
        for _ in 0..broker_count {
            let node_id = src.get_i32();
            let host = KafkaDecoder::read_string(src)?.unwrap_or_default();
            let port = src.get_i32();
            let rack = if version >= 1 {
                KafkaDecoder::read_string(src)?
            } else {
                None
            };
            brokers.push(BrokerMetadata {
                node_id,
                host,
                port,
                rack,
            });
        }

        let cluster_id = if version >= 2 {
            KafkaDecoder::read_string(src)?
        } else {
            None
        };
        let controller_id = if version >= 1 { src.get_i32() } else { 0 };

        let topic_count = src.get_i32() as usize;
        let mut topics = Vec::with_capacity(topic_count);
        for _ in 0..topic_count {
            let error_code = KafkaErrorCode::from_i16(src.get_i16());
            let name = KafkaDecoder::read_string(src)?.unwrap_or_default();
            let is_internal = if version >= 1 {
                src.get_u8() != 0
            } else {
                false
            };

            let partition_count = src.get_i32() as usize;
            let mut partitions = Vec::with_capacity(partition_count);
            for _ in 0..partition_count {
                let p_err = KafkaErrorCode::from_i16(src.get_i16());
                let partition_index = src.get_i32();
                let leader_id = src.get_i32();
                let leader_epoch = if version >= 7 { src.get_i32() } else { 0 };

                let replica_count = src.get_i32() as usize;
                let mut replica_nodes = Vec::with_capacity(replica_count);
                for _ in 0..replica_count {
                    replica_nodes.push(src.get_i32());
                }

                let isr_count = src.get_i32() as usize;
                let mut isr_nodes = Vec::with_capacity(isr_count);
                for _ in 0..isr_count {
                    isr_nodes.push(src.get_i32());
                }

                if version >= 5 {
                    let offline_count = src.get_i32() as usize;
                    for _ in 0..offline_count {
                        src.get_i32();
                    }
                }

                partitions.push(PartitionMetadata {
                    error_code: p_err,
                    partition_index,
                    leader_id,
                    leader_epoch,
                    replica_nodes,
                    isr_nodes,
                });
            }

            topics.push(TopicMetadata {
                error_code,
                name,
                is_internal,
                partitions,
            });
        }

        Ok(Self {
            throttle_time_ms,
            brokers,
            cluster_id,
            controller_id,
            topics,
        })
    }
}

// ==========================================
// Produce (Key 0)
// ==========================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionProduceData {
    pub partition: i32,
    pub records: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicProduceData {
    pub topic: String,
    pub partitions: Vec<PartitionProduceData>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceRequest {
    pub acks: i16,
    pub timeout_ms: i32,
    pub topic_data: Vec<TopicProduceData>,
}

impl ProduceRequest {
    pub fn decode(src: &mut Bytes, version: i16) -> Result<Self> {
        if version >= 3 {
            let _transactional_id = KafkaDecoder::read_string(src)?;
        }
        let acks = src.get_i16();
        let timeout_ms = src.get_i32();

        let topic_count = src.get_i32() as usize;
        let mut topic_data = Vec::with_capacity(topic_count);

        for _ in 0..topic_count {
            let topic = KafkaDecoder::read_string(src)?.unwrap_or_default();
            let p_count = src.get_i32() as usize;
            let mut partitions = Vec::with_capacity(p_count);

            for _ in 0..p_count {
                let partition = src.get_i32();
                let records = KafkaDecoder::read_bytes(src)?.unwrap_or_default();
                partitions.push(PartitionProduceData { partition, records });
            }
            topic_data.push(TopicProduceData { topic, partitions });
        }

        Ok(Self {
            acks,
            timeout_ms,
            topic_data,
        })
    }

    pub fn encode(&self, dst: &mut BytesMut, version: i16) {
        if version >= 3 {
            KafkaEncoder::write_string(dst, None);
        }
        dst.put_i16(self.acks);
        dst.put_i32(self.timeout_ms);
        dst.put_i32(self.topic_data.len() as i32);

        for t in &self.topic_data {
            KafkaEncoder::write_string(dst, Some(&t.topic));
            dst.put_i32(t.partitions.len() as i32);
            for p in &t.partitions {
                dst.put_i32(p.partition);
                KafkaEncoder::write_bytes(dst, Some(&p.records));
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionProduceResponse {
    pub partition: i32,
    pub error_code: KafkaErrorCode,
    pub base_offset: i64,
    pub log_append_time_ms: i64,
    pub log_start_offset: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicProduceResponse {
    pub topic: String,
    pub partitions: Vec<PartitionProduceResponse>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceResponse {
    pub responses: Vec<TopicProduceResponse>,
    pub throttle_time_ms: i32,
}

impl ProduceResponse {
    pub fn encode(&self, dst: &mut BytesMut, version: i16) {
        dst.put_i32(self.responses.len() as i32);
        for t in &self.responses {
            KafkaEncoder::write_string(dst, Some(&t.topic));
            dst.put_i32(t.partitions.len() as i32);
            for p in &t.partitions {
                dst.put_i32(p.partition);
                dst.put_i16(p.error_code.code());
                dst.put_i64(p.base_offset);
                if version >= 2 {
                    dst.put_i64(p.log_append_time_ms);
                }
                if version >= 5 {
                    dst.put_i64(p.log_start_offset);
                }
            }
        }
        if version >= 1 {
            dst.put_i32(self.throttle_time_ms);
        }
    }

    pub fn decode(src: &mut Bytes, version: i16) -> Result<Self> {
        let topic_count = src.get_i32() as usize;
        let mut responses = Vec::with_capacity(topic_count);

        for _ in 0..topic_count {
            let topic = KafkaDecoder::read_string(src)?.unwrap_or_default();
            let p_count = src.get_i32() as usize;
            let mut partitions = Vec::with_capacity(p_count);

            for _ in 0..p_count {
                let partition = src.get_i32();
                let error_code = KafkaErrorCode::from_i16(src.get_i16());
                let base_offset = src.get_i64();
                let log_append_time_ms = if version >= 2 { src.get_i64() } else { -1 };
                let log_start_offset = if version >= 5 { src.get_i64() } else { 0 };

                partitions.push(PartitionProduceResponse {
                    partition,
                    error_code,
                    base_offset,
                    log_append_time_ms,
                    log_start_offset,
                });
            }
            responses.push(TopicProduceResponse { topic, partitions });
        }

        let throttle_time_ms = if version >= 1 { src.get_i32() } else { 0 };
        Ok(Self {
            responses,
            throttle_time_ms,
        })
    }
}

// ==========================================
// Fetch (Key 1)
// ==========================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchPartition {
    pub partition: i32,
    pub fetch_offset: i64,
    pub partition_max_bytes: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchTopic {
    pub topic: String,
    pub partitions: Vec<FetchPartition>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchRequest {
    pub max_wait_ms: i32,
    pub min_bytes: i32,
    pub max_bytes: i32,
    pub topics: Vec<FetchTopic>,
}

impl FetchRequest {
    pub fn decode(src: &mut Bytes, version: i16) -> Result<Self> {
        let _replica_id = src.get_i32();
        let max_wait_ms = src.get_i32();
        let min_bytes = src.get_i32();
        let max_bytes = if version >= 3 {
            src.get_i32()
        } else {
            0x7FFFFFFF
        };

        if version >= 4 {
            let _isolation_level = src.get_u8();
        }
        if version >= 7 {
            let _session_id = src.get_i32();
            let _session_epoch = src.get_i32();
        }

        let topic_count = src.get_i32() as usize;
        let mut topics = Vec::with_capacity(topic_count);

        for _ in 0..topic_count {
            let topic = KafkaDecoder::read_string(src)?.unwrap_or_default();
            let p_count = src.get_i32() as usize;
            let mut partitions = Vec::with_capacity(p_count);

            for _ in 0..p_count {
                let partition = src.get_i32();
                if version >= 9 {
                    let _current_leader_epoch = src.get_i32();
                }
                let fetch_offset = src.get_i64();
                if version >= 5 {
                    let _log_start_offset = src.get_i64();
                }
                let partition_max_bytes = src.get_i32();
                partitions.push(FetchPartition {
                    partition,
                    fetch_offset,
                    partition_max_bytes,
                });
            }
            topics.push(FetchTopic { topic, partitions });
        }

        Ok(Self {
            max_wait_ms,
            min_bytes,
            max_bytes,
            topics,
        })
    }

    pub fn encode(&self, dst: &mut BytesMut, version: i16) {
        dst.put_i32(-1); // replica_id (-1 for consumer)
        dst.put_i32(self.max_wait_ms);
        dst.put_i32(self.min_bytes);
        if version >= 3 {
            dst.put_i32(self.max_bytes);
        }
        if version >= 4 {
            dst.put_u8(0); // read_uncommitted
        }
        if version >= 7 {
            dst.put_i32(0); // session_id
            dst.put_i32(0); // session_epoch
        }

        dst.put_i32(self.topics.len() as i32);
        for t in &self.topics {
            KafkaEncoder::write_string(dst, Some(&t.topic));
            dst.put_i32(t.partitions.len() as i32);
            for p in &t.partitions {
                dst.put_i32(p.partition);
                if version >= 9 {
                    dst.put_i32(-1); // leader_epoch
                }
                dst.put_i64(p.fetch_offset);
                if version >= 5 {
                    dst.put_i64(0); // log_start_offset
                }
                dst.put_i32(p.partition_max_bytes);
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchPartitionResponse {
    pub partition_index: i32,
    pub error_code: KafkaErrorCode,
    pub high_watermark: i64,
    pub last_stable_offset: i64,
    pub records: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchTopicResponse {
    pub topic: String,
    pub partitions: Vec<FetchPartitionResponse>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchResponse {
    pub throttle_time_ms: i32,
    pub error_code: KafkaErrorCode,
    pub responses: Vec<FetchTopicResponse>,
}

impl FetchResponse {
    pub fn encode(&self, dst: &mut BytesMut, version: i16) {
        if version >= 1 {
            dst.put_i32(self.throttle_time_ms);
        }
        if version >= 7 {
            dst.put_i16(self.error_code.code());
            dst.put_i32(0); // session_id
        }

        dst.put_i32(self.responses.len() as i32);
        for t in &self.responses {
            KafkaEncoder::write_string(dst, Some(&t.topic));
            dst.put_i32(t.partitions.len() as i32);
            for p in &t.partitions {
                dst.put_i32(p.partition_index);
                dst.put_i16(p.error_code.code());
                dst.put_i64(p.high_watermark);
                if version >= 4 {
                    dst.put_i64(p.last_stable_offset);
                    dst.put_i64(0); // log_start_offset
                    dst.put_i32(-1); // aborted_transactions array (-1 = empty/null)
                }
                KafkaEncoder::write_bytes(dst, Some(&p.records));
            }
        }
    }

    pub fn decode(src: &mut Bytes, version: i16) -> Result<Self> {
        let throttle_time_ms = if version >= 1 { src.get_i32() } else { 0 };
        let error_code = if version >= 7 {
            let code = KafkaErrorCode::from_i16(src.get_i16());
            let _session_id = src.get_i32();
            code
        } else {
            KafkaErrorCode::None
        };

        let topic_count = src.get_i32() as usize;
        let mut responses = Vec::with_capacity(topic_count);

        for _ in 0..topic_count {
            let topic = KafkaDecoder::read_string(src)?.unwrap_or_default();
            let p_count = src.get_i32() as usize;
            let mut partitions = Vec::with_capacity(p_count);

            for _ in 0..p_count {
                let partition_index = src.get_i32();
                let error_code = KafkaErrorCode::from_i16(src.get_i16());
                let high_watermark = src.get_i64();
                let last_stable_offset = if version >= 4 {
                    let lso = src.get_i64();
                    let _log_start = src.get_i64();
                    let aborted_count = src.get_i32();
                    if aborted_count > 0 {
                        for _ in 0..aborted_count {
                            let _producer_id = src.get_i64();
                            let _first_offset = src.get_i64();
                        }
                    }
                    lso
                } else {
                    high_watermark
                };

                let records = KafkaDecoder::read_bytes(src)?.unwrap_or_default();

                partitions.push(FetchPartitionResponse {
                    partition_index,
                    error_code,
                    high_watermark,
                    last_stable_offset,
                    records,
                });
            }
            responses.push(FetchTopicResponse { topic, partitions });
        }

        Ok(Self {
            throttle_time_ms,
            error_code,
            responses,
        })
    }
}

// ==========================================
// FindCoordinator (Key 10)
// ==========================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FindCoordinatorRequest {
    pub key: String,
    pub key_type: i8, // 0 = group, 1 = transaction
}

impl FindCoordinatorRequest {
    pub fn decode(src: &mut Bytes, version: i16) -> Result<Self> {
        let key = KafkaDecoder::read_string(src)?.unwrap_or_default();
        let key_type = if version >= 1 { src.get_i8() } else { 0 };
        Ok(Self { key, key_type })
    }

    pub fn encode(&self, dst: &mut BytesMut, version: i16) {
        KafkaEncoder::write_string(dst, Some(&self.key));
        if version >= 1 {
            dst.put_i8(self.key_type);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FindCoordinatorResponse {
    pub throttle_time_ms: i32,
    pub error_code: KafkaErrorCode,
    pub error_message: Option<String>,
    pub node_id: i32,
    pub host: String,
    pub port: i32,
}

impl FindCoordinatorResponse {
    pub fn encode(&self, dst: &mut BytesMut, version: i16) {
        if version >= 1 {
            dst.put_i32(self.throttle_time_ms);
        }
        dst.put_i16(self.error_code.code());
        if version >= 1 {
            KafkaEncoder::write_string(dst, self.error_message.as_deref());
        }
        dst.put_i32(self.node_id);
        KafkaEncoder::write_string(dst, Some(&self.host));
        dst.put_i32(self.port);
    }

    pub fn decode(src: &mut Bytes, version: i16) -> Result<Self> {
        let throttle_time_ms = if version >= 1 { src.get_i32() } else { 0 };
        let error_code = KafkaErrorCode::from_i16(src.get_i16());
        let error_message = if version >= 1 {
            KafkaDecoder::read_string(src)?
        } else {
            None
        };
        let node_id = src.get_i32();
        let host = KafkaDecoder::read_string(src)?.unwrap_or_default();
        let port = src.get_i32();
        Ok(Self {
            throttle_time_ms,
            error_code,
            error_message,
            node_id,
            host,
            port,
        })
    }
}

// ==========================================
// ListOffsets (Key 2)
// ==========================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListOffsetsPartition {
    pub partition: i32,
    pub current_leader_epoch: i32,
    pub timestamp: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListOffsetsTopic {
    pub topic: String,
    pub partitions: Vec<ListOffsetsPartition>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListOffsetsRequest {
    pub replica_id: i32,
    pub isolation_level: i8,
    pub topics: Vec<ListOffsetsTopic>,
}

impl ListOffsetsRequest {
    pub fn decode(src: &mut Bytes, version: i16) -> Result<Self> {
        let replica_id = src.get_i32();
        let isolation_level = if version >= 2 { src.get_i8() } else { 0 };
        let topic_count = src.get_i32() as usize;
        let mut topics = Vec::with_capacity(topic_count);
        for _ in 0..topic_count {
            let topic = KafkaDecoder::read_string(src)?.unwrap_or_default();
            let p_count = src.get_i32() as usize;
            let mut partitions = Vec::with_capacity(p_count);
            for _ in 0..p_count {
                let partition = src.get_i32();
                let current_leader_epoch = if version >= 4 { src.get_i32() } else { -1 };
                let timestamp = src.get_i64();
                partitions.push(ListOffsetsPartition {
                    partition,
                    current_leader_epoch,
                    timestamp,
                });
            }
            topics.push(ListOffsetsTopic { topic, partitions });
        }
        Ok(Self {
            replica_id,
            isolation_level,
            topics,
        })
    }

    pub fn encode(&self, dst: &mut BytesMut, version: i16) {
        dst.put_i32(self.replica_id);
        if version >= 2 {
            dst.put_i8(self.isolation_level);
        }
        dst.put_i32(self.topics.len() as i32);
        for t in &self.topics {
            KafkaEncoder::write_string(dst, Some(&t.topic));
            dst.put_i32(t.partitions.len() as i32);
            for p in &t.partitions {
                dst.put_i32(p.partition);
                if version >= 4 {
                    dst.put_i32(p.current_leader_epoch);
                }
                dst.put_i64(p.timestamp);
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListOffsetsPartitionResponse {
    pub partition: i32,
    pub error_code: KafkaErrorCode,
    pub timestamp: i64,
    pub offset: i64,
    pub leader_epoch: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListOffsetsTopicResponse {
    pub topic: String,
    pub partitions: Vec<ListOffsetsPartitionResponse>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListOffsetsResponse {
    pub throttle_time_ms: i32,
    pub topics: Vec<ListOffsetsTopicResponse>,
}

impl ListOffsetsResponse {
    pub fn encode(&self, dst: &mut BytesMut, version: i16) {
        if version >= 2 {
            dst.put_i32(self.throttle_time_ms);
        }
        dst.put_i32(self.topics.len() as i32);
        for t in &self.topics {
            KafkaEncoder::write_string(dst, Some(&t.topic));
            dst.put_i32(t.partitions.len() as i32);
            for p in &t.partitions {
                dst.put_i32(p.partition);
                dst.put_i16(p.error_code.code());
                dst.put_i64(p.timestamp);
                dst.put_i64(p.offset);
                if version >= 4 {
                    dst.put_i32(p.leader_epoch);
                }
            }
        }
    }

    pub fn decode(src: &mut Bytes, version: i16) -> Result<Self> {
        let throttle_time_ms = if version >= 2 { src.get_i32() } else { 0 };
        let topic_count = src.get_i32() as usize;
        let mut topics = Vec::with_capacity(topic_count);
        for _ in 0..topic_count {
            let topic = KafkaDecoder::read_string(src)?.unwrap_or_default();
            let p_count = src.get_i32() as usize;
            let mut partitions = Vec::with_capacity(p_count);
            for _ in 0..p_count {
                let partition = src.get_i32();
                let error_code = KafkaErrorCode::from_i16(src.get_i16());
                let timestamp = src.get_i64();
                let offset = src.get_i64();
                let leader_epoch = if version >= 4 { src.get_i32() } else { -1 };
                partitions.push(ListOffsetsPartitionResponse {
                    partition,
                    error_code,
                    timestamp,
                    offset,
                    leader_epoch,
                });
            }
            topics.push(ListOffsetsTopicResponse { topic, partitions });
        }
        Ok(Self {
            throttle_time_ms,
            topics,
        })
    }
}

// ==========================================
// OffsetCommit (Key 8)
// ==========================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetCommitPartition {
    pub partition: i32,
    pub committed_offset: i64,
    pub metadata: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetCommitTopic {
    pub topic: String,
    pub partitions: Vec<OffsetCommitPartition>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetCommitRequest {
    pub group_id: String,
    pub generation_id: i32,
    pub member_id: String,
    pub topics: Vec<OffsetCommitTopic>,
}

impl OffsetCommitRequest {
    pub fn decode(src: &mut Bytes, version: i16) -> Result<Self> {
        let group_id = KafkaDecoder::read_string(src)?.unwrap_or_default();
        let (generation_id, member_id) = if version >= 1 {
            (
                src.get_i32(),
                KafkaDecoder::read_string(src)?.unwrap_or_default(),
            )
        } else {
            (0, String::new())
        };
        if (2..=4).contains(&version) {
            let _retention_time_ms = src.get_i64();
        }
        let topic_count = src.get_i32() as usize;
        let mut topics = Vec::with_capacity(topic_count);
        for _ in 0..topic_count {
            let topic = KafkaDecoder::read_string(src)?.unwrap_or_default();
            let p_count = src.get_i32() as usize;
            let mut partitions = Vec::with_capacity(p_count);
            for _ in 0..p_count {
                let partition = src.get_i32();
                let committed_offset = src.get_i64();
                let metadata = KafkaDecoder::read_string(src)?;
                partitions.push(OffsetCommitPartition {
                    partition,
                    committed_offset,
                    metadata,
                });
            }
            topics.push(OffsetCommitTopic { topic, partitions });
        }
        Ok(Self {
            group_id,
            generation_id,
            member_id,
            topics,
        })
    }

    pub fn encode(&self, dst: &mut BytesMut, version: i16) {
        KafkaEncoder::write_string(dst, Some(&self.group_id));
        if version >= 1 {
            dst.put_i32(self.generation_id);
            KafkaEncoder::write_string(dst, Some(&self.member_id));
        }
        if (2..=4).contains(&version) {
            dst.put_i64(-1);
        }
        dst.put_i32(self.topics.len() as i32);
        for t in &self.topics {
            KafkaEncoder::write_string(dst, Some(&t.topic));
            dst.put_i32(t.partitions.len() as i32);
            for p in &t.partitions {
                dst.put_i32(p.partition);
                dst.put_i64(p.committed_offset);
                KafkaEncoder::write_string(dst, p.metadata.as_deref());
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetCommitPartitionResponse {
    pub partition: i32,
    pub error_code: KafkaErrorCode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetCommitTopicResponse {
    pub topic: String,
    pub partitions: Vec<OffsetCommitPartitionResponse>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetCommitResponse {
    pub throttle_time_ms: i32,
    pub topics: Vec<OffsetCommitTopicResponse>,
}

impl OffsetCommitResponse {
    pub fn encode(&self, dst: &mut BytesMut, version: i16) {
        if version >= 3 {
            dst.put_i32(self.throttle_time_ms);
        }
        dst.put_i32(self.topics.len() as i32);
        for t in &self.topics {
            KafkaEncoder::write_string(dst, Some(&t.topic));
            dst.put_i32(t.partitions.len() as i32);
            for p in &t.partitions {
                dst.put_i32(p.partition);
                dst.put_i16(p.error_code.code());
            }
        }
    }

    pub fn decode(src: &mut Bytes, version: i16) -> Result<Self> {
        let throttle_time_ms = if version >= 3 { src.get_i32() } else { 0 };
        let topic_count = src.get_i32() as usize;
        let mut topics = Vec::with_capacity(topic_count);
        for _ in 0..topic_count {
            let topic = KafkaDecoder::read_string(src)?.unwrap_or_default();
            let p_count = src.get_i32() as usize;
            let mut partitions = Vec::with_capacity(p_count);
            for _ in 0..p_count {
                let partition = src.get_i32();
                let error_code = KafkaErrorCode::from_i16(src.get_i16());
                partitions.push(OffsetCommitPartitionResponse {
                    partition,
                    error_code,
                });
            }
            topics.push(OffsetCommitTopicResponse { topic, partitions });
        }
        Ok(Self {
            throttle_time_ms,
            topics,
        })
    }
}

// ==========================================
// OffsetFetch (Key 9)
// ==========================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetFetchTopic {
    pub topic: String,
    pub partitions: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetFetchRequest {
    pub group_id: String,
    pub topics: Option<Vec<OffsetFetchTopic>>,
}

impl OffsetFetchRequest {
    pub fn decode(src: &mut Bytes, _version: i16) -> Result<Self> {
        let group_id = KafkaDecoder::read_string(src)?.unwrap_or_default();
        let topic_count = src.get_i32();
        let topics = if topic_count < 0 {
            None
        } else {
            let mut t_vec = Vec::with_capacity(topic_count as usize);
            for _ in 0..topic_count {
                let topic = KafkaDecoder::read_string(src)?.unwrap_or_default();
                let p_count = src.get_i32() as usize;
                let mut partitions = Vec::with_capacity(p_count);
                for _ in 0..p_count {
                    partitions.push(src.get_i32());
                }
                t_vec.push(OffsetFetchTopic { topic, partitions });
            }
            Some(t_vec)
        };
        Ok(Self { group_id, topics })
    }

    pub fn encode(&self, dst: &mut BytesMut, _version: i16) {
        KafkaEncoder::write_string(dst, Some(&self.group_id));
        match &self.topics {
            Some(t_vec) => {
                dst.put_i32(t_vec.len() as i32);
                for t in t_vec {
                    KafkaEncoder::write_string(dst, Some(&t.topic));
                    dst.put_i32(t.partitions.len() as i32);
                    for p in &t.partitions {
                        dst.put_i32(*p);
                    }
                }
            }
            None => {
                dst.put_i32(-1);
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetFetchPartitionResponse {
    pub partition: i32,
    pub offset: i64,
    pub metadata: Option<String>,
    pub error_code: KafkaErrorCode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetFetchTopicResponse {
    pub topic: String,
    pub partitions: Vec<OffsetFetchPartitionResponse>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetFetchResponse {
    pub throttle_time_ms: i32,
    pub topics: Vec<OffsetFetchTopicResponse>,
    pub error_code: KafkaErrorCode,
}

impl OffsetFetchResponse {
    pub fn encode(&self, dst: &mut BytesMut, version: i16) {
        if version >= 3 {
            dst.put_i32(self.throttle_time_ms);
        }
        dst.put_i32(self.topics.len() as i32);
        for t in &self.topics {
            KafkaEncoder::write_string(dst, Some(&t.topic));
            dst.put_i32(t.partitions.len() as i32);
            for p in &t.partitions {
                dst.put_i32(p.partition);
                dst.put_i64(p.offset);
                if version >= 5 {
                    dst.put_i32(-1); // leader_epoch
                }
                KafkaEncoder::write_string(dst, p.metadata.as_deref());
                dst.put_i16(p.error_code.code());
            }
        }
        if version >= 2 {
            dst.put_i16(self.error_code.code());
        }
    }

    pub fn decode(src: &mut Bytes, version: i16) -> Result<Self> {
        let throttle_time_ms = if version >= 3 { src.get_i32() } else { 0 };
        let topic_count = src.get_i32() as usize;
        let mut topics = Vec::with_capacity(topic_count);
        for _ in 0..topic_count {
            let topic = KafkaDecoder::read_string(src)?.unwrap_or_default();
            let p_count = src.get_i32() as usize;
            let mut partitions = Vec::with_capacity(p_count);
            for _ in 0..p_count {
                let partition = src.get_i32();
                let offset = src.get_i64();
                if version >= 5 {
                    let _epoch = src.get_i32();
                }
                let metadata = KafkaDecoder::read_string(src)?;
                let error_code = KafkaErrorCode::from_i16(src.get_i16());
                partitions.push(OffsetFetchPartitionResponse {
                    partition,
                    offset,
                    metadata,
                    error_code,
                });
            }
            topics.push(OffsetFetchTopicResponse { topic, partitions });
        }
        let error_code = if version >= 2 {
            KafkaErrorCode::from_i16(src.get_i16())
        } else {
            KafkaErrorCode::None
        };
        Ok(Self {
            throttle_time_ms,
            topics,
            error_code,
        })
    }
}

// ==========================================
// Heartbeat (Key 12)
// ==========================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatRequest {
    pub group_id: String,
    pub generation_id: i32,
    pub member_id: String,
}

impl HeartbeatRequest {
    pub fn decode(src: &mut Bytes, _version: i16) -> Result<Self> {
        let group_id = KafkaDecoder::read_string(src)?.unwrap_or_default();
        let generation_id = src.get_i32();
        let member_id = KafkaDecoder::read_string(src)?.unwrap_or_default();
        Ok(Self {
            group_id,
            generation_id,
            member_id,
        })
    }

    pub fn encode(&self, dst: &mut BytesMut, _version: i16) {
        KafkaEncoder::write_string(dst, Some(&self.group_id));
        dst.put_i32(self.generation_id);
        KafkaEncoder::write_string(dst, Some(&self.member_id));
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatResponse {
    pub throttle_time_ms: i32,
    pub error_code: KafkaErrorCode,
}

impl HeartbeatResponse {
    pub fn encode(&self, dst: &mut BytesMut, version: i16) {
        if version >= 1 {
            dst.put_i32(self.throttle_time_ms);
        }
        dst.put_i16(self.error_code.code());
    }

    pub fn decode(src: &mut Bytes, version: i16) -> Result<Self> {
        let throttle_time_ms = if version >= 1 { src.get_i32() } else { 0 };
        let error_code = KafkaErrorCode::from_i16(src.get_i16());
        Ok(Self {
            throttle_time_ms,
            error_code,
        })
    }
}

// ==========================================
// LeaveGroup (Key 13)
// ==========================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaveGroupRequest {
    pub group_id: String,
    pub member_id: String,
}

impl LeaveGroupRequest {
    pub fn decode(src: &mut Bytes, _version: i16) -> Result<Self> {
        let group_id = KafkaDecoder::read_string(src)?.unwrap_or_default();
        let member_id = KafkaDecoder::read_string(src)?.unwrap_or_default();
        Ok(Self {
            group_id,
            member_id,
        })
    }

    pub fn encode(&self, dst: &mut BytesMut, _version: i16) {
        KafkaEncoder::write_string(dst, Some(&self.group_id));
        KafkaEncoder::write_string(dst, Some(&self.member_id));
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaveGroupResponse {
    pub throttle_time_ms: i32,
    pub error_code: KafkaErrorCode,
}

impl LeaveGroupResponse {
    pub fn encode(&self, dst: &mut BytesMut, version: i16) {
        if version >= 1 {
            dst.put_i32(self.throttle_time_ms);
        }
        dst.put_i16(self.error_code.code());
    }

    pub fn decode(src: &mut Bytes, version: i16) -> Result<Self> {
        let throttle_time_ms = if version >= 1 { src.get_i32() } else { 0 };
        let error_code = KafkaErrorCode::from_i16(src.get_i16());
        Ok(Self {
            throttle_time_ms,
            error_code,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_api_versions_codec() {
        let req = ApiVersionsRequest {
            client_software_name: Some("test-client".into()),
            client_software_version: Some("1.0.0".into()),
        };
        for v in [0, 1, 3] {
            let mut buf = BytesMut::new();
            req.encode(&mut buf, v);
            let mut read_buf = buf.freeze();
            let decoded = ApiVersionsRequest::decode(&mut read_buf, v).unwrap();
            if v >= 3 {
                assert_eq!(decoded.client_software_name, req.client_software_name);
            }
        }

        let resp = ApiVersionsResponse::default_supported();
        for v in [0, 1, 3] {
            let mut buf = BytesMut::new();
            resp.encode(&mut buf, v);
            let mut read_buf = buf.freeze();
            let decoded = ApiVersionsResponse::decode(&mut read_buf, v).unwrap();
            assert_eq!(decoded.error_code, KafkaErrorCode::None);
            assert_eq!(decoded.api_keys.len(), resp.api_keys.len());
        }
    }

    #[test]
    fn test_metadata_codec() {
        let req = MetadataRequest {
            topics: Some(vec!["topic-a".into(), "topic-b".into()]),
            allow_auto_topic_creation: true,
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf, 1);
        let mut read_buf = buf.freeze();
        let decoded = MetadataRequest::decode(&mut read_buf, 1).unwrap();
        assert_eq!(decoded.topics, req.topics);

        let resp = MetadataResponse {
            throttle_time_ms: 10,
            brokers: vec![BrokerMetadata {
                node_id: 1,
                host: "localhost".into(),
                port: 9092,
                rack: Some("rack-1".into()),
            }],
            cluster_id: Some("cluster-xyz".into()),
            controller_id: 1,
            topics: vec![TopicMetadata {
                error_code: KafkaErrorCode::None,
                name: "topic-a".into(),
                is_internal: false,
                partitions: vec![PartitionMetadata {
                    error_code: KafkaErrorCode::None,
                    partition_index: 0,
                    leader_id: 1,
                    leader_epoch: 1,
                    replica_nodes: vec![1],
                    isr_nodes: vec![1],
                }],
            }],
        };

        for v in [1, 3, 5] {
            let mut b = BytesMut::new();
            resp.encode(&mut b, v);
            let mut r = b.freeze();
            let dec = MetadataResponse::decode(&mut r, v).unwrap();
            assert_eq!(dec.brokers.len(), 1);
            assert_eq!(dec.topics.len(), 1);
            assert_eq!(dec.topics[0].partitions.len(), 1);
        }
    }

    #[test]
    fn test_produce_codec() {
        let req = ProduceRequest {
            acks: 1,
            timeout_ms: 5000,
            topic_data: vec![TopicProduceData {
                topic: "test-topic".into(),
                partitions: vec![PartitionProduceData {
                    partition: 0,
                    records: Bytes::from_static(b"fake-record-batch"),
                }],
            }],
        };

        for v in [2, 3, 7] {
            let mut buf = BytesMut::new();
            req.encode(&mut buf, v);
            let mut read_buf = buf.freeze();
            let decoded = ProduceRequest::decode(&mut read_buf, v).unwrap();
            assert_eq!(decoded.acks, req.acks);
            assert_eq!(decoded.topic_data.len(), 1);
            assert_eq!(
                decoded.topic_data[0].partitions[0].records,
                Bytes::from_static(b"fake-record-batch")
            );
        }

        let resp = ProduceResponse {
            responses: vec![TopicProduceResponse {
                topic: "test-topic".into(),
                partitions: vec![PartitionProduceResponse {
                    partition: 0,
                    error_code: KafkaErrorCode::None,
                    base_offset: 100,
                    log_append_time_ms: 123456789,
                    log_start_offset: 0,
                }],
            }],
            throttle_time_ms: 5,
        };

        for v in [2, 5, 7] {
            let mut buf = BytesMut::new();
            resp.encode(&mut buf, v);
            let mut read_buf = buf.freeze();
            let decoded = ProduceResponse::decode(&mut read_buf, v).unwrap();
            assert_eq!(decoded.responses.len(), 1);
            assert_eq!(decoded.responses[0].partitions[0].base_offset, 100);
        }
    }

    #[test]
    fn test_fetch_codec() {
        let req = FetchRequest {
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1048576,
            topics: vec![FetchTopic {
                topic: "topic-1".into(),
                partitions: vec![FetchPartition {
                    partition: 0,
                    fetch_offset: 50,
                    partition_max_bytes: 65536,
                }],
            }],
        };

        for v in [4, 6] {
            let mut buf = BytesMut::new();
            req.encode(&mut buf, v);
            let mut read_buf = buf.freeze();
            let decoded = FetchRequest::decode(&mut read_buf, v).unwrap();
            assert_eq!(decoded.topics.len(), 1);
            assert_eq!(decoded.topics[0].partitions[0].fetch_offset, 50);
        }

        let resp = FetchResponse {
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
            responses: vec![FetchTopicResponse {
                topic: "topic-1".into(),
                partitions: vec![FetchPartitionResponse {
                    partition_index: 0,
                    error_code: KafkaErrorCode::None,
                    high_watermark: 100,
                    last_stable_offset: 100,
                    records: Bytes::from_static(b"records-data"),
                }],
            }],
        };

        for v in [4, 6] {
            let mut buf = BytesMut::new();
            resp.encode(&mut buf, v);
            let mut read_buf = buf.freeze();
            let decoded = FetchResponse::decode(&mut read_buf, v).unwrap();
            assert_eq!(decoded.responses.len(), 1);
            assert_eq!(decoded.responses[0].partitions[0].high_watermark, 100);
        }
    }

    #[test]
    fn test_list_offsets_codec() {
        let req = ListOffsetsRequest {
            replica_id: -1,
            isolation_level: 0,
            topics: vec![ListOffsetsTopic {
                topic: "test-topic".into(),
                partitions: vec![ListOffsetsPartition {
                    partition: 0,
                    current_leader_epoch: 1,
                    timestamp: -1,
                }],
            }],
        };

        for v in [1, 2, 4, 5] {
            let mut buf = BytesMut::new();
            req.encode(&mut buf, v);
            let mut read_buf = buf.freeze();
            let decoded = ListOffsetsRequest::decode(&mut read_buf, v).unwrap();
            assert_eq!(decoded.topics.len(), 1);
            assert_eq!(decoded.topics[0].partitions[0].timestamp, -1);
        }

        let resp = ListOffsetsResponse {
            throttle_time_ms: 0,
            topics: vec![ListOffsetsTopicResponse {
                topic: "test-topic".into(),
                partitions: vec![ListOffsetsPartitionResponse {
                    partition: 0,
                    error_code: KafkaErrorCode::None,
                    timestamp: 1000,
                    offset: 42,
                    leader_epoch: 1,
                }],
            }],
        };

        for v in [1, 2, 4, 5] {
            let mut buf = BytesMut::new();
            resp.encode(&mut buf, v);
            let mut read_buf = buf.freeze();
            let decoded = ListOffsetsResponse::decode(&mut read_buf, v).unwrap();
            assert_eq!(decoded.topics.len(), 1);
            assert_eq!(decoded.topics[0].partitions[0].offset, 42);
        }
    }

    #[test]
    fn test_coordinator_and_offsets_codec() {
        // FindCoordinator
        let fc_req = FindCoordinatorRequest {
            key: "my-group".into(),
            key_type: 0,
        };
        for v in [1, 2] {
            let mut buf = BytesMut::new();
            fc_req.encode(&mut buf, v);
            let mut read_buf = buf.freeze();
            let decoded = FindCoordinatorRequest::decode(&mut read_buf, v).unwrap();
            assert_eq!(decoded.key, "my-group");
        }

        let fc_resp = FindCoordinatorResponse {
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
            error_message: None,
            node_id: 1,
            host: "127.0.0.1".into(),
            port: 9092,
        };
        for v in [1, 2] {
            let mut buf = BytesMut::new();
            fc_resp.encode(&mut buf, v);
            let mut read_buf = buf.freeze();
            let decoded = FindCoordinatorResponse::decode(&mut read_buf, v).unwrap();
            assert_eq!(decoded.node_id, 1);
        }

        // OffsetCommit
        let oc_req = OffsetCommitRequest {
            group_id: "group-1".into(),
            generation_id: 1,
            member_id: "member-1".into(),
            topics: vec![OffsetCommitTopic {
                topic: "test-topic".into(),
                partitions: vec![OffsetCommitPartition {
                    partition: 0,
                    committed_offset: 123,
                    metadata: Some("meta".into()),
                }],
            }],
        };
        for v in [1, 2] {
            let mut buf = BytesMut::new();
            oc_req.encode(&mut buf, v);
            let mut read_buf = buf.freeze();
            let decoded = OffsetCommitRequest::decode(&mut read_buf, v).unwrap();
            assert_eq!(decoded.group_id, "group-1");
            assert_eq!(decoded.topics[0].partitions[0].committed_offset, 123);
        }

        let oc_resp = OffsetCommitResponse {
            throttle_time_ms: 0,
            topics: vec![OffsetCommitTopicResponse {
                topic: "test-topic".into(),
                partitions: vec![OffsetCommitPartitionResponse {
                    partition: 0,
                    error_code: KafkaErrorCode::None,
                }],
            }],
        };
        for v in [1, 2] {
            let mut buf = BytesMut::new();
            oc_resp.encode(&mut buf, v);
            let mut read_buf = buf.freeze();
            let decoded = OffsetCommitResponse::decode(&mut read_buf, v).unwrap();
            assert_eq!(decoded.topics.len(), 1);
        }

        // OffsetFetch
        let of_req = OffsetFetchRequest {
            group_id: "group-1".into(),
            topics: Some(vec![OffsetFetchTopic {
                topic: "test-topic".into(),
                partitions: vec![0],
            }]),
        };
        for v in [1, 5] {
            let mut buf = BytesMut::new();
            of_req.encode(&mut buf, v);
            let mut read_buf = buf.freeze();
            let decoded = OffsetFetchRequest::decode(&mut read_buf, v).unwrap();
            assert_eq!(decoded.group_id, "group-1");
        }

        let of_resp = OffsetFetchResponse {
            throttle_time_ms: 0,
            topics: vec![OffsetFetchTopicResponse {
                topic: "test-topic".into(),
                partitions: vec![OffsetFetchPartitionResponse {
                    partition: 0,
                    offset: 123,
                    metadata: Some("meta".into()),
                    error_code: KafkaErrorCode::None,
                }],
            }],
            error_code: KafkaErrorCode::None,
        };
        for v in [1, 5] {
            let mut buf = BytesMut::new();
            of_resp.encode(&mut buf, v);
            let mut read_buf = buf.freeze();
            let decoded = OffsetFetchResponse::decode(&mut read_buf, v).unwrap();
            assert_eq!(decoded.topics.len(), 1);
        }
    }

    #[test]
    fn test_heartbeat_and_leave_group() {
        let hb_req = HeartbeatRequest {
            group_id: "group-hb".into(),
            generation_id: 2,
            member_id: "member-hb".into(),
        };
        let mut buf = BytesMut::new();
        hb_req.encode(&mut buf, 1);
        let mut read_buf = buf.freeze();
        let decoded = HeartbeatRequest::decode(&mut read_buf, 1).unwrap();
        assert_eq!(decoded.group_id, "group-hb");

        let hb_resp = HeartbeatResponse {
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
        };
        let mut buf = BytesMut::new();
        hb_resp.encode(&mut buf, 1);
        let mut read_buf = buf.freeze();
        let decoded = HeartbeatResponse::decode(&mut read_buf, 1).unwrap();
        assert_eq!(decoded.error_code, KafkaErrorCode::None);

        let lg_req = LeaveGroupRequest {
            group_id: "group-lg".into(),
            member_id: "member-lg".into(),
        };
        let mut buf = BytesMut::new();
        lg_req.encode(&mut buf, 1);
        let mut read_buf = buf.freeze();
        let decoded = LeaveGroupRequest::decode(&mut read_buf, 1).unwrap();
        assert_eq!(decoded.group_id, "group-lg");

        let lg_resp = LeaveGroupResponse {
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
        };
        let mut buf = BytesMut::new();
        lg_resp.encode(&mut buf, 1);
        let mut read_buf = buf.freeze();
        let decoded = LeaveGroupResponse::decode(&mut read_buf, 1).unwrap();
        assert_eq!(decoded.error_code, KafkaErrorCode::None);
    }
}
