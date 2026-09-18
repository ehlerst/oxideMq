use crate::error_code::KafkaErrorCode;
use crate::parser::{KafkaDecoder, KafkaEncoder};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use oxidemq_core::error::{OxideMqError, Result};
use oxidemq_core::types::{CompressionCodec, Record, RecordHeader};

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
                api_key: 15,
                min_version: 0,
                max_version: 2,
            }, // DescribeGroups
            ApiVersionKey {
                api_key: 16,
                min_version: 0,
                max_version: 2,
            }, // ListGroups
            ApiVersionKey {
                api_key: 17,
                min_version: 0,
                max_version: 1,
            }, // SaslHandshake
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
            ApiVersionKey {
                api_key: 22,
                min_version: 0,
                max_version: 4,
            }, // InitProducerId
            ApiVersionKey {
                api_key: 24,
                min_version: 0,
                max_version: 3,
            }, // AddPartitionsToTxn
            ApiVersionKey {
                api_key: 25,
                min_version: 0,
                max_version: 3,
            }, // AddOffsetsToTxn
            ApiVersionKey {
                api_key: 26,
                min_version: 0,
                max_version: 3,
            }, // EndTxn
            ApiVersionKey {
                api_key: 29,
                min_version: 0,
                max_version: 2,
            }, // DescribeAcls
            ApiVersionKey {
                api_key: 30,
                min_version: 0,
                max_version: 2,
            }, // CreateAcls
            ApiVersionKey {
                api_key: 31,
                min_version: 0,
                max_version: 2,
            }, // DeleteAcls
            ApiVersionKey {
                api_key: 32,
                min_version: 0,
                max_version: 2,
            }, // DescribeConfigs
            ApiVersionKey {
                api_key: 33,
                min_version: 0,
                max_version: 1,
            }, // AlterConfigs
            ApiVersionKey {
                api_key: 36,
                min_version: 0,
                max_version: 2,
            }, // SaslAuthenticate
            ApiVersionKey {
                api_key: 42,
                min_version: 0,
                max_version: 2,
            }, // DeleteGroups
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

impl PartitionProduceData {
    /// Returns the decompressed records payload if the batch is compressed.
    pub fn decompressed_records(&self) -> Result<Bytes> {
        decompress_record_batch(&self.records)
    }

    /// Compresses or recompresses the records in this partition data with the specified codec.
    pub fn compress_with(&mut self, codec: CompressionCodec) -> Result<()> {
        self.records = compress_record_batch(&self.records, codec)?;
        Ok(())
    }
}

/// Decompresses any compressed Kafka RecordBatch v2 within `batch_bytes`.
/// If the batch is already uncompressed, returns the bytes unchanged.
pub fn decompress_record_batch(batch_bytes: &Bytes) -> Result<Bytes> {
    if batch_bytes.len() < 61 {
        return Ok(batch_bytes.clone());
    }

    let mut cursor = 0;
    let mut out = BytesMut::new();
    let src = batch_bytes.as_ref();
    let mut any_decompressed = false;

    while cursor + 61 <= src.len() {
        if src[cursor + 16] == 2 {
            let batch_len =
                i32::from_be_bytes(src[cursor + 8..cursor + 12].try_into().unwrap()) as usize;
            let total_batch_size = 12 + batch_len;
            if cursor + total_batch_size > src.len() {
                break;
            }

            let attributes = i16::from_be_bytes(src[cursor + 21..cursor + 23].try_into().unwrap());
            let codec = CompressionCodec::from_attributes(attributes);

            if codec != CompressionCodec::None {
                any_decompressed = true;
                let compressed_payload = &src[cursor + 61..cursor + total_batch_size];
                let decompressed = codec.decompress(compressed_payload)?;

                let new_batch_len = (49 + decompressed.len()) as i32;
                let start_idx = out.len();

                out.extend_from_slice(&src[cursor..cursor + 8]);
                out.put_i32(new_batch_len);
                out.extend_from_slice(&src[cursor + 12..cursor + 16]);
                out.put_u8(2);
                out.put_u32(0);
                let uncompressed_attributes = attributes & !0x07;
                out.put_i16(uncompressed_attributes);
                out.extend_from_slice(&src[cursor + 23..cursor + 61]);
                out.extend_from_slice(&decompressed);

                let crc_data = &out[start_idx + 21..];
                let crc = oxidemq_core::compute_crc32c(crc_data);
                out[start_idx + 17..start_idx + 21].copy_from_slice(&crc.to_be_bytes());
            } else {
                out.extend_from_slice(&src[cursor..cursor + total_batch_size]);
            }

            cursor += total_batch_size;
        } else {
            break;
        }
    }

    if !any_decompressed {
        return Ok(batch_bytes.clone());
    }

    if cursor < src.len() {
        out.extend_from_slice(&src[cursor..]);
    }

    Ok(out.freeze())
}

/// Compresses any Kafka RecordBatch v2 within `batch_bytes` using `target_codec`.
pub fn compress_record_batch(batch_bytes: &Bytes, target_codec: CompressionCodec) -> Result<Bytes> {
    if target_codec == CompressionCodec::None {
        return decompress_record_batch(batch_bytes);
    }

    let uncompressed_bytes = decompress_record_batch(batch_bytes)?;
    let src = uncompressed_bytes.as_ref();
    if src.len() < 61 {
        return Ok(batch_bytes.clone());
    }

    let mut cursor = 0;
    let mut out = BytesMut::new();

    while cursor + 61 <= src.len() {
        if src[cursor + 16] == 2 {
            let batch_len =
                i32::from_be_bytes(src[cursor + 8..cursor + 12].try_into().unwrap()) as usize;
            let total_batch_size = 12 + batch_len;
            if cursor + total_batch_size > src.len() {
                break;
            }

            let attributes = i16::from_be_bytes(src[cursor + 21..cursor + 23].try_into().unwrap());
            let payload = &src[cursor + 61..cursor + total_batch_size];
            let compressed = target_codec.compress(payload)?;

            let new_batch_len = (49 + compressed.len()) as i32;
            let start_idx = out.len();

            out.extend_from_slice(&src[cursor..cursor + 8]);
            out.put_i32(new_batch_len);
            out.extend_from_slice(&src[cursor + 12..cursor + 16]);
            out.put_u8(2);
            out.put_u32(0);
            let new_attributes = (attributes & !0x07) | target_codec.to_attributes();
            out.put_i16(new_attributes);
            out.extend_from_slice(&src[cursor + 23..cursor + 61]);
            out.extend_from_slice(&compressed);

            let crc_data = &out[start_idx + 21..];
            let crc = oxidemq_core::compute_crc32c(crc_data);
            out[start_idx + 17..start_idx + 21].copy_from_slice(&crc.to_be_bytes());

            cursor += total_batch_size;
        } else {
            break;
        }
    }

    if cursor < src.len() {
        out.extend_from_slice(&src[cursor..]);
    }

    Ok(out.freeze())
}

/// Encodes a slice of records into a Kafka RecordBatch v2 byte buffer, optionally compressing with `codec`.
pub fn encode_record_batch_v2(
    base_offset: i64,
    records: &[Record],
    codec: CompressionCodec,
) -> Result<Bytes> {
    let mut payload = BytesMut::new();
    let base_timestamp = records.first().map(|r| r.timestamp).unwrap_or(0);
    let mut max_timestamp = base_timestamp;

    for record in records {
        max_timestamp = max_timestamp.max(record.timestamp);

        let mut rec_body = BytesMut::new();
        rec_body.put_i8(0);
        KafkaEncoder::write_varint(&mut rec_body, record.timestamp - base_timestamp);
        KafkaEncoder::write_varint(&mut rec_body, record.offset - base_offset);

        match &record.key {
            Some(k) => {
                KafkaEncoder::write_varint(&mut rec_body, k.len() as i64);
                rec_body.extend_from_slice(k);
            }
            None => {
                KafkaEncoder::write_varint(&mut rec_body, -1);
            }
        }

        match &record.value {
            Some(v) => {
                KafkaEncoder::write_varint(&mut rec_body, v.len() as i64);
                rec_body.extend_from_slice(v);
            }
            None => {
                KafkaEncoder::write_varint(&mut rec_body, -1);
            }
        }

        KafkaEncoder::write_varint(&mut rec_body, record.headers.len() as i64);
        for h in &record.headers {
            KafkaEncoder::write_varint(&mut rec_body, h.key.len() as i64);
            rec_body.extend_from_slice(h.key.as_bytes());
            KafkaEncoder::write_varint(&mut rec_body, h.value.len() as i64);
            rec_body.extend_from_slice(&h.value);
        }

        KafkaEncoder::write_varint(&mut payload, rec_body.len() as i64);
        payload.extend_from_slice(&rec_body);
    }

    let records_payload = if codec != CompressionCodec::None {
        codec.compress(&payload)?
    } else {
        payload.freeze()
    };

    let count = records.len() as i32;
    let last_offset_delta = (count - 1).max(0);
    let batch_len = (49 + records_payload.len()) as i32;

    let mut out = BytesMut::with_capacity(12 + batch_len as usize);
    out.put_i64(base_offset);
    out.put_i32(batch_len);
    out.put_i32(0);
    out.put_i8(2);
    out.put_u32(0);

    let attributes = codec.to_attributes();
    out.put_i16(attributes);
    out.put_i32(last_offset_delta);
    out.put_i64(base_timestamp);
    out.put_i64(max_timestamp);
    out.put_i64(-1);
    out.put_i16(-1);
    out.put_i32(-1);
    out.put_i32(count);
    out.extend_from_slice(&records_payload);

    let crc = oxidemq_core::compute_crc32c(&out[21..]);
    out[17..21].copy_from_slice(&crc.to_be_bytes());

    Ok(out.freeze())
}

/// Parses individual `Record` items from a Kafka RecordBatch v2 buffer, decompressing if necessary.
pub fn parse_record_batch_records(batch_bytes: &Bytes) -> Result<Vec<Record>> {
    let uncompressed = decompress_record_batch(batch_bytes)?;
    let mut records = Vec::new();
    let src = uncompressed.as_ref();
    let mut cursor = 0;

    while cursor + 61 <= src.len() {
        if src[cursor + 16] == 2 {
            let base_offset = i64::from_be_bytes(src[cursor..cursor + 8].try_into().unwrap());
            let batch_len =
                i32::from_be_bytes(src[cursor + 8..cursor + 12].try_into().unwrap()) as usize;
            let total_batch_size = 12 + batch_len;
            if cursor + total_batch_size > src.len() {
                break;
            }

            let base_timestamp =
                i64::from_be_bytes(src[cursor + 27..cursor + 35].try_into().unwrap());
            let num_records = i32::from_be_bytes(src[cursor + 57..cursor + 61].try_into().unwrap());

            let mut payload = Bytes::copy_from_slice(&src[cursor + 61..cursor + total_batch_size]);

            for _ in 0..num_records {
                if !payload.has_remaining() {
                    break;
                }
                let _record_len = KafkaDecoder::read_varint(&mut payload)?;
                if !payload.has_remaining() {
                    break;
                }
                let _attributes = payload.get_i8();
                let ts_delta = KafkaDecoder::read_varint(&mut payload)?;
                let off_delta = KafkaDecoder::read_varint(&mut payload)?;

                let key_len = KafkaDecoder::read_varint(&mut payload)?;
                let key = if key_len >= 0 {
                    let len = key_len as usize;
                    if payload.len() < len {
                        return Err(OxideMqError::Protocol("Truncated record key".into()));
                    }
                    Some(payload.copy_to_bytes(len))
                } else {
                    None
                };

                let val_len = KafkaDecoder::read_varint(&mut payload)?;
                let value = if val_len >= 0 {
                    let len = val_len as usize;
                    if payload.len() < len {
                        return Err(OxideMqError::Protocol("Truncated record value".into()));
                    }
                    Some(payload.copy_to_bytes(len))
                } else {
                    None
                };

                let headers_count = KafkaDecoder::read_varint(&mut payload)?;
                let mut headers = Vec::new();
                for _ in 0..headers_count.max(0) {
                    let k_len = KafkaDecoder::read_varint(&mut payload)?;
                    if k_len < 0 || payload.len() < k_len as usize {
                        return Err(OxideMqError::Protocol("Truncated header key".into()));
                    }
                    let k_bytes = payload.copy_to_bytes(k_len as usize);
                    let k_str = String::from_utf8(k_bytes.to_vec()).map_err(|e| {
                        OxideMqError::Protocol(format!("Invalid header key UTF-8: {e}"))
                    })?;

                    let v_len = KafkaDecoder::read_varint(&mut payload)?;
                    let v_bytes = if v_len >= 0 {
                        if payload.len() < v_len as usize {
                            return Err(OxideMqError::Protocol("Truncated header value".into()));
                        }
                        payload.copy_to_bytes(v_len as usize)
                    } else {
                        Bytes::new()
                    };

                    headers.push(RecordHeader::new(k_str, v_bytes));
                }

                records.push(Record {
                    offset: base_offset + off_delta,
                    timestamp: base_timestamp + ts_delta,
                    key,
                    value,
                    headers,
                });
            }

            cursor += total_batch_size;
        } else {
            break;
        }
    }

    Ok(records)
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
    pub isolation_level: i8,
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

        let isolation_level = if version >= 4 { src.get_i8() } else { 0 };
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
            isolation_level,
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
            dst.put_i8(self.isolation_level);
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

impl FetchPartitionResponse {
    /// Returns the decompressed records payload if the batch is compressed.
    pub fn decompressed_records(&self) -> Result<Bytes> {
        decompress_record_batch(&self.records)
    }

    /// Compresses or recompresses the records in this fetch partition response with the specified codec.
    pub fn compress_with(&mut self, codec: CompressionCodec) -> Result<()> {
        self.records = compress_record_batch(&self.records, codec)?;
        Ok(())
    }
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

// ==========================================
// InitProducerId (Key 22)
// ==========================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitProducerIdRequest {
    pub transactional_id: Option<String>,
    pub transaction_timeout_ms: i32,
    pub producer_id: i64,
    pub producer_epoch: i16,
}

impl InitProducerIdRequest {
    pub fn decode(src: &mut Bytes, version: i16) -> Result<Self> {
        let transactional_id = KafkaDecoder::read_string(src)?;
        let transaction_timeout_ms = src.get_i32();
        let (producer_id, producer_epoch) = if version >= 3 {
            (src.get_i64(), src.get_i16())
        } else {
            (-1, -1)
        };
        Ok(Self {
            transactional_id,
            transaction_timeout_ms,
            producer_id,
            producer_epoch,
        })
    }

    pub fn encode(&self, dst: &mut BytesMut, version: i16) {
        KafkaEncoder::write_string(dst, self.transactional_id.as_deref());
        dst.put_i32(self.transaction_timeout_ms);
        if version >= 3 {
            dst.put_i64(self.producer_id);
            dst.put_i16(self.producer_epoch);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitProducerIdResponse {
    pub throttle_time_ms: i32,
    pub error_code: KafkaErrorCode,
    pub producer_id: i64,
    pub producer_epoch: i16,
}

impl InitProducerIdResponse {
    pub fn encode(&self, dst: &mut BytesMut, _version: i16) {
        dst.put_i32(self.throttle_time_ms);
        dst.put_i16(self.error_code.code());
        dst.put_i64(self.producer_id);
        dst.put_i16(self.producer_epoch);
    }

    pub fn decode(src: &mut Bytes, _version: i16) -> Result<Self> {
        let throttle_time_ms = src.get_i32();
        let error_code = KafkaErrorCode::from_i16(src.get_i16());
        let producer_id = src.get_i64();
        let producer_epoch = src.get_i16();
        Ok(Self {
            throttle_time_ms,
            error_code,
            producer_id,
            producer_epoch,
        })
    }
}

// ==========================================
// AddPartitionsToTxn (Key 24)
// ==========================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddPartitionsToTxnTopic {
    pub name: String,
    pub partitions: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddPartitionsToTxnRequest {
    pub transactional_id: String,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub topics: Vec<AddPartitionsToTxnTopic>,
}

impl AddPartitionsToTxnRequest {
    pub fn decode(src: &mut Bytes, _version: i16) -> Result<Self> {
        let transactional_id = KafkaDecoder::read_string(src)?.unwrap_or_default();
        let producer_id = src.get_i64();
        let producer_epoch = src.get_i16();
        let topic_count = src.get_i32() as usize;
        let mut topics = Vec::with_capacity(topic_count);
        for _ in 0..topic_count {
            let name = KafkaDecoder::read_string(src)?.unwrap_or_default();
            let p_count = src.get_i32() as usize;
            let mut partitions = Vec::with_capacity(p_count);
            for _ in 0..p_count {
                partitions.push(src.get_i32());
            }
            topics.push(AddPartitionsToTxnTopic { name, partitions });
        }
        Ok(Self {
            transactional_id,
            producer_id,
            producer_epoch,
            topics,
        })
    }

    pub fn encode(&self, dst: &mut BytesMut, _version: i16) {
        KafkaEncoder::write_string(dst, Some(&self.transactional_id));
        dst.put_i64(self.producer_id);
        dst.put_i16(self.producer_epoch);
        dst.put_i32(self.topics.len() as i32);
        for t in &self.topics {
            KafkaEncoder::write_string(dst, Some(&t.name));
            dst.put_i32(t.partitions.len() as i32);
            for &p in &t.partitions {
                dst.put_i32(p);
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddPartitionsToTxnPartitionResult {
    pub partition_index: i32,
    pub error_code: KafkaErrorCode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddPartitionsToTxnTopicResult {
    pub name: String,
    pub results: Vec<AddPartitionsToTxnPartitionResult>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddPartitionsToTxnResponse {
    pub throttle_time_ms: i32,
    pub errors: Vec<AddPartitionsToTxnTopicResult>,
}

impl AddPartitionsToTxnResponse {
    pub fn encode(&self, dst: &mut BytesMut, _version: i16) {
        dst.put_i32(self.throttle_time_ms);
        dst.put_i32(self.errors.len() as i32);
        for t in &self.errors {
            KafkaEncoder::write_string(dst, Some(&t.name));
            dst.put_i32(t.results.len() as i32);
            for p in &t.results {
                dst.put_i32(p.partition_index);
                dst.put_i16(p.error_code.code());
            }
        }
    }

    pub fn decode(src: &mut Bytes, _version: i16) -> Result<Self> {
        let throttle_time_ms = src.get_i32();
        let topic_count = src.get_i32() as usize;
        let mut errors = Vec::with_capacity(topic_count);
        for _ in 0..topic_count {
            let name = KafkaDecoder::read_string(src)?.unwrap_or_default();
            let p_count = src.get_i32() as usize;
            let mut results = Vec::with_capacity(p_count);
            for _ in 0..p_count {
                let partition_index = src.get_i32();
                let error_code = KafkaErrorCode::from_i16(src.get_i16());
                results.push(AddPartitionsToTxnPartitionResult {
                    partition_index,
                    error_code,
                });
            }
            errors.push(AddPartitionsToTxnTopicResult { name, results });
        }
        Ok(Self {
            throttle_time_ms,
            errors,
        })
    }
}

// ==========================================
// AddOffsetsToTxn (Key 25)
// ==========================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddOffsetsToTxnRequest {
    pub transactional_id: String,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub group_id: String,
}

impl AddOffsetsToTxnRequest {
    pub fn decode(src: &mut Bytes, _version: i16) -> Result<Self> {
        let transactional_id = KafkaDecoder::read_string(src)?.unwrap_or_default();
        let producer_id = src.get_i64();
        let producer_epoch = src.get_i16();
        let group_id = KafkaDecoder::read_string(src)?.unwrap_or_default();
        Ok(Self {
            transactional_id,
            producer_id,
            producer_epoch,
            group_id,
        })
    }

    pub fn encode(&self, dst: &mut BytesMut, _version: i16) {
        KafkaEncoder::write_string(dst, Some(&self.transactional_id));
        dst.put_i64(self.producer_id);
        dst.put_i16(self.producer_epoch);
        KafkaEncoder::write_string(dst, Some(&self.group_id));
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddOffsetsToTxnResponse {
    pub throttle_time_ms: i32,
    pub error_code: KafkaErrorCode,
}

impl AddOffsetsToTxnResponse {
    pub fn encode(&self, dst: &mut BytesMut, _version: i16) {
        dst.put_i32(self.throttle_time_ms);
        dst.put_i16(self.error_code.code());
    }

    pub fn decode(src: &mut Bytes, _version: i16) -> Result<Self> {
        let throttle_time_ms = src.get_i32();
        let error_code = KafkaErrorCode::from_i16(src.get_i16());
        Ok(Self {
            throttle_time_ms,
            error_code,
        })
    }
}

// ==========================================
// EndTxn (Key 26)
// ==========================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndTxnRequest {
    pub transactional_id: String,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub committed: bool,
}

impl EndTxnRequest {
    pub fn decode(src: &mut Bytes, _version: i16) -> Result<Self> {
        let transactional_id = KafkaDecoder::read_string(src)?.unwrap_or_default();
        let producer_id = src.get_i64();
        let producer_epoch = src.get_i16();
        let committed = src.get_i8() != 0;
        Ok(Self {
            transactional_id,
            producer_id,
            producer_epoch,
            committed,
        })
    }

    pub fn encode(&self, dst: &mut BytesMut, _version: i16) {
        KafkaEncoder::write_string(dst, Some(&self.transactional_id));
        dst.put_i64(self.producer_id);
        dst.put_i16(self.producer_epoch);
        dst.put_i8(if self.committed { 1 } else { 0 });
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndTxnResponse {
    pub throttle_time_ms: i32,
    pub error_code: KafkaErrorCode,
}

impl EndTxnResponse {
    pub fn encode(&self, dst: &mut BytesMut, _version: i16) {
        dst.put_i32(self.throttle_time_ms);
        dst.put_i16(self.error_code.code());
    }

    pub fn decode(src: &mut Bytes, _version: i16) -> Result<Self> {
        let throttle_time_ms = src.get_i32();
        let error_code = KafkaErrorCode::from_i16(src.get_i16());
        Ok(Self {
            throttle_time_ms,
            error_code,
        })
    }
}

/// Encodes a control batch (e.g. EndTxn Commit or Abort) for a given partition, producer ID, and epoch.
pub fn encode_control_batch(
    base_offset: i64,
    producer_id: i64,
    producer_epoch: i16,
    is_commit: bool,
) -> Bytes {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    let control_type: i16 = if is_commit { 1 } else { 0 };

    let mut rec_body = BytesMut::new();
    rec_body.put_i8(0);
    KafkaEncoder::write_varint(&mut rec_body, 0); // timestamp delta
    KafkaEncoder::write_varint(&mut rec_body, 0); // offset delta

    // Key: 4 bytes: version (0), type (0 = abort, 1 = commit)
    KafkaEncoder::write_varint(&mut rec_body, 4);
    rec_body.put_i16(0);
    rec_body.put_i16(control_type);

    // Value: 2 bytes: version (0)
    KafkaEncoder::write_varint(&mut rec_body, 2);
    rec_body.put_i16(0);

    // Headers count: 0
    KafkaEncoder::write_varint(&mut rec_body, 0);

    let mut payload = BytesMut::new();
    KafkaEncoder::write_varint(&mut payload, rec_body.len() as i64);
    payload.extend_from_slice(&rec_body);

    let batch_len = (49 + payload.len()) as i32;
    let mut out = BytesMut::with_capacity(12 + batch_len as usize);
    out.put_i64(base_offset);
    out.put_i32(batch_len);
    out.put_i32(0); // partition leader epoch
    out.put_i8(2); // magic v2
    out.put_u32(0); // crc placeholder

    // Attributes: 0x0030 = is_control_batch (0x0020) | is_transactional (0x0010)
    out.put_i16(0x0030);
    out.put_i32(0); // last offset delta
    out.put_i64(now); // base timestamp
    out.put_i64(now); // max timestamp
    out.put_i64(producer_id);
    out.put_i16(producer_epoch);
    out.put_i32(-1); // base sequence
    out.put_i32(1); // record count = 1
    out.extend_from_slice(&payload);

    let crc = oxidemq_core::compute_crc32c(&out[21..]);
    out[17..21].copy_from_slice(&crc.to_be_bytes());

    out.freeze()
}

// ==========================================
// SaslHandshake (Key 17)
// ==========================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SaslHandshakeRequest {
    pub mechanism: String,
}

impl SaslHandshakeRequest {
    pub fn decode(src: &mut Bytes, _version: i16) -> Result<Self> {
        let mechanism = KafkaDecoder::read_string(src)?
            .ok_or_else(|| OxideMqError::Protocol("Missing SASL mechanism".into()))?;
        Ok(Self { mechanism })
    }

    pub fn encode(&self, dst: &mut BytesMut, _version: i16) {
        KafkaEncoder::write_string(dst, Some(&self.mechanism));
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SaslHandshakeResponse {
    pub error_code: KafkaErrorCode,
    pub enabled_mechanisms: Vec<String>,
}

impl SaslHandshakeResponse {
    pub fn decode(src: &mut Bytes, _version: i16) -> Result<Self> {
        if src.len() < 2 {
            return Err(OxideMqError::Protocol(
                "Truncated SaslHandshakeResponse error code".into(),
            ));
        }
        let error_code = KafkaErrorCode::from_i16(src.get_i16());
        if src.len() < 4 {
            return Err(OxideMqError::Protocol(
                "Truncated SaslHandshakeResponse array count".into(),
            ));
        }
        let count = src.get_i32();
        let mut enabled_mechanisms = Vec::new();
        if count > 0 {
            for _ in 0..count {
                if let Some(mech) = KafkaDecoder::read_string(src)? {
                    enabled_mechanisms.push(mech);
                }
            }
        }
        Ok(Self {
            error_code,
            enabled_mechanisms,
        })
    }

    pub fn encode(&self, dst: &mut BytesMut, _version: i16) {
        dst.put_i16(self.error_code.code());
        dst.put_i32(self.enabled_mechanisms.len() as i32);
        for mech in &self.enabled_mechanisms {
            KafkaEncoder::write_string(dst, Some(mech));
        }
    }
}

// ==========================================
// SaslAuthenticate (Key 36)
// ==========================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SaslAuthenticateRequest {
    pub auth_bytes: Bytes,
}

impl SaslAuthenticateRequest {
    pub fn decode(src: &mut Bytes, version: i16) -> Result<Self> {
        let bytes = if version >= 2 {
            KafkaDecoder::read_compact_bytes(src)?
        } else {
            KafkaDecoder::read_bytes(src)?
        };
        let auth_bytes = bytes.unwrap_or_default();
        if version >= 2 && src.has_remaining() {
            let _tag_count = KafkaDecoder::read_unsigned_varint(src)?;
        }
        Ok(Self { auth_bytes })
    }

    pub fn encode(&self, dst: &mut BytesMut, version: i16) {
        if version >= 2 {
            KafkaEncoder::write_compact_bytes(dst, Some(&self.auth_bytes));
            KafkaEncoder::write_unsigned_varint(dst, 0); // tagged fields
        } else {
            KafkaEncoder::write_bytes(dst, Some(&self.auth_bytes));
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SaslAuthenticateResponse {
    pub error_code: KafkaErrorCode,
    pub error_message: Option<String>,
    pub auth_bytes: Bytes,
    pub session_lifetime_ms: i64,
}

impl SaslAuthenticateResponse {
    pub fn decode(src: &mut Bytes, version: i16) -> Result<Self> {
        if src.len() < 2 {
            return Err(OxideMqError::Protocol(
                "Truncated SaslAuthenticateResponse error code".into(),
            ));
        }
        let error_code = KafkaErrorCode::from_i16(src.get_i16());
        let error_message = if version >= 2 {
            KafkaDecoder::read_compact_string(src)?
        } else {
            KafkaDecoder::read_string(src)?
        };
        let auth_bytes = if version >= 2 {
            KafkaDecoder::read_compact_bytes(src)?.unwrap_or_default()
        } else {
            KafkaDecoder::read_bytes(src)?.unwrap_or_default()
        };
        let session_lifetime_ms = if version >= 1 {
            if src.len() < 8 {
                return Err(OxideMqError::Protocol(
                    "Truncated session_lifetime_ms".into(),
                ));
            }
            src.get_i64()
        } else {
            0
        };
        if version >= 2 && src.has_remaining() {
            let _tag_count = KafkaDecoder::read_unsigned_varint(src)?;
        }
        Ok(Self {
            error_code,
            error_message,
            auth_bytes,
            session_lifetime_ms,
        })
    }

    pub fn encode(&self, dst: &mut BytesMut, version: i16) {
        dst.put_i16(self.error_code.code());
        if version >= 2 {
            KafkaEncoder::write_compact_string(dst, self.error_message.as_deref());
            KafkaEncoder::write_compact_bytes(dst, Some(&self.auth_bytes));
            dst.put_i64(self.session_lifetime_ms);
            KafkaEncoder::write_unsigned_varint(dst, 0); // tagged fields
        } else {
            KafkaEncoder::write_string(dst, self.error_message.as_deref());
            KafkaEncoder::write_bytes(dst, Some(&self.auth_bytes));
            if version >= 1 {
                dst.put_i64(self.session_lifetime_ms);
            }
        }
    }
}

// ============================================================================
// ACL Definitions & Codecs (ApiKeys 29 DescribeAcls, 30 CreateAcls, 31 DeleteAcls)
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(i8)]
pub enum AclResourceType {
    #[default]
    Unknown = 0,
    Any = 1,
    Topic = 2,
    Group = 3,
    Cluster = 4,
    TransactionalId = 5,
    DelegationToken = 6,
    User = 7,
}

impl AclResourceType {
    pub fn from_i8(val: i8) -> Self {
        match val {
            1 => Self::Any,
            2 => Self::Topic,
            3 => Self::Group,
            4 => Self::Cluster,
            5 => Self::TransactionalId,
            6 => Self::DelegationToken,
            7 => Self::User,
            _ => Self::Unknown,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(i8)]
pub enum AclResourcePatternType {
    #[default]
    Unknown = 0,
    Any = 1,
    Match = 2,
    Literal = 3,
    Prefixed = 4,
}

impl AclResourcePatternType {
    pub fn from_i8(val: i8) -> Self {
        match val {
            1 => Self::Any,
            2 => Self::Match,
            3 => Self::Literal,
            4 => Self::Prefixed,
            _ => Self::Unknown,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(i8)]
pub enum AclOperation {
    #[default]
    Unknown = 0,
    Any = 1,
    All = 2,
    Read = 3,
    Write = 4,
    Create = 5,
    Delete = 6,
    Alter = 7,
    Describe = 8,
    ClusterAction = 9,
    DescribeConfigs = 10,
    AlterConfigs = 11,
    IdempotentWrite = 12,
}

impl AclOperation {
    pub fn from_i8(val: i8) -> Self {
        match val {
            1 => Self::Any,
            2 => Self::All,
            3 => Self::Read,
            4 => Self::Write,
            5 => Self::Create,
            6 => Self::Delete,
            7 => Self::Alter,
            8 => Self::Describe,
            9 => Self::ClusterAction,
            10 => Self::DescribeConfigs,
            11 => Self::AlterConfigs,
            12 => Self::IdempotentWrite,
            _ => Self::Unknown,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(i8)]
pub enum AclPermissionType {
    #[default]
    Unknown = 0,
    Any = 1,
    Deny = 2,
    Allow = 3,
}

impl AclPermissionType {
    pub fn from_i8(val: i8) -> Self {
        match val {
            1 => Self::Any,
            2 => Self::Deny,
            3 => Self::Allow,
            _ => Self::Unknown,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AclCreation {
    pub resource_type: i8,
    pub resource_name: String,
    pub resource_pattern_type: i8,
    pub principal: String,
    pub host: String,
    pub operation: i8,
    pub permission_type: i8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AclCreationResult {
    pub error_code: KafkaErrorCode,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateAclsRequest {
    pub creations: Vec<AclCreation>,
}

impl CreateAclsRequest {
    pub fn decode(src: &mut Bytes, version: i16) -> Result<Self> {
        let count = if version >= 2 {
            (KafkaDecoder::read_unsigned_varint(src)? as usize).saturating_sub(1)
        } else {
            if src.len() < 4 {
                return Err(OxideMqError::Protocol(
                    "Truncated CreateAclsRequest count".into(),
                ));
            }
            src.get_i32() as usize
        };

        let mut creations = Vec::with_capacity(count);
        for _ in 0..count {
            if src.is_empty() {
                return Err(OxideMqError::Protocol(
                    "Truncated AclCreation resource_type".into(),
                ));
            }
            let resource_type = src.get_i8();
            let resource_name = if version >= 2 {
                KafkaDecoder::read_compact_string(src)?.unwrap_or_default()
            } else {
                KafkaDecoder::read_string(src)?.unwrap_or_default()
            };
            let resource_pattern_type = if version >= 1 {
                if src.is_empty() {
                    return Err(OxideMqError::Protocol(
                        "Truncated resource_pattern_type".into(),
                    ));
                }
                src.get_i8()
            } else {
                3 // Literal
            };
            let principal = if version >= 2 {
                KafkaDecoder::read_compact_string(src)?.unwrap_or_default()
            } else {
                KafkaDecoder::read_string(src)?.unwrap_or_default()
            };
            let host = if version >= 2 {
                KafkaDecoder::read_compact_string(src)?.unwrap_or_default()
            } else {
                KafkaDecoder::read_string(src)?.unwrap_or_default()
            };
            if src.len() < 2 {
                return Err(OxideMqError::Protocol(
                    "Truncated operation / permission_type".into(),
                ));
            }
            let operation = src.get_i8();
            let permission_type = src.get_i8();

            if version >= 2 && src.has_remaining() {
                let _tagged = KafkaDecoder::read_unsigned_varint(src)?;
            }

            creations.push(AclCreation {
                resource_type,
                resource_name,
                resource_pattern_type,
                principal,
                host,
                operation,
                permission_type,
            });
        }

        if version >= 2 && src.has_remaining() {
            let _tagged = KafkaDecoder::read_unsigned_varint(src)?;
        }

        Ok(Self { creations })
    }

    pub fn encode(&self, dst: &mut BytesMut, version: i16) {
        if version >= 2 {
            KafkaEncoder::write_unsigned_varint(dst, (self.creations.len() + 1) as u64);
        } else {
            dst.put_i32(self.creations.len() as i32);
        }

        for c in &self.creations {
            dst.put_i8(c.resource_type);
            if version >= 2 {
                KafkaEncoder::write_compact_string(dst, Some(&c.resource_name));
            } else {
                KafkaEncoder::write_string(dst, Some(&c.resource_name));
            }
            if version >= 1 {
                dst.put_i8(c.resource_pattern_type);
            }
            if version >= 2 {
                KafkaEncoder::write_compact_string(dst, Some(&c.principal));
                KafkaEncoder::write_compact_string(dst, Some(&c.host));
            } else {
                KafkaEncoder::write_string(dst, Some(&c.principal));
                KafkaEncoder::write_string(dst, Some(&c.host));
            }
            dst.put_i8(c.operation);
            dst.put_i8(c.permission_type);
            if version >= 2 {
                KafkaEncoder::write_unsigned_varint(dst, 0); // tagged fields
            }
        }

        if version >= 2 {
            KafkaEncoder::write_unsigned_varint(dst, 0); // tagged fields
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateAclsResponse {
    pub throttle_time_ms: i32,
    pub results: Vec<AclCreationResult>,
}

impl CreateAclsResponse {
    pub fn decode(src: &mut Bytes, version: i16) -> Result<Self> {
        let throttle_time_ms = if version >= 1 {
            if src.len() < 4 {
                return Err(OxideMqError::Protocol("Truncated throttle_time_ms".into()));
            }
            src.get_i32()
        } else {
            0
        };

        let count = if version >= 2 {
            (KafkaDecoder::read_unsigned_varint(src)? as usize).saturating_sub(1)
        } else {
            if src.len() < 4 {
                return Err(OxideMqError::Protocol("Truncated results count".into()));
            }
            src.get_i32() as usize
        };

        let mut results = Vec::with_capacity(count);
        for _ in 0..count {
            if src.len() < 2 {
                return Err(OxideMqError::Protocol("Truncated error_code".into()));
            }
            let error_code = KafkaErrorCode::from_i16(src.get_i16());
            let error_message = if version >= 2 {
                KafkaDecoder::read_compact_string(src)?
            } else {
                KafkaDecoder::read_string(src)?
            };
            if version >= 2 && src.has_remaining() {
                let _tagged = KafkaDecoder::read_unsigned_varint(src)?;
            }
            results.push(AclCreationResult {
                error_code,
                error_message,
            });
        }

        if version >= 2 && src.has_remaining() {
            let _tagged = KafkaDecoder::read_unsigned_varint(src)?;
        }

        Ok(Self {
            throttle_time_ms,
            results,
        })
    }

    pub fn encode(&self, dst: &mut BytesMut, version: i16) {
        if version >= 1 {
            dst.put_i32(self.throttle_time_ms);
        }

        if version >= 2 {
            KafkaEncoder::write_unsigned_varint(dst, (self.results.len() + 1) as u64);
        } else {
            dst.put_i32(self.results.len() as i32);
        }

        for r in &self.results {
            dst.put_i16(r.error_code.code());
            if version >= 2 {
                KafkaEncoder::write_compact_string(dst, r.error_message.as_deref());
                KafkaEncoder::write_unsigned_varint(dst, 0);
            } else {
                KafkaEncoder::write_string(dst, r.error_message.as_deref());
            }
        }

        if version >= 2 {
            KafkaEncoder::write_unsigned_varint(dst, 0);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeAclsRequest {
    pub resource_type_filter: i8,
    pub resource_name_filter: Option<String>,
    pub resource_pattern_type_filter: i8,
    pub principal_filter: Option<String>,
    pub host_filter: Option<String>,
    pub operation: i8,
    pub permission_type: i8,
}

impl DescribeAclsRequest {
    pub fn decode(src: &mut Bytes, version: i16) -> Result<Self> {
        if src.is_empty() {
            return Err(OxideMqError::Protocol(
                "Truncated DescribeAclsRequest resource_type".into(),
            ));
        }
        let resource_type_filter = src.get_i8();
        let resource_name_filter = if version >= 2 {
            KafkaDecoder::read_compact_string(src)?
        } else {
            KafkaDecoder::read_string(src)?
        };
        let resource_pattern_type_filter = if version >= 1 {
            if src.is_empty() {
                return Err(OxideMqError::Protocol(
                    "Truncated resource_pattern_type_filter".into(),
                ));
            }
            src.get_i8()
        } else {
            1 // Any
        };
        let principal_filter = if version >= 2 {
            KafkaDecoder::read_compact_string(src)?
        } else {
            KafkaDecoder::read_string(src)?
        };
        let host_filter = if version >= 2 {
            KafkaDecoder::read_compact_string(src)?
        } else {
            KafkaDecoder::read_string(src)?
        };
        if src.len() < 2 {
            return Err(OxideMqError::Protocol(
                "Truncated operation / permission_type".into(),
            ));
        }
        let operation = src.get_i8();
        let permission_type = src.get_i8();

        if version >= 2 && src.has_remaining() {
            let _tagged = KafkaDecoder::read_unsigned_varint(src)?;
        }

        Ok(Self {
            resource_type_filter,
            resource_name_filter,
            resource_pattern_type_filter,
            principal_filter,
            host_filter,
            operation,
            permission_type,
        })
    }

    pub fn encode(&self, dst: &mut BytesMut, version: i16) {
        dst.put_i8(self.resource_type_filter);
        if version >= 2 {
            KafkaEncoder::write_compact_string(dst, self.resource_name_filter.as_deref());
        } else {
            KafkaEncoder::write_string(dst, self.resource_name_filter.as_deref());
        }
        if version >= 1 {
            dst.put_i8(self.resource_pattern_type_filter);
        }
        if version >= 2 {
            KafkaEncoder::write_compact_string(dst, self.principal_filter.as_deref());
            KafkaEncoder::write_compact_string(dst, self.host_filter.as_deref());
        } else {
            KafkaEncoder::write_string(dst, self.principal_filter.as_deref());
            KafkaEncoder::write_string(dst, self.host_filter.as_deref());
        }
        dst.put_i8(self.operation);
        dst.put_i8(self.permission_type);
        if version >= 2 {
            KafkaEncoder::write_unsigned_varint(dst, 0); // tagged fields
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AclDescription {
    pub principal: String,
    pub host: String,
    pub operation: i8,
    pub permission_type: i8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeAclsResource {
    pub resource_type: i8,
    pub resource_name: String,
    pub resource_pattern_type: i8,
    pub acls: Vec<AclDescription>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeAclsResponse {
    pub throttle_time_ms: i32,
    pub error_code: KafkaErrorCode,
    pub error_message: Option<String>,
    pub resources: Vec<DescribeAclsResource>,
}

impl DescribeAclsResponse {
    pub fn decode(src: &mut Bytes, version: i16) -> Result<Self> {
        if src.len() < 6 {
            return Err(OxideMqError::Protocol(
                "Truncated DescribeAclsResponse header".into(),
            ));
        }
        let throttle_time_ms = src.get_i32();
        let error_code = KafkaErrorCode::from_i16(src.get_i16());
        let error_message = if version >= 2 {
            KafkaDecoder::read_compact_string(src)?
        } else {
            KafkaDecoder::read_string(src)?
        };

        let res_count = if version >= 2 {
            (KafkaDecoder::read_unsigned_varint(src)? as usize).saturating_sub(1)
        } else {
            if src.len() < 4 {
                return Err(OxideMqError::Protocol("Truncated resources count".into()));
            }
            src.get_i32() as usize
        };

        let mut resources = Vec::with_capacity(res_count);
        for _ in 0..res_count {
            if src.is_empty() {
                return Err(OxideMqError::Protocol("Truncated resource_type".into()));
            }
            let resource_type = src.get_i8();
            let resource_name = if version >= 2 {
                KafkaDecoder::read_compact_string(src)?.unwrap_or_default()
            } else {
                KafkaDecoder::read_string(src)?.unwrap_or_default()
            };
            let resource_pattern_type = if version >= 1 {
                if src.is_empty() {
                    return Err(OxideMqError::Protocol("Truncated pattern_type".into()));
                }
                src.get_i8()
            } else {
                3 // Literal
            };

            let acl_count = if version >= 2 {
                (KafkaDecoder::read_unsigned_varint(src)? as usize).saturating_sub(1)
            } else {
                if src.len() < 4 {
                    return Err(OxideMqError::Protocol("Truncated acls count".into()));
                }
                src.get_i32() as usize
            };

            let mut acls = Vec::with_capacity(acl_count);
            for _ in 0..acl_count {
                let principal = if version >= 2 {
                    KafkaDecoder::read_compact_string(src)?.unwrap_or_default()
                } else {
                    KafkaDecoder::read_string(src)?.unwrap_or_default()
                };
                let host = if version >= 2 {
                    KafkaDecoder::read_compact_string(src)?.unwrap_or_default()
                } else {
                    KafkaDecoder::read_string(src)?.unwrap_or_default()
                };
                if src.len() < 2 {
                    return Err(OxideMqError::Protocol(
                        "Truncated acl operation/permission".into(),
                    ));
                }
                let operation = src.get_i8();
                let permission_type = src.get_i8();

                if version >= 2 && src.has_remaining() {
                    let _tagged = KafkaDecoder::read_unsigned_varint(src)?;
                }

                acls.push(AclDescription {
                    principal,
                    host,
                    operation,
                    permission_type,
                });
            }

            if version >= 2 && src.has_remaining() {
                let _tagged = KafkaDecoder::read_unsigned_varint(src)?;
            }

            resources.push(DescribeAclsResource {
                resource_type,
                resource_name,
                resource_pattern_type,
                acls,
            });
        }

        if version >= 2 && src.has_remaining() {
            let _tagged = KafkaDecoder::read_unsigned_varint(src)?;
        }

        Ok(Self {
            throttle_time_ms,
            error_code,
            error_message,
            resources,
        })
    }

    pub fn encode(&self, dst: &mut BytesMut, version: i16) {
        dst.put_i32(self.throttle_time_ms);
        dst.put_i16(self.error_code.code());
        if version >= 2 {
            KafkaEncoder::write_compact_string(dst, self.error_message.as_deref());
            KafkaEncoder::write_unsigned_varint(dst, (self.resources.len() + 1) as u64);
        } else {
            KafkaEncoder::write_string(dst, self.error_message.as_deref());
            dst.put_i32(self.resources.len() as i32);
        }

        for res in &self.resources {
            dst.put_i8(res.resource_type);
            if version >= 2 {
                KafkaEncoder::write_compact_string(dst, Some(&res.resource_name));
            } else {
                KafkaEncoder::write_string(dst, Some(&res.resource_name));
            }
            if version >= 1 {
                dst.put_i8(res.resource_pattern_type);
            }

            if version >= 2 {
                KafkaEncoder::write_unsigned_varint(dst, (res.acls.len() + 1) as u64);
            } else {
                dst.put_i32(res.acls.len() as i32);
            }

            for acl in &res.acls {
                if version >= 2 {
                    KafkaEncoder::write_compact_string(dst, Some(&acl.principal));
                    KafkaEncoder::write_compact_string(dst, Some(&acl.host));
                } else {
                    KafkaEncoder::write_string(dst, Some(&acl.principal));
                    KafkaEncoder::write_string(dst, Some(&acl.host));
                }
                dst.put_i8(acl.operation);
                dst.put_i8(acl.permission_type);
                if version >= 2 {
                    KafkaEncoder::write_unsigned_varint(dst, 0); // tagged
                }
            }

            if version >= 2 {
                KafkaEncoder::write_unsigned_varint(dst, 0); // tagged
            }
        }

        if version >= 2 {
            KafkaEncoder::write_unsigned_varint(dst, 0); // tagged
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteAclsFilter {
    pub resource_type_filter: i8,
    pub resource_name_filter: Option<String>,
    pub resource_pattern_type_filter: i8,
    pub principal_filter: Option<String>,
    pub host_filter: Option<String>,
    pub operation: i8,
    pub permission_type: i8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteAclsMatchingAcl {
    pub error_code: KafkaErrorCode,
    pub error_message: Option<String>,
    pub resource_type: i8,
    pub resource_name: String,
    pub resource_pattern_type: i8,
    pub principal: String,
    pub host: String,
    pub operation: i8,
    pub permission_type: i8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteAclsFilterResult {
    pub error_code: KafkaErrorCode,
    pub error_message: Option<String>,
    pub matching_acls: Vec<DeleteAclsMatchingAcl>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteAclsRequest {
    pub filters: Vec<DeleteAclsFilter>,
}

impl DeleteAclsRequest {
    pub fn decode(src: &mut Bytes, version: i16) -> Result<Self> {
        let count = if version >= 2 {
            (KafkaDecoder::read_unsigned_varint(src)? as usize).saturating_sub(1)
        } else {
            if src.len() < 4 {
                return Err(OxideMqError::Protocol(
                    "Truncated DeleteAclsRequest filters count".into(),
                ));
            }
            src.get_i32() as usize
        };

        let mut filters = Vec::with_capacity(count);
        for _ in 0..count {
            if src.is_empty() {
                return Err(OxideMqError::Protocol(
                    "Truncated filter resource_type".into(),
                ));
            }
            let resource_type_filter = src.get_i8();
            let resource_name_filter = if version >= 2 {
                KafkaDecoder::read_compact_string(src)?
            } else {
                KafkaDecoder::read_string(src)?
            };
            let resource_pattern_type_filter = if version >= 1 {
                if src.is_empty() {
                    return Err(OxideMqError::Protocol(
                        "Truncated resource_pattern_type_filter".into(),
                    ));
                }
                src.get_i8()
            } else {
                1 // Any
            };
            let principal_filter = if version >= 2 {
                KafkaDecoder::read_compact_string(src)?
            } else {
                KafkaDecoder::read_string(src)?
            };
            let host_filter = if version >= 2 {
                KafkaDecoder::read_compact_string(src)?
            } else {
                KafkaDecoder::read_string(src)?
            };
            if src.len() < 2 {
                return Err(OxideMqError::Protocol(
                    "Truncated filter operation/permission".into(),
                ));
            }
            let operation = src.get_i8();
            let permission_type = src.get_i8();

            if version >= 2 && src.has_remaining() {
                let _tagged = KafkaDecoder::read_unsigned_varint(src)?;
            }

            filters.push(DeleteAclsFilter {
                resource_type_filter,
                resource_name_filter,
                resource_pattern_type_filter,
                principal_filter,
                host_filter,
                operation,
                permission_type,
            });
        }

        if version >= 2 && src.has_remaining() {
            let _tagged = KafkaDecoder::read_unsigned_varint(src)?;
        }

        Ok(Self { filters })
    }

    pub fn encode(&self, dst: &mut BytesMut, version: i16) {
        if version >= 2 {
            KafkaEncoder::write_unsigned_varint(dst, (self.filters.len() + 1) as u64);
        } else {
            dst.put_i32(self.filters.len() as i32);
        }

        for f in &self.filters {
            dst.put_i8(f.resource_type_filter);
            if version >= 2 {
                KafkaEncoder::write_compact_string(dst, f.resource_name_filter.as_deref());
            } else {
                KafkaEncoder::write_string(dst, f.resource_name_filter.as_deref());
            }
            if version >= 1 {
                dst.put_i8(f.resource_pattern_type_filter);
            }
            if version >= 2 {
                KafkaEncoder::write_compact_string(dst, f.principal_filter.as_deref());
                KafkaEncoder::write_compact_string(dst, f.host_filter.as_deref());
            } else {
                KafkaEncoder::write_string(dst, f.principal_filter.as_deref());
                KafkaEncoder::write_string(dst, f.host_filter.as_deref());
            }
            dst.put_i8(f.operation);
            dst.put_i8(f.permission_type);
            if version >= 2 {
                KafkaEncoder::write_unsigned_varint(dst, 0); // tagged
            }
        }

        if version >= 2 {
            KafkaEncoder::write_unsigned_varint(dst, 0); // tagged
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteAclsResponse {
    pub throttle_time_ms: i32,
    pub filter_results: Vec<DeleteAclsFilterResult>,
}

impl DeleteAclsResponse {
    pub fn decode(src: &mut Bytes, version: i16) -> Result<Self> {
        if src.len() < 4 {
            return Err(OxideMqError::Protocol(
                "Truncated DeleteAclsResponse throttle_time_ms".into(),
            ));
        }
        let throttle_time_ms = src.get_i32();

        let count = if version >= 2 {
            (KafkaDecoder::read_unsigned_varint(src)? as usize).saturating_sub(1)
        } else {
            if src.len() < 4 {
                return Err(OxideMqError::Protocol(
                    "Truncated filter_results count".into(),
                ));
            }
            src.get_i32() as usize
        };

        let mut filter_results = Vec::with_capacity(count);
        for _ in 0..count {
            if src.len() < 2 {
                return Err(OxideMqError::Protocol(
                    "Truncated filter_result error_code".into(),
                ));
            }
            let error_code = KafkaErrorCode::from_i16(src.get_i16());
            let error_message = if version >= 2 {
                KafkaDecoder::read_compact_string(src)?
            } else {
                KafkaDecoder::read_string(src)?
            };

            let match_count = if version >= 2 {
                (KafkaDecoder::read_unsigned_varint(src)? as usize).saturating_sub(1)
            } else {
                if src.len() < 4 {
                    return Err(OxideMqError::Protocol(
                        "Truncated matching_acls count".into(),
                    ));
                }
                src.get_i32() as usize
            };

            let mut matching_acls = Vec::with_capacity(match_count);
            for _ in 0..match_count {
                if src.len() < 2 {
                    return Err(OxideMqError::Protocol(
                        "Truncated matching_acl error_code".into(),
                    ));
                }
                let m_error_code = KafkaErrorCode::from_i16(src.get_i16());
                let m_error_message = if version >= 2 {
                    KafkaDecoder::read_compact_string(src)?
                } else {
                    KafkaDecoder::read_string(src)?
                };
                if src.is_empty() {
                    return Err(OxideMqError::Protocol(
                        "Truncated matching_acl resource_type".into(),
                    ));
                }
                let resource_type = src.get_i8();
                let resource_name = if version >= 2 {
                    KafkaDecoder::read_compact_string(src)?.unwrap_or_default()
                } else {
                    KafkaDecoder::read_string(src)?.unwrap_or_default()
                };
                let resource_pattern_type = if version >= 1 {
                    if src.is_empty() {
                        return Err(OxideMqError::Protocol("Truncated pattern_type".into()));
                    }
                    src.get_i8()
                } else {
                    3 // Literal
                };
                let principal = if version >= 2 {
                    KafkaDecoder::read_compact_string(src)?.unwrap_or_default()
                } else {
                    KafkaDecoder::read_string(src)?.unwrap_or_default()
                };
                let host = if version >= 2 {
                    KafkaDecoder::read_compact_string(src)?.unwrap_or_default()
                } else {
                    KafkaDecoder::read_string(src)?.unwrap_or_default()
                };
                if src.len() < 2 {
                    return Err(OxideMqError::Protocol(
                        "Truncated matching_acl operation/permission".into(),
                    ));
                }
                let operation = src.get_i8();
                let permission_type = src.get_i8();

                if version >= 2 && src.has_remaining() {
                    let _tagged = KafkaDecoder::read_unsigned_varint(src)?;
                }

                matching_acls.push(DeleteAclsMatchingAcl {
                    error_code: m_error_code,
                    error_message: m_error_message,
                    resource_type,
                    resource_name,
                    resource_pattern_type,
                    principal,
                    host,
                    operation,
                    permission_type,
                });
            }

            if version >= 2 && src.has_remaining() {
                let _tagged = KafkaDecoder::read_unsigned_varint(src)?;
            }

            filter_results.push(DeleteAclsFilterResult {
                error_code,
                error_message,
                matching_acls,
            });
        }

        if version >= 2 && src.has_remaining() {
            let _tagged = KafkaDecoder::read_unsigned_varint(src)?;
        }

        Ok(Self {
            throttle_time_ms,
            filter_results,
        })
    }

    pub fn encode(&self, dst: &mut BytesMut, version: i16) {
        dst.put_i32(self.throttle_time_ms);
        if version >= 2 {
            KafkaEncoder::write_unsigned_varint(dst, (self.filter_results.len() + 1) as u64);
        } else {
            dst.put_i32(self.filter_results.len() as i32);
        }

        for fr in &self.filter_results {
            dst.put_i16(fr.error_code.code());
            if version >= 2 {
                KafkaEncoder::write_compact_string(dst, fr.error_message.as_deref());
                KafkaEncoder::write_unsigned_varint(dst, (fr.matching_acls.len() + 1) as u64);
            } else {
                KafkaEncoder::write_string(dst, fr.error_message.as_deref());
                dst.put_i32(fr.matching_acls.len() as i32);
            }

            for ma in &fr.matching_acls {
                dst.put_i16(ma.error_code.code());
                if version >= 2 {
                    KafkaEncoder::write_compact_string(dst, ma.error_message.as_deref());
                } else {
                    KafkaEncoder::write_string(dst, ma.error_message.as_deref());
                }
                dst.put_i8(ma.resource_type);
                if version >= 2 {
                    KafkaEncoder::write_compact_string(dst, Some(&ma.resource_name));
                } else {
                    KafkaEncoder::write_string(dst, Some(&ma.resource_name));
                }
                if version >= 1 {
                    dst.put_i8(ma.resource_pattern_type);
                }
                if version >= 2 {
                    KafkaEncoder::write_compact_string(dst, Some(&ma.principal));
                    KafkaEncoder::write_compact_string(dst, Some(&ma.host));
                } else {
                    KafkaEncoder::write_string(dst, Some(&ma.principal));
                    KafkaEncoder::write_string(dst, Some(&ma.host));
                }
                dst.put_i8(ma.operation);
                dst.put_i8(ma.permission_type);

                if version >= 2 {
                    KafkaEncoder::write_unsigned_varint(dst, 0); // tagged
                }
            }

            if version >= 2 {
                KafkaEncoder::write_unsigned_varint(dst, 0); // tagged
            }
        }

        if version >= 2 {
            KafkaEncoder::write_unsigned_varint(dst, 0); // tagged
        }
    }
}

// ============================================================================
// Topic Management Definitions & Codecs (ApiKeys 19 CreateTopics, 20 DeleteTopics)
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTopicsReplicaAssignment {
    pub partition_index: i32,
    pub broker_ids: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTopicsConfig {
    pub name: String,
    pub value: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatableTopic {
    pub name: String,
    pub num_partitions: i32,
    pub replication_factor: i16,
    pub assignments: Vec<CreateTopicsReplicaAssignment>,
    pub configs: Vec<CreateTopicsConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTopicsRequest {
    pub topics: Vec<CreatableTopic>,
    pub timeout_ms: i32,
    pub validate_only: bool,
}

impl CreateTopicsRequest {
    pub fn decode(src: &mut Bytes, version: i16) -> Result<Self> {
        if src.len() < 4 {
            return Err(OxideMqError::Protocol(
                "Truncated CreateTopicsRequest topics count".into(),
            ));
        }
        let count = src.get_i32();
        if count < 0 {
            return Err(OxideMqError::Protocol("Negative topics count".into()));
        }
        let count = count as usize;
        let mut topics = Vec::with_capacity(count);

        for _ in 0..count {
            let name = KafkaDecoder::read_string(src)?.unwrap_or_default();
            if src.len() < 6 {
                return Err(OxideMqError::Protocol(
                    "Truncated CreatableTopic partitions/replication".into(),
                ));
            }
            let num_partitions = src.get_i32();
            let replication_factor = src.get_i16();

            if src.len() < 4 {
                return Err(OxideMqError::Protocol("Truncated assignments count".into()));
            }
            let assign_count = src.get_i32();
            let assign_count = if assign_count < 0 {
                0
            } else {
                assign_count as usize
            };
            let mut assignments = Vec::with_capacity(assign_count);
            for _ in 0..assign_count {
                if src.len() < 8 {
                    return Err(OxideMqError::Protocol("Truncated assignment data".into()));
                }
                let partition_index = src.get_i32();
                let brokers_count = src.get_i32();
                let brokers_count = if brokers_count < 0 {
                    0
                } else {
                    brokers_count as usize
                };
                if src.len() < brokers_count * 4 {
                    return Err(OxideMqError::Protocol("Truncated broker_ids".into()));
                }
                let mut broker_ids = Vec::with_capacity(brokers_count);
                for _ in 0..brokers_count {
                    broker_ids.push(src.get_i32());
                }
                assignments.push(CreateTopicsReplicaAssignment {
                    partition_index,
                    broker_ids,
                });
            }

            if src.len() < 4 {
                return Err(OxideMqError::Protocol("Truncated configs count".into()));
            }
            let config_count = src.get_i32();
            let config_count = if config_count < 0 {
                0
            } else {
                config_count as usize
            };
            let mut configs = Vec::with_capacity(config_count);
            for _ in 0..config_count {
                let c_name = KafkaDecoder::read_string(src)?.unwrap_or_default();
                let c_val = KafkaDecoder::read_string(src)?;
                configs.push(CreateTopicsConfig {
                    name: c_name,
                    value: c_val,
                });
            }

            topics.push(CreatableTopic {
                name,
                num_partitions,
                replication_factor,
                assignments,
                configs,
            });
        }

        if src.len() < 4 {
            return Err(OxideMqError::Protocol("Truncated timeout_ms".into()));
        }
        let timeout_ms = src.get_i32();
        let validate_only = if version >= 1 && src.has_remaining() {
            src.get_u8() != 0
        } else {
            false
        };

        Ok(Self {
            topics,
            timeout_ms,
            validate_only,
        })
    }

    pub fn encode(&self, dst: &mut BytesMut, version: i16) {
        dst.put_i32(self.topics.len() as i32);
        for t in &self.topics {
            KafkaEncoder::write_string(dst, Some(&t.name));
            dst.put_i32(t.num_partitions);
            dst.put_i16(t.replication_factor);

            dst.put_i32(t.assignments.len() as i32);
            for a in &t.assignments {
                dst.put_i32(a.partition_index);
                dst.put_i32(a.broker_ids.len() as i32);
                for b in &a.broker_ids {
                    dst.put_i32(*b);
                }
            }

            dst.put_i32(t.configs.len() as i32);
            for c in &t.configs {
                KafkaEncoder::write_string(dst, Some(&c.name));
                KafkaEncoder::write_string(dst, c.value.as_deref());
            }
        }

        dst.put_i32(self.timeout_ms);
        if version >= 1 {
            dst.put_u8(self.validate_only as u8);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatableTopicResult {
    pub name: String,
    pub error_code: KafkaErrorCode,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTopicsResponse {
    pub throttle_time_ms: i32,
    pub topics: Vec<CreatableTopicResult>,
}

impl CreateTopicsResponse {
    pub fn decode(src: &mut Bytes, version: i16) -> Result<Self> {
        let throttle_time_ms = if version >= 2 {
            if src.len() < 4 {
                return Err(OxideMqError::Protocol("Truncated throttle_time_ms".into()));
            }
            src.get_i32()
        } else {
            0
        };

        if src.len() < 4 {
            return Err(OxideMqError::Protocol("Truncated topics count".into()));
        }
        let count = src.get_i32();
        if count < 0 {
            return Err(OxideMqError::Protocol("Negative topics count".into()));
        }
        let count = count as usize;
        let mut topics = Vec::with_capacity(count);

        for _ in 0..count {
            let name = KafkaDecoder::read_string(src)?.unwrap_or_default();
            if src.len() < 2 {
                return Err(OxideMqError::Protocol("Truncated error_code".into()));
            }
            let error_code = KafkaErrorCode::from_i16(src.get_i16());
            let error_message = if version >= 1 {
                KafkaDecoder::read_string(src)?
            } else {
                None
            };
            topics.push(CreatableTopicResult {
                name,
                error_code,
                error_message,
            });
        }

        Ok(Self {
            throttle_time_ms,
            topics,
        })
    }

    pub fn encode(&self, dst: &mut BytesMut, version: i16) {
        if version >= 2 {
            dst.put_i32(self.throttle_time_ms);
        }
        dst.put_i32(self.topics.len() as i32);
        for t in &self.topics {
            KafkaEncoder::write_string(dst, Some(&t.name));
            dst.put_i16(t.error_code.code());
            if version >= 1 {
                KafkaEncoder::write_string(dst, t.error_message.as_deref());
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteTopicsRequest {
    pub topic_names: Vec<String>,
    pub timeout_ms: i32,
}

impl DeleteTopicsRequest {
    pub fn decode(src: &mut Bytes, _version: i16) -> Result<Self> {
        if src.len() < 4 {
            return Err(OxideMqError::Protocol(
                "Truncated DeleteTopicsRequest count".into(),
            ));
        }
        let count = src.get_i32();
        if count < 0 {
            return Err(OxideMqError::Protocol("Negative count".into()));
        }
        let count = count as usize;
        let mut topic_names = Vec::with_capacity(count);
        for _ in 0..count {
            if let Some(name) = KafkaDecoder::read_string(src)? {
                topic_names.push(name);
            }
        }
        if src.len() < 4 {
            return Err(OxideMqError::Protocol("Truncated timeout_ms".into()));
        }
        let timeout_ms = src.get_i32();

        Ok(Self {
            topic_names,
            timeout_ms,
        })
    }

    pub fn encode(&self, dst: &mut BytesMut, _version: i16) {
        dst.put_i32(self.topic_names.len() as i32);
        for name in &self.topic_names {
            KafkaEncoder::write_string(dst, Some(name));
        }
        dst.put_i32(self.timeout_ms);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeletableTopicResult {
    pub name: String,
    pub error_code: KafkaErrorCode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteTopicsResponse {
    pub throttle_time_ms: i32,
    pub responses: Vec<DeletableTopicResult>,
}

impl DeleteTopicsResponse {
    pub fn decode(src: &mut Bytes, version: i16) -> Result<Self> {
        let throttle_time_ms = if version >= 1 {
            if src.len() < 4 {
                return Err(OxideMqError::Protocol("Truncated throttle_time_ms".into()));
            }
            src.get_i32()
        } else {
            0
        };

        if src.len() < 4 {
            return Err(OxideMqError::Protocol("Truncated responses count".into()));
        }
        let count = src.get_i32();
        if count < 0 {
            return Err(OxideMqError::Protocol("Negative responses count".into()));
        }
        let count = count as usize;
        let mut responses = Vec::with_capacity(count);

        for _ in 0..count {
            let name = KafkaDecoder::read_string(src)?.unwrap_or_default();
            if src.len() < 2 {
                return Err(OxideMqError::Protocol("Truncated error_code".into()));
            }
            let error_code = KafkaErrorCode::from_i16(src.get_i16());
            responses.push(DeletableTopicResult { name, error_code });
        }

        Ok(Self {
            throttle_time_ms,
            responses,
        })
    }

    pub fn encode(&self, dst: &mut BytesMut, version: i16) {
        if version >= 1 {
            dst.put_i32(self.throttle_time_ms);
        }
        dst.put_i32(self.responses.len() as i32);
        for r in &self.responses {
            KafkaEncoder::write_string(dst, Some(&r.name));
            dst.put_i16(r.error_code.code());
        }
    }
}

// ==========================================
// DescribeConfigs (Key 32)
// ==========================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeConfigsResource {
    pub resource_type: i8,
    pub resource_name: String,
    pub configuration_keys: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeConfigsRequest {
    pub resources: Vec<DescribeConfigsResource>,
    pub include_synonyms: bool,
}

impl DescribeConfigsRequest {
    pub fn decode(src: &mut Bytes, version: i16) -> Result<Self> {
        if src.len() < 4 {
            return Err(OxideMqError::Protocol(
                "Truncated DescribeConfigsRequest resources count".into(),
            ));
        }
        let count = src.get_i32();
        if count < 0 {
            return Err(OxideMqError::Protocol("Negative resources count".into()));
        }
        let count = count as usize;
        let mut resources = Vec::with_capacity(count);
        for _ in 0..count {
            if src.is_empty() {
                return Err(OxideMqError::Protocol("Truncated resource_type".into()));
            }
            let resource_type = src.get_i8();
            let resource_name = KafkaDecoder::read_string(src)?.unwrap_or_default();
            if src.len() < 4 {
                return Err(OxideMqError::Protocol(
                    "Truncated configuration_keys count".into(),
                ));
            }
            let keys_count = src.get_i32();
            let configuration_keys = if keys_count < 0 {
                None
            } else {
                let mut keys = Vec::with_capacity(keys_count as usize);
                for _ in 0..keys_count {
                    if let Some(k) = KafkaDecoder::read_string(src)? {
                        keys.push(k);
                    }
                }
                Some(keys)
            };
            resources.push(DescribeConfigsResource {
                resource_type,
                resource_name,
                configuration_keys,
            });
        }
        let include_synonyms = if version >= 1 {
            if src.is_empty() {
                return Err(OxideMqError::Protocol("Truncated include_synonyms".into()));
            }
            src.get_u8() != 0
        } else {
            false
        };

        Ok(Self {
            resources,
            include_synonyms,
        })
    }

    pub fn encode(&self, dst: &mut BytesMut, version: i16) {
        dst.put_i32(self.resources.len() as i32);
        for r in &self.resources {
            dst.put_i8(r.resource_type);
            KafkaEncoder::write_string(dst, Some(&r.resource_name));
            match &r.configuration_keys {
                Some(keys) => {
                    dst.put_i32(keys.len() as i32);
                    for k in keys {
                        KafkaEncoder::write_string(dst, Some(k));
                    }
                }
                None => {
                    dst.put_i32(-1);
                }
            }
        }
        if version >= 1 {
            dst.put_u8(if self.include_synonyms { 1 } else { 0 });
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeConfigsSynonym {
    pub name: String,
    pub value: Option<String>,
    pub source: i8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeConfigsResourceResult {
    pub name: String,
    pub value: Option<String>,
    pub read_only: bool,
    pub is_default: bool,
    pub config_source: i8,
    pub is_sensitive: bool,
    pub synonyms: Vec<DescribeConfigsSynonym>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeConfigsResult {
    pub error_code: KafkaErrorCode,
    pub error_message: Option<String>,
    pub resource_type: i8,
    pub resource_name: String,
    pub configs: Vec<DescribeConfigsResourceResult>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeConfigsResponse {
    pub throttle_time_ms: i32,
    pub results: Vec<DescribeConfigsResult>,
}

impl DescribeConfigsResponse {
    pub fn decode(src: &mut Bytes, version: i16) -> Result<Self> {
        if src.len() < 4 {
            return Err(OxideMqError::Protocol("Truncated throttle_time_ms".into()));
        }
        let throttle_time_ms = src.get_i32();

        if src.len() < 4 {
            return Err(OxideMqError::Protocol("Truncated results count".into()));
        }
        let count = src.get_i32();
        if count < 0 {
            return Err(OxideMqError::Protocol("Negative results count".into()));
        }
        let count = count as usize;
        let mut results = Vec::with_capacity(count);

        for _ in 0..count {
            if src.len() < 2 {
                return Err(OxideMqError::Protocol("Truncated error_code".into()));
            }
            let error_code = KafkaErrorCode::from_i16(src.get_i16());
            let error_message = KafkaDecoder::read_string(src)?;
            if src.is_empty() {
                return Err(OxideMqError::Protocol("Truncated resource_type".into()));
            }
            let resource_type = src.get_i8();
            let resource_name = KafkaDecoder::read_string(src)?.unwrap_or_default();

            if src.len() < 4 {
                return Err(OxideMqError::Protocol("Truncated configs count".into()));
            }
            let configs_count = src.get_i32();
            if configs_count < 0 {
                return Err(OxideMqError::Protocol("Negative configs count".into()));
            }
            let configs_count = configs_count as usize;
            let mut configs = Vec::with_capacity(configs_count);

            for _ in 0..configs_count {
                let name = KafkaDecoder::read_string(src)?.unwrap_or_default();
                let value = KafkaDecoder::read_string(src)?;
                if src.is_empty() {
                    return Err(OxideMqError::Protocol("Truncated read_only".into()));
                }
                let read_only = src.get_u8() != 0;

                let (is_default, config_source) = if version == 0 {
                    if src.is_empty() {
                        return Err(OxideMqError::Protocol("Truncated is_default".into()));
                    }
                    (src.get_u8() != 0, -1i8)
                } else {
                    if src.is_empty() {
                        return Err(OxideMqError::Protocol("Truncated config_source".into()));
                    }
                    (false, src.get_i8())
                };

                if src.is_empty() {
                    return Err(OxideMqError::Protocol("Truncated is_sensitive".into()));
                }
                let is_sensitive = src.get_u8() != 0;

                let synonyms = if version >= 1 {
                    if src.len() < 4 {
                        return Err(OxideMqError::Protocol("Truncated synonyms count".into()));
                    }
                    let syn_count = src.get_i32();
                    if syn_count < 0 {
                        Vec::new()
                    } else {
                        let mut syns = Vec::with_capacity(syn_count as usize);
                        for _ in 0..syn_count {
                            let syn_name = KafkaDecoder::read_string(src)?.unwrap_or_default();
                            let syn_value = KafkaDecoder::read_string(src)?;
                            if src.is_empty() {
                                return Err(OxideMqError::Protocol(
                                    "Truncated synonym source".into(),
                                ));
                            }
                            let source = src.get_i8();
                            syns.push(DescribeConfigsSynonym {
                                name: syn_name,
                                value: syn_value,
                                source,
                            });
                        }
                        syns
                    }
                } else {
                    Vec::new()
                };

                configs.push(DescribeConfigsResourceResult {
                    name,
                    value,
                    read_only,
                    is_default,
                    config_source,
                    is_sensitive,
                    synonyms,
                });
            }

            results.push(DescribeConfigsResult {
                error_code,
                error_message,
                resource_type,
                resource_name,
                configs,
            });
        }

        Ok(Self {
            throttle_time_ms,
            results,
        })
    }

    pub fn encode(&self, dst: &mut BytesMut, version: i16) {
        dst.put_i32(self.throttle_time_ms);
        dst.put_i32(self.results.len() as i32);
        for res in &self.results {
            dst.put_i16(res.error_code.code());
            KafkaEncoder::write_string(dst, res.error_message.as_deref());
            dst.put_i8(res.resource_type);
            KafkaEncoder::write_string(dst, Some(&res.resource_name));

            dst.put_i32(res.configs.len() as i32);
            for c in &res.configs {
                KafkaEncoder::write_string(dst, Some(&c.name));
                KafkaEncoder::write_string(dst, c.value.as_deref());
                dst.put_u8(if c.read_only { 1 } else { 0 });
                if version == 0 {
                    dst.put_u8(if c.is_default { 1 } else { 0 });
                } else {
                    dst.put_i8(c.config_source);
                }
                dst.put_u8(if c.is_sensitive { 1 } else { 0 });
                if version >= 1 {
                    dst.put_i32(c.synonyms.len() as i32);
                    for s in &c.synonyms {
                        KafkaEncoder::write_string(dst, Some(&s.name));
                        KafkaEncoder::write_string(dst, s.value.as_deref());
                        dst.put_i8(s.source);
                    }
                }
            }
        }
    }
}

// ==========================================
// AlterConfigs (Key 33)
// ==========================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterableConfig {
    pub name: String,
    pub value: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterConfigsResource {
    pub resource_type: i8,
    pub resource_name: String,
    pub configs: Vec<AlterableConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterConfigsRequest {
    pub resources: Vec<AlterConfigsResource>,
    pub validate_only: bool,
}

impl AlterConfigsRequest {
    pub fn decode(src: &mut Bytes, _version: i16) -> Result<Self> {
        if src.len() < 4 {
            return Err(OxideMqError::Protocol(
                "Truncated AlterConfigsRequest resources count".into(),
            ));
        }
        let count = src.get_i32();
        if count < 0 {
            return Err(OxideMqError::Protocol("Negative resources count".into()));
        }
        let count = count as usize;
        let mut resources = Vec::with_capacity(count);

        for _ in 0..count {
            if src.is_empty() {
                return Err(OxideMqError::Protocol("Truncated resource_type".into()));
            }
            let resource_type = src.get_i8();
            let resource_name = KafkaDecoder::read_string(src)?.unwrap_or_default();
            if src.len() < 4 {
                return Err(OxideMqError::Protocol("Truncated configs count".into()));
            }
            let configs_count = src.get_i32();
            if configs_count < 0 {
                return Err(OxideMqError::Protocol("Negative configs count".into()));
            }
            let configs_count = configs_count as usize;
            let mut configs = Vec::with_capacity(configs_count);
            for _ in 0..configs_count {
                let name = KafkaDecoder::read_string(src)?.unwrap_or_default();
                let value = KafkaDecoder::read_string(src)?;
                configs.push(AlterableConfig { name, value });
            }
            resources.push(AlterConfigsResource {
                resource_type,
                resource_name,
                configs,
            });
        }

        if src.is_empty() {
            return Err(OxideMqError::Protocol("Truncated validate_only".into()));
        }
        let validate_only = src.get_u8() != 0;

        Ok(Self {
            resources,
            validate_only,
        })
    }

    pub fn encode(&self, dst: &mut BytesMut, _version: i16) {
        dst.put_i32(self.resources.len() as i32);
        for r in &self.resources {
            dst.put_i8(r.resource_type);
            KafkaEncoder::write_string(dst, Some(&r.resource_name));
            dst.put_i32(r.configs.len() as i32);
            for c in &r.configs {
                KafkaEncoder::write_string(dst, Some(&c.name));
                KafkaEncoder::write_string(dst, c.value.as_deref());
            }
        }
        dst.put_u8(if self.validate_only { 1 } else { 0 });
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterConfigsResourceResponse {
    pub error_code: KafkaErrorCode,
    pub error_message: Option<String>,
    pub resource_type: i8,
    pub resource_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterConfigsResponse {
    pub throttle_time_ms: i32,
    pub responses: Vec<AlterConfigsResourceResponse>,
}

impl AlterConfigsResponse {
    pub fn decode(src: &mut Bytes, _version: i16) -> Result<Self> {
        if src.len() < 4 {
            return Err(OxideMqError::Protocol("Truncated throttle_time_ms".into()));
        }
        let throttle_time_ms = src.get_i32();

        if src.len() < 4 {
            return Err(OxideMqError::Protocol("Truncated responses count".into()));
        }
        let count = src.get_i32();
        if count < 0 {
            return Err(OxideMqError::Protocol("Negative responses count".into()));
        }
        let count = count as usize;
        let mut responses = Vec::with_capacity(count);

        for _ in 0..count {
            if src.len() < 2 {
                return Err(OxideMqError::Protocol("Truncated error_code".into()));
            }
            let error_code = KafkaErrorCode::from_i16(src.get_i16());
            let error_message = KafkaDecoder::read_string(src)?;
            if src.is_empty() {
                return Err(OxideMqError::Protocol("Truncated resource_type".into()));
            }
            let resource_type = src.get_i8();
            let resource_name = KafkaDecoder::read_string(src)?.unwrap_or_default();
            responses.push(AlterConfigsResourceResponse {
                error_code,
                error_message,
                resource_type,
                resource_name,
            });
        }

        Ok(Self {
            throttle_time_ms,
            responses,
        })
    }

    pub fn encode(&self, dst: &mut BytesMut, _version: i16) {
        dst.put_i32(self.throttle_time_ms);
        dst.put_i32(self.responses.len() as i32);
        for r in &self.responses {
            dst.put_i16(r.error_code.code());
            KafkaEncoder::write_string(dst, r.error_message.as_deref());
            dst.put_i8(r.resource_type);
            KafkaEncoder::write_string(dst, Some(&r.resource_name));
        }
    }
}

// ==========================================
// ListGroups (Key 16)
// ==========================================

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ListGroupsRequest {}

impl ListGroupsRequest {
    pub fn decode(_src: &mut Bytes, _version: i16) -> Result<Self> {
        Ok(Self {})
    }

    pub fn encode(&self, _dst: &mut BytesMut, _version: i16) {}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedGroup {
    pub group_id: String,
    pub protocol_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListGroupsResponse {
    pub throttle_time_ms: i32,
    pub error_code: KafkaErrorCode,
    pub groups: Vec<ListedGroup>,
}

impl ListGroupsResponse {
    pub fn decode(src: &mut Bytes, version: i16) -> Result<Self> {
        let throttle_time_ms = if version >= 1 {
            if src.len() < 4 {
                return Err(OxideMqError::Protocol("Truncated throttle_time_ms".into()));
            }
            src.get_i32()
        } else {
            0
        };

        if src.len() < 2 {
            return Err(OxideMqError::Protocol("Truncated error_code".into()));
        }
        let error_code = KafkaErrorCode::from_i16(src.get_i16());

        if src.len() < 4 {
            return Err(OxideMqError::Protocol("Truncated groups count".into()));
        }
        let count = src.get_i32();
        if count < 0 {
            return Err(OxideMqError::Protocol("Negative groups count".into()));
        }
        let count = count as usize;
        let mut groups = Vec::with_capacity(count);

        for _ in 0..count {
            let group_id = KafkaDecoder::read_string(src)?.unwrap_or_default();
            let protocol_type = KafkaDecoder::read_string(src)?.unwrap_or_default();
            groups.push(ListedGroup {
                group_id,
                protocol_type,
            });
        }

        Ok(Self {
            throttle_time_ms,
            error_code,
            groups,
        })
    }

    pub fn encode(&self, dst: &mut BytesMut, version: i16) {
        if version >= 1 {
            dst.put_i32(self.throttle_time_ms);
        }
        dst.put_i16(self.error_code.code());
        dst.put_i32(self.groups.len() as i32);
        for g in &self.groups {
            KafkaEncoder::write_string(dst, Some(&g.group_id));
            KafkaEncoder::write_string(dst, Some(&g.protocol_type));
        }
    }
}

// ==========================================
// DescribeGroups (Key 15)
// ==========================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeGroupsRequest {
    pub groups: Vec<String>,
}

impl DescribeGroupsRequest {
    pub fn decode(src: &mut Bytes, _version: i16) -> Result<Self> {
        if src.len() < 4 {
            return Err(OxideMqError::Protocol(
                "Truncated DescribeGroupsRequest groups count".into(),
            ));
        }
        let count = src.get_i32();
        if count < 0 {
            return Err(OxideMqError::Protocol("Negative groups count".into()));
        }
        let count = count as usize;
        let mut groups = Vec::with_capacity(count);
        for _ in 0..count {
            if let Some(g) = KafkaDecoder::read_string(src)? {
                groups.push(g);
            }
        }
        Ok(Self { groups })
    }

    pub fn encode(&self, dst: &mut BytesMut, _version: i16) {
        dst.put_i32(self.groups.len() as i32);
        for g in &self.groups {
            KafkaEncoder::write_string(dst, Some(g));
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedGroupMember {
    pub member_id: String,
    pub client_id: String,
    pub client_host: String,
    pub member_metadata: Bytes,
    pub member_assignment: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedGroup {
    pub error_code: KafkaErrorCode,
    pub group_id: String,
    pub group_state: String,
    pub protocol_type: String,
    pub protocol_data: String,
    pub members: Vec<DescribedGroupMember>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeGroupsResponse {
    pub throttle_time_ms: i32,
    pub groups: Vec<DescribedGroup>,
}

impl DescribeGroupsResponse {
    pub fn decode(src: &mut Bytes, version: i16) -> Result<Self> {
        let throttle_time_ms = if version >= 1 {
            if src.len() < 4 {
                return Err(OxideMqError::Protocol("Truncated throttle_time_ms".into()));
            }
            src.get_i32()
        } else {
            0
        };

        if src.len() < 4 {
            return Err(OxideMqError::Protocol("Truncated groups count".into()));
        }
        let count = src.get_i32();
        if count < 0 {
            return Err(OxideMqError::Protocol("Negative groups count".into()));
        }
        let count = count as usize;
        let mut groups = Vec::with_capacity(count);

        for _ in 0..count {
            if src.len() < 2 {
                return Err(OxideMqError::Protocol("Truncated error_code".into()));
            }
            let error_code = KafkaErrorCode::from_i16(src.get_i16());
            let group_id = KafkaDecoder::read_string(src)?.unwrap_or_default();
            let group_state = KafkaDecoder::read_string(src)?.unwrap_or_default();
            let protocol_type = KafkaDecoder::read_string(src)?.unwrap_or_default();
            let protocol_data = KafkaDecoder::read_string(src)?.unwrap_or_default();

            if src.len() < 4 {
                return Err(OxideMqError::Protocol("Truncated members count".into()));
            }
            let members_count = src.get_i32();
            if members_count < 0 {
                return Err(OxideMqError::Protocol("Negative members count".into()));
            }
            let members_count = members_count as usize;
            let mut members = Vec::with_capacity(members_count);

            for _ in 0..members_count {
                let member_id = KafkaDecoder::read_string(src)?.unwrap_or_default();
                let client_id = KafkaDecoder::read_string(src)?.unwrap_or_default();
                let client_host = KafkaDecoder::read_string(src)?.unwrap_or_default();
                let member_metadata = KafkaDecoder::read_bytes(src)?.unwrap_or_default();
                let member_assignment = KafkaDecoder::read_bytes(src)?.unwrap_or_default();
                members.push(DescribedGroupMember {
                    member_id,
                    client_id,
                    client_host,
                    member_metadata,
                    member_assignment,
                });
            }

            groups.push(DescribedGroup {
                error_code,
                group_id,
                group_state,
                protocol_type,
                protocol_data,
                members,
            });
        }

        Ok(Self {
            throttle_time_ms,
            groups,
        })
    }

    pub fn encode(&self, dst: &mut BytesMut, version: i16) {
        if version >= 1 {
            dst.put_i32(self.throttle_time_ms);
        }
        dst.put_i32(self.groups.len() as i32);
        for g in &self.groups {
            dst.put_i16(g.error_code.code());
            KafkaEncoder::write_string(dst, Some(&g.group_id));
            KafkaEncoder::write_string(dst, Some(&g.group_state));
            KafkaEncoder::write_string(dst, Some(&g.protocol_type));
            KafkaEncoder::write_string(dst, Some(&g.protocol_data));

            dst.put_i32(g.members.len() as i32);
            for m in &g.members {
                KafkaEncoder::write_string(dst, Some(&m.member_id));
                KafkaEncoder::write_string(dst, Some(&m.client_id));
                KafkaEncoder::write_string(dst, Some(&m.client_host));
                KafkaEncoder::write_bytes(dst, Some(&m.member_metadata));
                KafkaEncoder::write_bytes(dst, Some(&m.member_assignment));
            }
        }
    }
}

// ==========================================
// DeleteGroups (Key 42)
// ==========================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteGroupsRequest {
    pub groups_names: Vec<String>,
}

impl DeleteGroupsRequest {
    pub fn decode(src: &mut Bytes, _version: i16) -> Result<Self> {
        if src.len() < 4 {
            return Err(OxideMqError::Protocol(
                "Truncated DeleteGroupsRequest groups count".into(),
            ));
        }
        let count = src.get_i32();
        if count < 0 {
            return Err(OxideMqError::Protocol("Negative groups count".into()));
        }
        let count = count as usize;
        let mut groups_names = Vec::with_capacity(count);
        for _ in 0..count {
            if let Some(g) = KafkaDecoder::read_string(src)? {
                groups_names.push(g);
            }
        }
        Ok(Self { groups_names })
    }

    pub fn encode(&self, dst: &mut BytesMut, _version: i16) {
        dst.put_i32(self.groups_names.len() as i32);
        for g in &self.groups_names {
            KafkaEncoder::write_string(dst, Some(g));
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeletableGroupResult {
    pub group_id: String,
    pub error_code: KafkaErrorCode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteGroupsResponse {
    pub throttle_time_ms: i32,
    pub results: Vec<DeletableGroupResult>,
}

impl DeleteGroupsResponse {
    pub fn decode(src: &mut Bytes, _version: i16) -> Result<Self> {
        if src.len() < 4 {
            return Err(OxideMqError::Protocol("Truncated throttle_time_ms".into()));
        }
        let throttle_time_ms = src.get_i32();

        if src.len() < 4 {
            return Err(OxideMqError::Protocol("Truncated results count".into()));
        }
        let count = src.get_i32();
        if count < 0 {
            return Err(OxideMqError::Protocol("Negative results count".into()));
        }
        let count = count as usize;
        let mut results = Vec::with_capacity(count);

        for _ in 0..count {
            let group_id = KafkaDecoder::read_string(src)?.unwrap_or_default();
            if src.len() < 2 {
                return Err(OxideMqError::Protocol("Truncated error_code".into()));
            }
            let error_code = KafkaErrorCode::from_i16(src.get_i16());
            results.push(DeletableGroupResult {
                group_id,
                error_code,
            });
        }

        Ok(Self {
            throttle_time_ms,
            results,
        })
    }

    pub fn encode(&self, dst: &mut BytesMut, _version: i16) {
        dst.put_i32(self.throttle_time_ms);
        dst.put_i32(self.results.len() as i32);
        for r in &self.results {
            KafkaEncoder::write_string(dst, Some(&r.group_id));
            dst.put_i16(r.error_code.code());
        }
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
            isolation_level: 0,
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

    #[test]
    fn test_record_batch_compression_codecs() {
        let mut rec1 = Record::new(
            100,
            1000,
            Some(Bytes::from_static(b"order-key-1")),
            Some(Bytes::from_static(
                b"order-payload-with-repetitive-data-12345-12345-12345",
            )),
        );
        rec1.headers
            .push(RecordHeader::new("source", Bytes::from_static(b"api")));

        let mut rec2 = Record::new(
            101,
            1001,
            Some(Bytes::from_static(b"order-key-2")),
            Some(Bytes::from_static(
                b"order-payload-with-repetitive-data-67890-67890-67890",
            )),
        );
        rec2.headers
            .push(RecordHeader::new("trace-id", Bytes::from_static(b"tx-999")));

        let original_records = vec![rec1, rec2];

        let codecs = [
            CompressionCodec::None,
            CompressionCodec::Gzip,
            CompressionCodec::Snappy,
            CompressionCodec::Lz4,
            CompressionCodec::Zstd,
        ];

        for codec in codecs {
            let batch = encode_record_batch_v2(100, &original_records, codec).unwrap();
            assert!(batch.len() >= 61);

            // Parse records directly
            let parsed = parse_record_batch_records(&batch).unwrap();
            assert_eq!(parsed.len(), 2);
            assert_eq!(parsed[0].offset, 100);
            assert_eq!(parsed[0].key.as_deref(), Some(&b"order-key-1"[..]));
            assert_eq!(
                parsed[0].value.as_deref(),
                Some(&b"order-payload-with-repetitive-data-12345-12345-12345"[..])
            );
            assert_eq!(parsed[0].headers.len(), 1);
            assert_eq!(parsed[0].headers[0].key, "source");

            assert_eq!(parsed[1].offset, 101);
            assert_eq!(parsed[1].key.as_deref(), Some(&b"order-key-2"[..]));
            assert_eq!(
                parsed[1].value.as_deref(),
                Some(&b"order-payload-with-repetitive-data-67890-67890-67890"[..])
            );
            assert_eq!(parsed[1].headers.len(), 1);
            assert_eq!(parsed[1].headers[0].key, "trace-id");

            // Decompress explicitly
            let decompressed_batch = decompress_record_batch(&batch).unwrap();
            assert_eq!(decompressed_batch[16], 2); // magic v2
            let attr = i16::from_be_bytes(decompressed_batch[21..23].try_into().unwrap());
            assert_eq!(
                CompressionCodec::from_attributes(attr),
                CompressionCodec::None
            );

            let decompressed_parsed = parse_record_batch_records(&decompressed_batch).unwrap();
            assert_eq!(decompressed_parsed.len(), 2);
        }

        // Test transcoding across codecs: Uncompressed -> Gzip -> Snappy -> LZ4 -> Zstd -> Uncompressed
        let base_batch =
            encode_record_batch_v2(0, &original_records, CompressionCodec::None).unwrap();

        let gz_batch = compress_record_batch(&base_batch, CompressionCodec::Gzip).unwrap();
        assert_eq!(
            CompressionCodec::from_attributes(i16::from_be_bytes(
                gz_batch[21..23].try_into().unwrap()
            )),
            CompressionCodec::Gzip
        );

        let snappy_batch = compress_record_batch(&gz_batch, CompressionCodec::Snappy).unwrap();
        assert_eq!(
            CompressionCodec::from_attributes(i16::from_be_bytes(
                snappy_batch[21..23].try_into().unwrap()
            )),
            CompressionCodec::Snappy
        );

        let lz4_batch = compress_record_batch(&snappy_batch, CompressionCodec::Lz4).unwrap();
        assert_eq!(
            CompressionCodec::from_attributes(i16::from_be_bytes(
                lz4_batch[21..23].try_into().unwrap()
            )),
            CompressionCodec::Lz4
        );

        let zstd_batch = compress_record_batch(&lz4_batch, CompressionCodec::Zstd).unwrap();
        assert_eq!(
            CompressionCodec::from_attributes(i16::from_be_bytes(
                zstd_batch[21..23].try_into().unwrap()
            )),
            CompressionCodec::Zstd
        );

        let final_uncompressed = decompress_record_batch(&zstd_batch).unwrap();
        assert_eq!(
            CompressionCodec::from_attributes(i16::from_be_bytes(
                final_uncompressed[21..23].try_into().unwrap()
            )),
            CompressionCodec::None
        );

        let final_parsed = parse_record_batch_records(&final_uncompressed).unwrap();
        assert_eq!(final_parsed.len(), 2);
        assert_eq!(final_parsed[0].key.as_deref(), Some(&b"order-key-1"[..]));
    }

    #[test]
    fn test_partition_produce_and_fetch_compression() {
        let rec = Record::new(
            0,
            500,
            None,
            Some(Bytes::from_static(
                b"repeat-payload-repeat-payload-repeat-payload",
            )),
        );
        let raw = encode_record_batch_v2(0, &[rec], CompressionCodec::Snappy).unwrap();

        let mut p_data = PartitionProduceData {
            partition: 0,
            records: raw.clone(),
        };

        let decompressed = p_data.decompressed_records().unwrap();
        assert_ne!(decompressed, raw);

        p_data.compress_with(CompressionCodec::Zstd).unwrap();
        assert_eq!(
            CompressionCodec::from_attributes(i16::from_be_bytes(
                p_data.records[21..23].try_into().unwrap()
            )),
            CompressionCodec::Zstd
        );

        let mut f_resp = FetchPartitionResponse {
            partition_index: 0,
            error_code: KafkaErrorCode::None,
            high_watermark: 1,
            last_stable_offset: 1,
            records: raw,
        };

        let f_decompressed = f_resp.decompressed_records().unwrap();
        assert_eq!(f_decompressed, decompressed);

        f_resp.compress_with(CompressionCodec::Lz4).unwrap();
        assert_eq!(
            CompressionCodec::from_attributes(i16::from_be_bytes(
                f_resp.records[21..23].try_into().unwrap()
            )),
            CompressionCodec::Lz4
        );
    }

    #[test]
    fn test_corrupted_compression_handling() {
        // Small buffer returns as-is
        let small = Bytes::from_static(b"too-small");
        assert_eq!(decompress_record_batch(&small).unwrap(), small);
        assert_eq!(
            compress_record_batch(&small, CompressionCodec::Gzip).unwrap(),
            small
        );

        // RecordBatch header with invalid compression payload
        let mut corrupted = BytesMut::new();
        corrupted.put_i64(0); // base_offset
        corrupted.put_i32(49 + 10); // batch_len
        corrupted.put_i32(0); // leader_epoch
        corrupted.put_u8(2); // magic
        corrupted.put_u32(0); // crc
        corrupted.put_i16(1); // attributes: GZIP
        corrupted.put_i32(0); // last_offset_delta
        corrupted.put_i64(0); // first_timestamp
        corrupted.put_i64(0); // max_timestamp
        corrupted.put_i64(-1); // producer_id
        corrupted.put_i16(-1); // producer_epoch
        corrupted.put_i32(-1); // first_sequence
        corrupted.put_i32(1); // num_records
        corrupted.extend_from_slice(b"bad-gzip-corrupted-bytes!");

        let res = decompress_record_batch(&corrupted.freeze());
        assert!(res.is_err(), "Expected error for corrupted gzip stream");
    }

    #[test]
    fn test_transactional_messages_roundtrip() {
        // InitProducerId
        let init_req = InitProducerIdRequest {
            transactional_id: Some("tx-123".into()),
            transaction_timeout_ms: 10000,
            producer_id: 42,
            producer_epoch: 1,
        };
        for v in [0, 1, 3] {
            let mut buf = BytesMut::new();
            init_req.encode(&mut buf, v);
            let mut read_buf = buf.freeze();
            let decoded = InitProducerIdRequest::decode(&mut read_buf, v).unwrap();
            assert_eq!(decoded.transactional_id, init_req.transactional_id);
            assert_eq!(
                decoded.transaction_timeout_ms,
                init_req.transaction_timeout_ms
            );
            if v >= 3 {
                assert_eq!(decoded.producer_id, 42);
                assert_eq!(decoded.producer_epoch, 1);
            }
        }

        let init_resp = InitProducerIdResponse {
            throttle_time_ms: 5,
            error_code: KafkaErrorCode::None,
            producer_id: 1000,
            producer_epoch: 2,
        };
        let mut buf = BytesMut::new();
        init_resp.encode(&mut buf, 0);
        let mut read_buf = buf.freeze();
        let decoded = InitProducerIdResponse::decode(&mut read_buf, 0).unwrap();
        assert_eq!(decoded, init_resp);

        // AddPartitionsToTxn
        let add_p_req = AddPartitionsToTxnRequest {
            transactional_id: "tx-order".into(),
            producer_id: 1000,
            producer_epoch: 2,
            topics: vec![AddPartitionsToTxnTopic {
                name: "orders".into(),
                partitions: vec![0, 1],
            }],
        };
        let mut buf = BytesMut::new();
        add_p_req.encode(&mut buf, 0);
        let mut read_buf = buf.freeze();
        let decoded_p_req = AddPartitionsToTxnRequest::decode(&mut read_buf, 0).unwrap();
        assert_eq!(decoded_p_req, add_p_req);

        let add_p_resp = AddPartitionsToTxnResponse {
            throttle_time_ms: 0,
            errors: vec![AddPartitionsToTxnTopicResult {
                name: "orders".into(),
                results: vec![AddPartitionsToTxnPartitionResult {
                    partition_index: 0,
                    error_code: KafkaErrorCode::None,
                }],
            }],
        };
        let mut buf = BytesMut::new();
        add_p_resp.encode(&mut buf, 0);
        let mut read_buf = buf.freeze();
        let decoded_p_resp = AddPartitionsToTxnResponse::decode(&mut read_buf, 0).unwrap();
        assert_eq!(decoded_p_resp, add_p_resp);

        // AddOffsetsToTxn
        let add_off_req = AddOffsetsToTxnRequest {
            transactional_id: "tx-order".into(),
            producer_id: 1000,
            producer_epoch: 2,
            group_id: "payment-group".into(),
        };
        let mut buf = BytesMut::new();
        add_off_req.encode(&mut buf, 0);
        let mut read_buf = buf.freeze();
        let decoded_off_req = AddOffsetsToTxnRequest::decode(&mut read_buf, 0).unwrap();
        assert_eq!(decoded_off_req, add_off_req);

        let add_off_resp = AddOffsetsToTxnResponse {
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
        };
        let mut buf = BytesMut::new();
        add_off_resp.encode(&mut buf, 0);
        let mut read_buf = buf.freeze();
        let decoded_off_resp = AddOffsetsToTxnResponse::decode(&mut read_buf, 0).unwrap();
        assert_eq!(decoded_off_resp, add_off_resp);

        // EndTxn
        let end_req = EndTxnRequest {
            transactional_id: "tx-order".into(),
            producer_id: 1000,
            producer_epoch: 2,
            committed: true,
        };
        let mut buf = BytesMut::new();
        end_req.encode(&mut buf, 0);
        let mut read_buf = buf.freeze();
        let decoded_end_req = EndTxnRequest::decode(&mut read_buf, 0).unwrap();
        assert_eq!(decoded_end_req, end_req);

        let end_resp = EndTxnResponse {
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
        };
        let mut buf = BytesMut::new();
        end_resp.encode(&mut buf, 0);
        let mut read_buf = buf.freeze();
        let decoded_end_resp = EndTxnResponse::decode(&mut read_buf, 0).unwrap();
        assert_eq!(decoded_end_resp, end_resp);

        // encode_control_batch
        let ctrl_bytes = encode_control_batch(50, 1000, 2, true);
        assert!(ctrl_bytes.len() >= 61);
        let records = parse_record_batch_records(&ctrl_bytes).unwrap();
        assert_eq!(records.len(), 1);
        assert!(records[0].key.is_some());

        // SaslHandshakeRequest & Response
        let hs_req = SaslHandshakeRequest {
            mechanism: "PLAIN".into(),
        };
        let mut buf = BytesMut::new();
        hs_req.encode(&mut buf, 0);
        let mut read_buf = buf.freeze();
        let decoded_hs_req = SaslHandshakeRequest::decode(&mut read_buf, 0).unwrap();
        assert_eq!(decoded_hs_req, hs_req);

        let hs_resp = SaslHandshakeResponse {
            error_code: KafkaErrorCode::None,
            enabled_mechanisms: vec!["PLAIN".into(), "SCRAM-SHA-256".into()],
        };
        let mut buf = BytesMut::new();
        hs_resp.encode(&mut buf, 0);
        let mut read_buf = buf.freeze();
        let decoded_hs_resp = SaslHandshakeResponse::decode(&mut read_buf, 0).unwrap();
        assert_eq!(decoded_hs_resp, hs_resp);

        // SaslAuthenticateRequest & Response
        for version in [0, 1, 2] {
            let auth_req = SaslAuthenticateRequest {
                auth_bytes: Bytes::from_static(b"\0alice\0secret123"),
            };
            let mut buf = BytesMut::new();
            auth_req.encode(&mut buf, version);
            let mut read_buf = buf.freeze();
            let decoded_auth_req = SaslAuthenticateRequest::decode(&mut read_buf, version).unwrap();
            assert_eq!(decoded_auth_req, auth_req);

            let auth_resp = SaslAuthenticateResponse {
                error_code: KafkaErrorCode::None,
                error_message: Some("Authenticated successfully".into()),
                auth_bytes: Bytes::from_static(b"challenge-data"),
                session_lifetime_ms: 3_600_000,
            };
            let mut buf = BytesMut::new();
            auth_resp.encode(&mut buf, version);
            let mut read_buf = buf.freeze();
            let decoded_auth_resp =
                SaslAuthenticateResponse::decode(&mut read_buf, version).unwrap();
            assert_eq!(decoded_auth_resp.error_code, auth_resp.error_code);
            assert_eq!(decoded_auth_resp.error_message, auth_resp.error_message);
            assert_eq!(decoded_auth_resp.auth_bytes, auth_resp.auth_bytes);
            if version >= 1 {
                assert_eq!(
                    decoded_auth_resp.session_lifetime_ms,
                    auth_resp.session_lifetime_ms
                );
            }
        }

        // CreateAclsRequest & Response
        for version in [0, 1, 2] {
            let create_req = CreateAclsRequest {
                creations: vec![AclCreation {
                    resource_type: AclResourceType::Topic as i8,
                    resource_name: "orders".into(),
                    resource_pattern_type: AclResourcePatternType::Literal as i8,
                    principal: "User:alice".into(),
                    host: "*".into(),
                    operation: AclOperation::Write as i8,
                    permission_type: AclPermissionType::Allow as i8,
                }],
            };
            let mut buf = BytesMut::new();
            create_req.encode(&mut buf, version);
            let mut read_buf = buf.freeze();
            let decoded_create_req = CreateAclsRequest::decode(&mut read_buf, version).unwrap();
            assert_eq!(decoded_create_req.creations.len(), 1);
            assert_eq!(decoded_create_req.creations[0].resource_name, "orders");
            assert_eq!(decoded_create_req.creations[0].principal, "User:alice");

            let create_resp = CreateAclsResponse {
                throttle_time_ms: 25,
                results: vec![AclCreationResult {
                    error_code: KafkaErrorCode::None,
                    error_message: None,
                }],
            };
            let mut buf = BytesMut::new();
            create_resp.encode(&mut buf, version);
            let mut read_buf = buf.freeze();
            let decoded_create_resp = CreateAclsResponse::decode(&mut read_buf, version).unwrap();
            assert_eq!(decoded_create_resp.results.len(), 1);
            assert_eq!(
                decoded_create_resp.results[0].error_code,
                KafkaErrorCode::None
            );
        }

        // DescribeAclsRequest & Response
        for version in [0, 1, 2] {
            let desc_req = DescribeAclsRequest {
                resource_type_filter: AclResourceType::Topic as i8,
                resource_name_filter: Some("orders".into()),
                resource_pattern_type_filter: AclResourcePatternType::Literal as i8,
                principal_filter: Some("User:alice".into()),
                host_filter: Some("*".into()),
                operation: AclOperation::Write as i8,
                permission_type: AclPermissionType::Allow as i8,
            };
            let mut buf = BytesMut::new();
            desc_req.encode(&mut buf, version);
            let mut read_buf = buf.freeze();
            let decoded_desc_req = DescribeAclsRequest::decode(&mut read_buf, version).unwrap();
            assert_eq!(decoded_desc_req.resource_name_filter, Some("orders".into()));
            assert_eq!(decoded_desc_req.principal_filter, Some("User:alice".into()));

            let desc_resp = DescribeAclsResponse {
                throttle_time_ms: 10,
                error_code: KafkaErrorCode::None,
                error_message: None,
                resources: vec![DescribeAclsResource {
                    resource_type: AclResourceType::Topic as i8,
                    resource_name: "orders".into(),
                    resource_pattern_type: AclResourcePatternType::Literal as i8,
                    acls: vec![AclDescription {
                        principal: "User:alice".into(),
                        host: "*".into(),
                        operation: AclOperation::Write as i8,
                        permission_type: AclPermissionType::Allow as i8,
                    }],
                }],
            };
            let mut buf = BytesMut::new();
            desc_resp.encode(&mut buf, version);
            let mut read_buf = buf.freeze();
            let decoded_desc_resp = DescribeAclsResponse::decode(&mut read_buf, version).unwrap();
            assert_eq!(decoded_desc_resp.resources.len(), 1);
            assert_eq!(decoded_desc_resp.resources[0].resource_name, "orders");
            assert_eq!(decoded_desc_resp.resources[0].acls.len(), 1);
            assert_eq!(
                decoded_desc_resp.resources[0].acls[0].principal,
                "User:alice"
            );
        }

        // DeleteAclsRequest & Response
        for version in [0, 1, 2] {
            let del_req = DeleteAclsRequest {
                filters: vec![DeleteAclsFilter {
                    resource_type_filter: AclResourceType::Topic as i8,
                    resource_name_filter: Some("orders".into()),
                    resource_pattern_type_filter: AclResourcePatternType::Literal as i8,
                    principal_filter: Some("User:alice".into()),
                    host_filter: Some("*".into()),
                    operation: AclOperation::Write as i8,
                    permission_type: AclPermissionType::Allow as i8,
                }],
            };
            let mut buf = BytesMut::new();
            del_req.encode(&mut buf, version);
            let mut read_buf = buf.freeze();
            let decoded_del_req = DeleteAclsRequest::decode(&mut read_buf, version).unwrap();
            assert_eq!(decoded_del_req.filters.len(), 1);
            assert_eq!(
                decoded_del_req.filters[0].resource_name_filter,
                Some("orders".into())
            );

            let del_resp = DeleteAclsResponse {
                throttle_time_ms: 15,
                filter_results: vec![DeleteAclsFilterResult {
                    error_code: KafkaErrorCode::None,
                    error_message: None,
                    matching_acls: vec![DeleteAclsMatchingAcl {
                        error_code: KafkaErrorCode::None,
                        error_message: None,
                        resource_type: AclResourceType::Topic as i8,
                        resource_name: "orders".into(),
                        resource_pattern_type: AclResourcePatternType::Literal as i8,
                        principal: "User:alice".into(),
                        host: "*".into(),
                        operation: AclOperation::Write as i8,
                        permission_type: AclPermissionType::Allow as i8,
                    }],
                }],
            };
            let mut buf = BytesMut::new();
            del_resp.encode(&mut buf, version);
            let mut read_buf = buf.freeze();
            let decoded_del_resp = DeleteAclsResponse::decode(&mut read_buf, version).unwrap();
            assert_eq!(decoded_del_resp.filter_results.len(), 1);
            assert_eq!(decoded_del_resp.filter_results[0].matching_acls.len(), 1);
            assert_eq!(
                decoded_del_resp.filter_results[0].matching_acls[0].principal,
                "User:alice"
            );
        }

        // CreateTopicsRequest & Response across versions [0, 1, 2, 3, 4]
        for version in [0, 1, 2, 3, 4] {
            let create_req = CreateTopicsRequest {
                topics: vec![CreatableTopic {
                    name: "analytics".into(),
                    num_partitions: 3,
                    replication_factor: 1,
                    assignments: vec![CreateTopicsReplicaAssignment {
                        partition_index: 0,
                        broker_ids: vec![1],
                    }],
                    configs: vec![CreateTopicsConfig {
                        name: "cleanup.policy".into(),
                        value: Some("delete".into()),
                    }],
                }],
                timeout_ms: 5000,
                validate_only: true,
            };
            let mut buf = BytesMut::new();
            create_req.encode(&mut buf, version);
            let mut read_buf = buf.freeze();
            let decoded_req = CreateTopicsRequest::decode(&mut read_buf, version).unwrap();
            assert_eq!(decoded_req.topics.len(), 1);
            assert_eq!(decoded_req.topics[0].name, "analytics");
            assert_eq!(decoded_req.topics[0].num_partitions, 3);
            assert_eq!(decoded_req.topics[0].replication_factor, 1);
            assert_eq!(decoded_req.topics[0].assignments.len(), 1);
            assert_eq!(decoded_req.topics[0].assignments[0].broker_ids, vec![1]);
            assert_eq!(decoded_req.topics[0].configs.len(), 1);
            assert_eq!(decoded_req.topics[0].configs[0].name, "cleanup.policy");
            assert_eq!(
                decoded_req.topics[0].configs[0].value.as_deref(),
                Some("delete")
            );
            assert_eq!(decoded_req.timeout_ms, 5000);
            if version >= 1 {
                assert!(decoded_req.validate_only);
            } else {
                assert!(!decoded_req.validate_only);
            }

            let create_resp = CreateTopicsResponse {
                throttle_time_ms: 12,
                topics: vec![CreatableTopicResult {
                    name: "analytics".into(),
                    error_code: KafkaErrorCode::None,
                    error_message: Some("success".into()),
                }],
            };
            let mut resp_buf = BytesMut::new();
            create_resp.encode(&mut resp_buf, version);
            let mut read_resp = resp_buf.freeze();
            let decoded_resp = CreateTopicsResponse::decode(&mut read_resp, version).unwrap();
            if version >= 2 {
                assert_eq!(decoded_resp.throttle_time_ms, 12);
            } else {
                assert_eq!(decoded_resp.throttle_time_ms, 0);
            }
            assert_eq!(decoded_resp.topics.len(), 1);
            assert_eq!(decoded_resp.topics[0].name, "analytics");
            assert_eq!(decoded_resp.topics[0].error_code, KafkaErrorCode::None);
            if version >= 1 {
                assert_eq!(
                    decoded_resp.topics[0].error_message.as_deref(),
                    Some("success")
                );
            } else {
                assert_eq!(decoded_resp.topics[0].error_message, None);
            }
        }

        // DeleteTopicsRequest & Response across versions [0, 1, 2, 3]
        for version in [0, 1, 2, 3] {
            let del_req = DeleteTopicsRequest {
                topic_names: vec!["analytics".into(), "metrics".into()],
                timeout_ms: 3000,
            };
            let mut buf = BytesMut::new();
            del_req.encode(&mut buf, version);
            let mut read_buf = buf.freeze();
            let decoded_del_req = DeleteTopicsRequest::decode(&mut read_buf, version).unwrap();
            assert_eq!(
                decoded_del_req.topic_names,
                vec!["analytics".to_string(), "metrics".to_string()]
            );
            assert_eq!(decoded_del_req.timeout_ms, 3000);

            let del_resp = DeleteTopicsResponse {
                throttle_time_ms: 8,
                responses: vec![
                    DeletableTopicResult {
                        name: "analytics".into(),
                        error_code: KafkaErrorCode::None,
                    },
                    DeletableTopicResult {
                        name: "metrics".into(),
                        error_code: KafkaErrorCode::UnknownTopicOrPartition,
                    },
                ],
            };
            let mut resp_buf = BytesMut::new();
            del_resp.encode(&mut resp_buf, version);
            let mut read_resp = resp_buf.freeze();
            let decoded_del_resp = DeleteTopicsResponse::decode(&mut read_resp, version).unwrap();
            if version >= 1 {
                assert_eq!(decoded_del_resp.throttle_time_ms, 8);
            } else {
                assert_eq!(decoded_del_resp.throttle_time_ms, 0);
            }
            assert_eq!(decoded_del_resp.responses.len(), 2);
            assert_eq!(decoded_del_resp.responses[0].name, "analytics");
            assert_eq!(
                decoded_del_resp.responses[0].error_code,
                KafkaErrorCode::None
            );
            assert_eq!(decoded_del_resp.responses[1].name, "metrics");
            assert_eq!(
                decoded_del_resp.responses[1].error_code,
                KafkaErrorCode::UnknownTopicOrPartition
            );
        }
    }

    #[test]
    fn test_describe_and_alter_configs_codec() {
        // DescribeConfigs
        let desc_req = DescribeConfigsRequest {
            resources: vec![
                DescribeConfigsResource {
                    resource_type: 2, // Topic
                    resource_name: "test-topic".into(),
                    configuration_keys: Some(vec!["cleanup.policy".into(), "retention.ms".into()]),
                },
                DescribeConfigsResource {
                    resource_type: 4, // Broker
                    resource_name: "1".into(),
                    configuration_keys: None,
                },
            ],
            include_synonyms: true,
        };

        for v in [0, 1, 2] {
            let mut buf = BytesMut::new();
            desc_req.encode(&mut buf, v);
            let mut read_buf = buf.freeze();
            let decoded = DescribeConfigsRequest::decode(&mut read_buf, v).unwrap();
            assert_eq!(decoded.resources.len(), 2);
            assert_eq!(decoded.resources[0].resource_type, 2);
            assert_eq!(decoded.resources[0].resource_name, "test-topic");
            assert_eq!(
                decoded.resources[0].configuration_keys,
                Some(vec!["cleanup.policy".into(), "retention.ms".into()])
            );
            assert_eq!(decoded.resources[1].resource_type, 4);
            assert_eq!(decoded.resources[1].configuration_keys, None);
            if v >= 1 {
                assert!(decoded.include_synonyms);
            } else {
                assert!(!decoded.include_synonyms);
            }
        }

        let desc_resp = DescribeConfigsResponse {
            throttle_time_ms: 25,
            results: vec![DescribeConfigsResult {
                error_code: KafkaErrorCode::None,
                error_message: None,
                resource_type: 2,
                resource_name: "test-topic".into(),
                configs: vec![DescribeConfigsResourceResult {
                    name: "cleanup.policy".into(),
                    value: Some("compact".into()),
                    read_only: false,
                    is_default: false,
                    config_source: 1, // DynamicTopicConfig
                    is_sensitive: false,
                    synonyms: vec![DescribeConfigsSynonym {
                        name: "cleanup.policy".into(),
                        value: Some("compact".into()),
                        source: 1,
                    }],
                }],
            }],
        };

        for v in [0, 1, 2] {
            let mut buf = BytesMut::new();
            desc_resp.encode(&mut buf, v);
            let mut read_buf = buf.freeze();
            let decoded = DescribeConfigsResponse::decode(&mut read_buf, v).unwrap();
            assert_eq!(decoded.throttle_time_ms, 25);
            assert_eq!(decoded.results.len(), 1);
            assert_eq!(decoded.results[0].resource_name, "test-topic");
            let c = &decoded.results[0].configs[0];
            assert_eq!(c.name, "cleanup.policy");
            assert_eq!(c.value.as_deref(), Some("compact"));
            assert!(!c.read_only);
            if v == 0 {
                assert!(!c.is_default);
                assert_eq!(c.synonyms.len(), 0);
            } else {
                assert_eq!(c.config_source, 1);
                assert_eq!(c.synonyms.len(), 1);
                assert_eq!(c.synonyms[0].name, "cleanup.policy");
            }
        }

        // AlterConfigs
        let alter_req = AlterConfigsRequest {
            resources: vec![AlterConfigsResource {
                resource_type: 2,
                resource_name: "test-topic".into(),
                configs: vec![AlterableConfig {
                    name: "retention.ms".into(),
                    value: Some("86400000".into()),
                }],
            }],
            validate_only: true,
        };

        for v in [0, 1] {
            let mut buf = BytesMut::new();
            alter_req.encode(&mut buf, v);
            let mut read_buf = buf.freeze();
            let decoded = AlterConfigsRequest::decode(&mut read_buf, v).unwrap();
            assert_eq!(decoded.resources.len(), 1);
            assert_eq!(decoded.resources[0].resource_name, "test-topic");
            assert_eq!(decoded.resources[0].configs[0].name, "retention.ms");
            assert_eq!(
                decoded.resources[0].configs[0].value.as_deref(),
                Some("86400000")
            );
            assert!(decoded.validate_only);
        }

        let alter_resp = AlterConfigsResponse {
            throttle_time_ms: 10,
            responses: vec![AlterConfigsResourceResponse {
                error_code: KafkaErrorCode::None,
                error_message: None,
                resource_type: 2,
                resource_name: "test-topic".into(),
            }],
        };

        for v in [0, 1] {
            let mut buf = BytesMut::new();
            alter_resp.encode(&mut buf, v);
            let mut read_buf = buf.freeze();
            let decoded = AlterConfigsResponse::decode(&mut read_buf, v).unwrap();
            assert_eq!(decoded.throttle_time_ms, 10);
            assert_eq!(decoded.responses.len(), 1);
            assert_eq!(decoded.responses[0].resource_name, "test-topic");
            assert_eq!(decoded.responses[0].error_code, KafkaErrorCode::None);
        }
    }

    #[test]
    fn test_groups_admin_codec() {
        // ListGroups
        let list_req = ListGroupsRequest {};
        for v in [0, 1, 2] {
            let mut buf = BytesMut::new();
            list_req.encode(&mut buf, v);
            let mut read_buf = buf.freeze();
            let decoded = ListGroupsRequest::decode(&mut read_buf, v).unwrap();
            assert_eq!(decoded, ListGroupsRequest {});
        }

        let list_resp = ListGroupsResponse {
            throttle_time_ms: 5,
            error_code: KafkaErrorCode::None,
            groups: vec![ListedGroup {
                group_id: "order-consumers".into(),
                protocol_type: "consumer".into(),
            }],
        };

        for v in [0, 1, 2] {
            let mut buf = BytesMut::new();
            list_resp.encode(&mut buf, v);
            let mut read_buf = buf.freeze();
            let decoded = ListGroupsResponse::decode(&mut read_buf, v).unwrap();
            if v >= 1 {
                assert_eq!(decoded.throttle_time_ms, 5);
            } else {
                assert_eq!(decoded.throttle_time_ms, 0);
            }
            assert_eq!(decoded.error_code, KafkaErrorCode::None);
            assert_eq!(decoded.groups.len(), 1);
            assert_eq!(decoded.groups[0].group_id, "order-consumers");
            assert_eq!(decoded.groups[0].protocol_type, "consumer");
        }

        // DescribeGroups
        let desc_groups_req = DescribeGroupsRequest {
            groups: vec!["order-consumers".into(), "ghost-group".into()],
        };

        for v in [0, 1, 2] {
            let mut buf = BytesMut::new();
            desc_groups_req.encode(&mut buf, v);
            let mut read_buf = buf.freeze();
            let decoded = DescribeGroupsRequest::decode(&mut read_buf, v).unwrap();
            assert_eq!(decoded.groups, vec!["order-consumers", "ghost-group"]);
        }

        let desc_groups_resp = DescribeGroupsResponse {
            throttle_time_ms: 12,
            groups: vec![
                DescribedGroup {
                    error_code: KafkaErrorCode::None,
                    group_id: "order-consumers".into(),
                    group_state: "Stable".into(),
                    protocol_type: "consumer".into(),
                    protocol_data: "range".into(),
                    members: vec![DescribedGroupMember {
                        member_id: "consumer-1".into(),
                        client_id: "client-app".into(),
                        client_host: "127.0.0.1".into(),
                        member_metadata: Bytes::from_static(b"meta"),
                        member_assignment: Bytes::from_static(b"assign"),
                    }],
                },
                DescribedGroup {
                    error_code: KafkaErrorCode::None,
                    group_id: "ghost-group".into(),
                    group_state: "Dead".into(),
                    protocol_type: "".into(),
                    protocol_data: "".into(),
                    members: Vec::new(),
                },
            ],
        };

        for v in [0, 1, 2] {
            let mut buf = BytesMut::new();
            desc_groups_resp.encode(&mut buf, v);
            let mut read_buf = buf.freeze();
            let decoded = DescribeGroupsResponse::decode(&mut read_buf, v).unwrap();
            if v >= 1 {
                assert_eq!(decoded.throttle_time_ms, 12);
            } else {
                assert_eq!(decoded.throttle_time_ms, 0);
            }
            assert_eq!(decoded.groups.len(), 2);
            assert_eq!(decoded.groups[0].group_id, "order-consumers");
            assert_eq!(decoded.groups[0].group_state, "Stable");
            assert_eq!(decoded.groups[0].members.len(), 1);
            assert_eq!(decoded.groups[0].members[0].member_id, "consumer-1");
            assert_eq!(
                decoded.groups[0].members[0].member_metadata,
                Bytes::from_static(b"meta")
            );
            assert_eq!(
                decoded.groups[0].members[0].member_assignment,
                Bytes::from_static(b"assign")
            );
            assert_eq!(decoded.groups[1].group_id, "ghost-group");
            assert_eq!(decoded.groups[1].group_state, "Dead");
            assert_eq!(decoded.groups[1].members.len(), 0);
        }

        // DeleteGroups
        let del_req = DeleteGroupsRequest {
            groups_names: vec!["order-consumers".into(), "nonexistent".into()],
        };

        for v in [0, 1, 2] {
            let mut buf = BytesMut::new();
            del_req.encode(&mut buf, v);
            let mut read_buf = buf.freeze();
            let decoded = DeleteGroupsRequest::decode(&mut read_buf, v).unwrap();
            assert_eq!(decoded.groups_names, vec!["order-consumers", "nonexistent"]);
        }

        let del_resp = DeleteGroupsResponse {
            throttle_time_ms: 7,
            results: vec![
                DeletableGroupResult {
                    group_id: "order-consumers".into(),
                    error_code: KafkaErrorCode::None,
                },
                DeletableGroupResult {
                    group_id: "nonexistent".into(),
                    error_code: KafkaErrorCode::GroupIdNotFound,
                },
            ],
        };

        for v in [0, 1, 2] {
            let mut buf = BytesMut::new();
            del_resp.encode(&mut buf, v);
            let mut read_buf = buf.freeze();
            let decoded = DeleteGroupsResponse::decode(&mut read_buf, v).unwrap();
            assert_eq!(decoded.throttle_time_ms, 7);
            assert_eq!(decoded.results.len(), 2);
            assert_eq!(decoded.results[0].group_id, "order-consumers");
            assert_eq!(decoded.results[0].error_code, KafkaErrorCode::None);
            assert_eq!(decoded.results[1].group_id, "nonexistent");
            assert_eq!(
                decoded.results[1].error_code,
                KafkaErrorCode::GroupIdNotFound
            );
        }
    }
}
