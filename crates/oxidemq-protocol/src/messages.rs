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
    }
}
