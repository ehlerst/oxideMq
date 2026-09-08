use crate::chaos::{ChaosEngine, FaultTarget};
use crate::coordinator::GroupCoordinator;
use crate::router::ClusterState;
use bytes::{BufMut, Bytes, BytesMut};
use oxidemq_core::error::{OxideMqError, Result};
use oxidemq_core::types::TopicPartition;
use oxidemq_protocol::header::{RequestHeader, ResponseHeader};
use oxidemq_protocol::messages::{
    ApiVersionsRequest, ApiVersionsResponse, FetchPartitionResponse, FetchRequest, FetchResponse,
    FetchTopicResponse, FindCoordinatorRequest, FindCoordinatorResponse, HeartbeatRequest,
    HeartbeatResponse, LeaveGroupRequest, LeaveGroupResponse, ListOffsetsPartitionResponse,
    ListOffsetsRequest, ListOffsetsResponse, ListOffsetsTopicResponse, MetadataRequest,
    OffsetCommitPartitionResponse, OffsetCommitRequest, OffsetCommitResponse,
    OffsetCommitTopicResponse, OffsetFetchPartitionResponse, OffsetFetchRequest,
    OffsetFetchResponse, OffsetFetchTopicResponse, PartitionProduceResponse, ProduceRequest,
    ProduceResponse, TopicProduceResponse,
};
use oxidemq_protocol::{ApiKey, KafkaErrorCode};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tracing::{debug, error, trace};

/// Central broker request execution engine.
#[derive(Clone)]
pub struct BrokerEngine {
    cluster_state: Arc<ClusterState>,
    coordinator: Arc<GroupCoordinator>,
    chaos: Arc<ChaosEngine>,
}

impl BrokerEngine {
    pub fn new(cluster_state: Arc<ClusterState>, coordinator: Arc<GroupCoordinator>) -> Self {
        Self {
            cluster_state,
            coordinator,
            chaos: Arc::new(ChaosEngine::new()),
        }
    }

    pub fn with_chaos(mut self, chaos: Arc<ChaosEngine>) -> Self {
        self.chaos = chaos;
        self
    }

    pub fn cluster_state(&self) -> &Arc<ClusterState> {
        &self.cluster_state
    }

    pub fn coordinator(&self) -> &Arc<GroupCoordinator> {
        &self.coordinator
    }

    pub fn chaos(&self) -> &Arc<ChaosEngine> {
        &self.chaos
    }

    /// Dispatches and processes an individual Kafka request given its header and body payload.
    /// Returns the serialized ResponseHeader + ResponsePayload (unframed).
    pub fn handle_request(&self, header: &RequestHeader, body: &mut Bytes) -> Result<BytesMut> {
        let mut out = BytesMut::with_capacity(1024);
        let resp_header = ResponseHeader::new(header.correlation_id);
        resp_header.encode(&mut out);

        match header.api_key {
            ApiKey::ApiVersions => {
                let _req = ApiVersionsRequest::decode(body, header.api_version)?;
                let resp = ApiVersionsResponse::default_supported();
                resp.encode(&mut out, header.api_version);
            }
            ApiKey::Metadata => {
                let req = MetadataRequest::decode(body, header.api_version)?;
                let resp = self.cluster_state.build_metadata(req.topics.as_deref());
                resp.encode(&mut out, header.api_version);
            }
            ApiKey::Produce => {
                let req = ProduceRequest::decode(body, header.api_version)?;
                let mut topic_responses = Vec::with_capacity(req.topic_data.len());
                let fault = self.chaos.check_fault(FaultTarget::Produce);

                for t in req.topic_data {
                    let mut part_responses = Vec::with_capacity(t.partitions.len());
                    for p in t.partitions {
                        let tp = TopicPartition::new(&t.topic, p.partition);
                        let partition = self.cluster_state.get_or_create_partition(&tp);

                        let (err, base_off, append_time, start_off) = if let Err(e) = &fault {
                            error!("Chaos fault injected on Produce for {}: {}", tp, e);
                            (KafkaErrorCode::UnknownServer, -1, -1, 0)
                        } else {
                            match partition.append_records(p.records) {
                                Ok((b_off, a_time)) => (
                                    KafkaErrorCode::None,
                                    b_off,
                                    a_time,
                                    partition.log_start_offset(),
                                ),
                                Err(e) => {
                                    error!("Failed to append records to {}: {}", tp, e);
                                    (KafkaErrorCode::UnknownServer, -1, -1, 0)
                                }
                            }
                        };

                        part_responses.push(PartitionProduceResponse {
                            partition: p.partition,
                            error_code: err,
                            base_offset: base_off,
                            log_append_time_ms: append_time,
                            log_start_offset: start_off,
                        });
                    }

                    topic_responses.push(TopicProduceResponse {
                        topic: t.topic,
                        partitions: part_responses,
                    });
                }

                let resp = ProduceResponse {
                    responses: topic_responses,
                    throttle_time_ms: 0,
                };
                resp.encode(&mut out, header.api_version);
            }
            ApiKey::Fetch => {
                let req = FetchRequest::decode(body, header.api_version)?;
                let mut topic_responses = Vec::with_capacity(req.topics.len());
                let fault = self.chaos.check_fault(FaultTarget::Fetch);

                for t in req.topics {
                    let mut part_responses = Vec::with_capacity(t.partitions.len());
                    for p in t.partitions {
                        let tp = TopicPartition::new(&t.topic, p.partition);
                        let partition = self.cluster_state.get_or_create_partition(&tp);

                        let (err, records_bytes) = if let Err(e) = &fault {
                            error!("Chaos fault injected on Fetch for {}: {}", tp, e);
                            (KafkaErrorCode::UnknownServer, Bytes::new())
                        } else {
                            match partition
                                .read_records(p.fetch_offset, p.partition_max_bytes as usize)
                            {
                                Ok(batches) => {
                                    let mut b = BytesMut::new();
                                    for (_off, batch_bytes) in batches {
                                        b.extend_from_slice(&batch_bytes);
                                    }
                                    (KafkaErrorCode::None, b.freeze())
                                }
                                Err(e) => {
                                    error!("Failed to fetch records from {}: {}", tp, e);
                                    (KafkaErrorCode::UnknownServer, Bytes::new())
                                }
                            }
                        };

                        let hw = partition.high_watermark();
                        part_responses.push(FetchPartitionResponse {
                            partition_index: p.partition,
                            error_code: err,
                            high_watermark: hw,
                            last_stable_offset: hw,
                            records: records_bytes,
                        });
                    }

                    topic_responses.push(FetchTopicResponse {
                        topic: t.topic,
                        partitions: part_responses,
                    });
                }

                let resp = FetchResponse {
                    throttle_time_ms: 0,
                    error_code: KafkaErrorCode::None,
                    responses: topic_responses,
                };
                resp.encode(&mut out, header.api_version);
            }
            ApiKey::FindCoordinator => {
                let _req = FindCoordinatorRequest::decode(body, header.api_version)?;
                let resp = FindCoordinatorResponse {
                    throttle_time_ms: 0,
                    error_code: KafkaErrorCode::None,
                    error_message: None,
                    node_id: self.cluster_state.node_id(),
                    host: self.cluster_state.host().to_string(),
                    port: self.cluster_state.port(),
                };
                resp.encode(&mut out, header.api_version);
            }
            ApiKey::ListOffsets => {
                let req = ListOffsetsRequest::decode(body, header.api_version)?;
                let mut topic_responses = Vec::with_capacity(req.topics.len());

                for t in req.topics {
                    let mut part_responses = Vec::with_capacity(t.partitions.len());
                    for p in t.partitions {
                        let tp = TopicPartition::new(&t.topic, p.partition);
                        let partition = self.cluster_state.get_or_create_partition(&tp);
                        let offset = if p.timestamp == -2 {
                            partition.log_start_offset()
                        } else {
                            partition.high_watermark()
                        };

                        part_responses.push(ListOffsetsPartitionResponse {
                            partition: p.partition,
                            error_code: KafkaErrorCode::None,
                            timestamp: -1,
                            offset,
                        });
                    }
                    topic_responses.push(ListOffsetsTopicResponse {
                        topic: t.topic,
                        partitions: part_responses,
                    });
                }

                let resp = ListOffsetsResponse {
                    throttle_time_ms: 0,
                    topics: topic_responses,
                };
                resp.encode(&mut out, header.api_version);
            }
            ApiKey::OffsetCommit => {
                let req = OffsetCommitRequest::decode(body, header.api_version)?;
                let mut topic_responses = Vec::with_capacity(req.topics.len());

                for t in req.topics {
                    let mut part_responses = Vec::with_capacity(t.partitions.len());
                    for p in t.partitions {
                        let tp = TopicPartition::new(&t.topic, p.partition);
                        self.coordinator
                            .commit_offset(&req.group_id, tp, p.committed_offset);

                        part_responses.push(OffsetCommitPartitionResponse {
                            partition: p.partition,
                            error_code: KafkaErrorCode::None,
                        });
                    }
                    topic_responses.push(OffsetCommitTopicResponse {
                        topic: t.topic,
                        partitions: part_responses,
                    });
                }

                let resp = OffsetCommitResponse {
                    throttle_time_ms: 0,
                    topics: topic_responses,
                };
                resp.encode(&mut out, header.api_version);
            }
            ApiKey::OffsetFetch => {
                let req = OffsetFetchRequest::decode(body, header.api_version)?;
                let mut topic_responses = Vec::new();

                if let Some(topics) = req.topics {
                    for t in topics {
                        let mut part_responses = Vec::with_capacity(t.partitions.len());
                        for p in t.partitions {
                            let tp = TopicPartition::new(&t.topic, p);
                            let off = self
                                .coordinator
                                .fetch_offset(&req.group_id, &tp)
                                .unwrap_or(-1);

                            part_responses.push(OffsetFetchPartitionResponse {
                                partition: p,
                                offset: off,
                                metadata: None,
                                error_code: KafkaErrorCode::None,
                            });
                        }
                        topic_responses.push(OffsetFetchTopicResponse {
                            topic: t.topic,
                            partitions: part_responses,
                        });
                    }
                }

                let resp = OffsetFetchResponse {
                    throttle_time_ms: 0,
                    topics: topic_responses,
                    error_code: KafkaErrorCode::None,
                };
                resp.encode(&mut out, header.api_version);
            }
            ApiKey::Heartbeat => {
                let req = HeartbeatRequest::decode(body, header.api_version)?;
                let err = self.coordinator.handle_heartbeat(
                    &req.group_id,
                    req.generation_id,
                    &req.member_id,
                );
                let resp = HeartbeatResponse {
                    throttle_time_ms: 0,
                    error_code: err,
                };
                resp.encode(&mut out, header.api_version);
            }
            ApiKey::LeaveGroup => {
                let req = LeaveGroupRequest::decode(body, header.api_version)?;
                let err = self
                    .coordinator
                    .handle_leave_group(&req.group_id, &req.member_id);
                let resp = LeaveGroupResponse {
                    throttle_time_ms: 0,
                    error_code: err,
                };
                resp.encode(&mut out, header.api_version);
            }
            _ => {
                return Err(OxideMqError::Protocol(format!(
                    "Unsupported Kafka API Key {:?}",
                    header.api_key
                )));
            }
        }

        Ok(out)
    }

    /// Handles a raw 4-byte length-prefixed frame.
    /// Returns the length-prefixed response frame.
    pub fn handle_frame(&self, mut frame: Bytes) -> Result<BytesMut> {
        let header = RequestHeader::decode(&mut frame)?;
        trace!(
            "Received Kafka request api_key={:?} version={} corr_id={}",
            header.api_key,
            header.api_version,
            header.correlation_id
        );

        let response_payload = self.handle_request(&header, &mut frame)?;
        let mut framed = BytesMut::with_capacity(response_payload.len() + 4);
        framed.put_i32(response_payload.len() as i32);
        framed.put_slice(&response_payload);
        Ok(framed)
    }

    /// Processes an incoming TCP client connection stream until EOF or error.
    pub async fn process_connection<S>(&self, mut stream: S) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        loop {
            // Read 4-byte frame length prefix
            let frame_len = match stream.read_i32().await {
                Ok(len) => len as usize,
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    debug!("Client disconnected");
                    break;
                }
                Err(e) => return Err(OxideMqError::Io(e)),
            };

            if frame_len == 0 || frame_len > 64 * 1024 * 1024 {
                return Err(OxideMqError::Protocol(format!(
                    "Invalid frame length: {}",
                    frame_len
                )));
            }

            let mut frame_buf = vec![0u8; frame_len];
            stream.read_exact(&mut frame_buf).await?;

            let response_frame = self.handle_frame(Bytes::from(frame_buf))?;
            stream.write_all(&response_frame).await?;
            stream.flush().await?;
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

    fn create_test_engine() -> BrokerEngine {
        let wal = Arc::new(MemoryWal::new());
        let storage = Arc::new(MemoryObjectStorage::new());
        let log_cache = Arc::new(LogCache::new(1024 * 1024));
        let block_cache = Arc::new(BlockCache::new(1024 * 1024));
        let state = Arc::new(ClusterState::new(
            1,
            "127.0.0.1",
            9092,
            "test-cluster",
            wal,
            storage,
            log_cache,
            block_cache,
        ));
        let coord = Arc::new(GroupCoordinator::new());
        BrokerEngine::new(state, coord)
    }

    #[test]
    fn test_engine_api_versions() {
        let engine = create_test_engine();

        let req_header = RequestHeader::new(ApiKey::ApiVersions, 0, 101, Some("test-client"));
        let mut body = Bytes::new();

        let resp_buf = engine.handle_request(&req_header, &mut body).unwrap();
        let mut resp_bytes = resp_buf.freeze();

        let resp_header = ResponseHeader::decode(&mut resp_bytes).unwrap();
        assert_eq!(resp_header.correlation_id, 101);

        let api_versions = ApiVersionsResponse::decode(&mut resp_bytes, 0).unwrap();
        assert_eq!(api_versions.error_code, KafkaErrorCode::None);
        assert!(!api_versions.api_keys.is_empty());
    }

    #[test]
    fn test_engine_produce_fetch_pipeline() {
        let engine = create_test_engine();

        // 1. Produce request
        let req_header = RequestHeader::new(ApiKey::Produce, 0, 201, Some("test-producer"));
        let produce_req = ProduceRequest {
            acks: 1,
            timeout_ms: 1000,
            topic_data: vec![oxidemq_protocol::TopicProduceData {
                topic: "orders".to_string(),
                partitions: vec![oxidemq_protocol::PartitionProduceData {
                    partition: 0,
                    records: Bytes::from_static(b"payload-12345"),
                }],
            }],
        };

        let mut body = BytesMut::new();
        produce_req.encode(&mut body, 0);
        let mut body_bytes = body.freeze();

        let resp_buf = engine.handle_request(&req_header, &mut body_bytes).unwrap();
        let mut resp_bytes = resp_buf.freeze();

        let _ = ResponseHeader::decode(&mut resp_bytes).unwrap();
        let produce_resp = ProduceResponse::decode(&mut resp_bytes, 0).unwrap();
        assert_eq!(produce_resp.responses.len(), 1);
        assert_eq!(produce_resp.responses[0].partitions[0].base_offset, 0);

        // 2. Fetch request
        let fetch_header = RequestHeader::new(ApiKey::Fetch, 0, 202, Some("test-consumer"));
        let fetch_req = FetchRequest {
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1024 * 1024,
            topics: vec![oxidemq_protocol::FetchTopic {
                topic: "orders".to_string(),
                partitions: vec![oxidemq_protocol::FetchPartition {
                    partition: 0,
                    fetch_offset: 0,
                    partition_max_bytes: 65536,
                }],
            }],
        };

        let mut fetch_body = BytesMut::new();
        fetch_req.encode(&mut fetch_body, 0);
        let mut fetch_bytes = fetch_body.freeze();

        let f_resp_buf = engine
            .handle_request(&fetch_header, &mut fetch_bytes)
            .unwrap();
        let mut f_resp_bytes = f_resp_buf.freeze();

        let _ = ResponseHeader::decode(&mut f_resp_bytes).unwrap();
        let fetch_resp = FetchResponse::decode(&mut f_resp_bytes, 0).unwrap();
        assert_eq!(fetch_resp.responses.len(), 1);
        assert_eq!(fetch_resp.responses[0].partitions.len(), 1);
        assert_eq!(
            fetch_resp.responses[0].partitions[0].records.as_ref(),
            b"payload-12345"
        );
    }
}
