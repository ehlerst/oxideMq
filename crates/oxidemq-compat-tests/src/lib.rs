//! # oxideMq Compatibility Tests (Tier 1)
//!
//! Pure in-memory integration & Kafka protocol compatibility tests executing in milliseconds.

#[cfg(test)]
mod tests {
    use bytes::{BufMut, Bytes, BytesMut};
    use oxidemq_broker::{BrokerEngine, ClusterState, GroupCoordinator};
    use oxidemq_core::prelude::*;
    use oxidemq_protocol::header::{RequestHeader, ResponseHeader};
    use oxidemq_protocol::messages::{
        ApiVersionsRequest, ApiVersionsResponse, FetchPartition, FetchRequest, FetchResponse,
        FetchTopic, FindCoordinatorRequest, FindCoordinatorResponse, ListOffsetsPartition,
        ListOffsetsRequest, ListOffsetsResponse, ListOffsetsTopic, MetadataRequest,
        MetadataResponse, OffsetCommitPartition, OffsetCommitRequest, OffsetCommitResponse,
        OffsetCommitTopic, OffsetFetchRequest, OffsetFetchResponse, OffsetFetchTopic,
        PartitionProduceData, ProduceRequest, ProduceResponse, TopicProduceData,
    };
    use oxidemq_protocol::{ApiKey, KafkaErrorCode};
    use oxidemq_s3stream::block_cache::BlockCache;
    use oxidemq_s3stream::client::MemoryObjectStorage;
    use oxidemq_s3stream::log_cache::LogCache;
    use oxidemq_wal::memory::MemoryWal;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    fn setup_test_engine() -> BrokerEngine {
        let wal = Arc::new(MemoryWal::new());
        let storage = Arc::new(MemoryObjectStorage::new());
        let log_cache = Arc::new(LogCache::new(10 * 1024 * 1024));
        let block_cache = Arc::new(BlockCache::new(10 * 1024 * 1024));
        let state = Arc::new(ClusterState::new(
            1,
            "127.0.0.1",
            9092,
            "compat-cluster-1",
            wal,
            storage,
            log_cache,
            block_cache,
        ));
        let coord = Arc::new(GroupCoordinator::new());
        BrokerEngine::new(state, coord)
    }

    #[test]
    fn test_core_primitives_in_memory() {
        let tp = TopicPartition::new("telemetry-events", 0);
        assert_eq!(tp.topic, "telemetry-events");
        assert_eq!(tp.partition, 0);

        let record = Record::new(
            0,
            123456789,
            Some(Bytes::from_static(b"key-1")),
            Some(Bytes::from_static(b"value-1")),
        );
        let batch = RecordBatch::new(0, vec![record], Bytes::from_static(b"raw-batch-payload"));
        assert_eq!(batch.count(), 1);
        assert_eq!(batch.last_offset(), 0);
    }

    #[test]
    fn test_produce_10000_records_and_fetch() {
        let engine = setup_test_engine();

        // Produce 10,000 records across 2 partitions
        for i in 0..5000 {
            let req_header = RequestHeader::new(ApiKey::Produce, 0, i, Some("batch-producer"));
            let produce_req = ProduceRequest {
                acks: 1,
                timeout_ms: 5000,
                topic_data: vec![TopicProduceData {
                    topic: "high-throughput-topic".to_string(),
                    partitions: vec![
                        PartitionProduceData {
                            partition: 0,
                            records: Bytes::from(format!("record-p0-{}", i)),
                        },
                        PartitionProduceData {
                            partition: 1,
                            records: Bytes::from(format!("record-p1-{}", i)),
                        },
                    ],
                }],
            };

            let mut body = BytesMut::new();
            produce_req.encode(&mut body, 0);
            let mut body_bytes = body.freeze();

            let resp_buf = engine.handle_request(&req_header, &mut body_bytes).unwrap();
            let mut resp_bytes = resp_buf.freeze();
            let _ = ResponseHeader::decode(&mut resp_bytes).unwrap();
            let resp = ProduceResponse::decode(&mut resp_bytes, 0).unwrap();

            assert_eq!(resp.responses[0].partitions[0].base_offset, i as i64);
            assert_eq!(resp.responses[0].partitions[1].base_offset, i as i64);
        }

        // Fetch back records from partition 0
        let fetch_header = RequestHeader::new(ApiKey::Fetch, 0, 99999, Some("batch-consumer"));
        let fetch_req = FetchRequest {
            max_wait_ms: 1000,
            min_bytes: 1,
            max_bytes: 10 * 1024 * 1024,
            isolation_level: 0,
            topics: vec![FetchTopic {
                topic: "high-throughput-topic".to_string(),
                partitions: vec![FetchPartition {
                    partition: 0,
                    fetch_offset: 0,
                    partition_max_bytes: 10 * 1024 * 1024,
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
        let f_resp = FetchResponse::decode(&mut f_resp_bytes, 0).unwrap();

        assert_eq!(f_resp.responses[0].partitions[0].high_watermark, 5000);
        assert!(!f_resp.responses[0].partitions[0].records.is_empty());
    }

    #[test]
    fn test_consumer_group_coordination_and_offset_commit() {
        let engine = setup_test_engine();

        // 1. FindCoordinator
        let find_header = RequestHeader::new(ApiKey::FindCoordinator, 0, 1, Some("client-app"));
        let find_req = FindCoordinatorRequest {
            key: "analytics-consumer-group".to_string(),
            key_type: 0,
        };
        let mut f_body = BytesMut::new();
        find_req.encode(&mut f_body, 0);
        let mut f_bytes = f_body.freeze();

        let f_resp_buf = engine.handle_request(&find_header, &mut f_bytes).unwrap();
        let mut f_resp_bytes = f_resp_buf.freeze();
        let _ = ResponseHeader::decode(&mut f_resp_bytes).unwrap();
        let find_resp = FindCoordinatorResponse::decode(&mut f_resp_bytes, 0).unwrap();
        assert_eq!(find_resp.error_code, KafkaErrorCode::None);
        assert_eq!(find_resp.node_id, 1);

        // 2. Commit offset 2500
        let commit_header = RequestHeader::new(ApiKey::OffsetCommit, 0, 2, Some("client-app"));
        let commit_req = OffsetCommitRequest {
            group_id: "analytics-consumer-group".to_string(),
            generation_id: 1,
            member_id: "member-1".to_string(),
            topics: vec![OffsetCommitTopic {
                topic: "orders".to_string(),
                partitions: vec![OffsetCommitPartition {
                    partition: 0,
                    committed_offset: 2500,
                    metadata: None,
                }],
            }],
        };
        let mut c_body = BytesMut::new();
        commit_req.encode(&mut c_body, 0);
        let mut c_bytes = c_body.freeze();

        let c_resp_buf = engine.handle_request(&commit_header, &mut c_bytes).unwrap();
        let mut c_resp_bytes = c_resp_buf.freeze();
        let _ = ResponseHeader::decode(&mut c_resp_bytes).unwrap();
        let c_resp = OffsetCommitResponse::decode(&mut c_resp_bytes, 0).unwrap();
        assert_eq!(
            c_resp.topics[0].partitions[0].error_code,
            KafkaErrorCode::None
        );

        // 3. Fetch offset back
        let fetch_off_header = RequestHeader::new(ApiKey::OffsetFetch, 0, 3, Some("client-app"));
        let fetch_off_req = OffsetFetchRequest {
            group_id: "analytics-consumer-group".to_string(),
            topics: Some(vec![OffsetFetchTopic {
                topic: "orders".to_string(),
                partitions: vec![0],
            }]),
        };
        let mut fo_body = BytesMut::new();
        fetch_off_req.encode(&mut fo_body, 0);
        let mut fo_bytes = fo_body.freeze();

        let fo_resp_buf = engine
            .handle_request(&fetch_off_header, &mut fo_bytes)
            .unwrap();
        let mut fo_resp_bytes = fo_resp_buf.freeze();
        let _ = ResponseHeader::decode(&mut fo_resp_bytes).unwrap();
        let fo_resp = OffsetFetchResponse::decode(&mut fo_resp_bytes, 0).unwrap();
        assert_eq!(fo_resp.topics[0].partitions[0].offset, 2500);

        // 4. ListOffsets
        let list_header = RequestHeader::new(ApiKey::ListOffsets, 0, 4, Some("client-app"));
        let list_req = ListOffsetsRequest {
            replica_id: -1,
            isolation_level: 0,
            topics: vec![ListOffsetsTopic {
                topic: "orders".to_string(),
                partitions: vec![ListOffsetsPartition {
                    partition: 0,
                    current_leader_epoch: -1,
                    timestamp: -1,
                }],
            }],
        };
        let mut l_body = BytesMut::new();
        list_req.encode(&mut l_body, 0);
        let mut l_bytes = l_body.freeze();

        let l_resp_buf = engine.handle_request(&list_header, &mut l_bytes).unwrap();
        let mut l_resp_bytes = l_resp_buf.freeze();
        let _ = ResponseHeader::decode(&mut l_resp_bytes).unwrap();
        let l_resp = ListOffsetsResponse::decode(&mut l_resp_bytes, 0).unwrap();
        assert_eq!(
            l_resp.topics[0].partitions[0].error_code,
            KafkaErrorCode::None
        );
    }

    #[tokio::test]
    async fn test_e2e_framed_tcp_socket_communication() {
        let engine = setup_test_engine();

        // 1. Bind ephemeral TCP socket
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr: SocketAddr = listener.local_addr().unwrap();

        let engine_clone = engine.clone();
        tokio::spawn(async move {
            if let Ok((socket, _)) = listener.accept().await {
                let (reader, writer) = socket.into_split();
                let _ = engine_clone
                    .process_connection(tokio::io::join(reader, writer))
                    .await;
            }
        });

        // 2. Connect client stream
        let mut client = TcpStream::connect(addr).await.unwrap();

        // 3. Send ApiVersions request over TCP
        let header = RequestHeader::new(ApiKey::ApiVersions, 0, 777, Some("tcp-tester"));
        let mut req_body = BytesMut::new();
        header.encode(&mut req_body);
        let api_req = ApiVersionsRequest::default();
        api_req.encode(&mut req_body, 0);

        let mut frame = BytesMut::new();
        frame.put_i32(req_body.len() as i32);
        frame.put_slice(&req_body);

        client.write_all(&frame).await.unwrap();
        client.flush().await.unwrap();

        // 4. Read response frame
        let resp_len = client.read_i32().await.unwrap() as usize;
        let mut resp_buf = vec![0u8; resp_len];
        client.read_exact(&mut resp_buf).await.unwrap();

        let mut resp_bytes = Bytes::from(resp_buf);
        let resp_header = ResponseHeader::decode(&mut resp_bytes).unwrap();
        assert_eq!(resp_header.correlation_id, 777);

        let api_versions = ApiVersionsResponse::decode(&mut resp_bytes, 0).unwrap();
        assert_eq!(api_versions.error_code, KafkaErrorCode::None);
        assert!(!api_versions.api_keys.is_empty());

        // 5. Send Metadata request over TCP
        let meta_header = RequestHeader::new(ApiKey::Metadata, 0, 778, Some("tcp-tester"));
        let mut meta_req_buf = BytesMut::new();
        meta_header.encode(&mut meta_req_buf);
        let meta_req = MetadataRequest {
            topics: Some(vec!["tcp-telemetry".to_string()]),
            allow_auto_topic_creation: true,
        };
        meta_req.encode(&mut meta_req_buf, 0);

        let mut meta_frame = BytesMut::new();
        meta_frame.put_i32(meta_req_buf.len() as i32);
        meta_frame.put_slice(&meta_req_buf);

        client.write_all(&meta_frame).await.unwrap();
        client.flush().await.unwrap();

        let m_resp_len = client.read_i32().await.unwrap() as usize;
        let mut m_resp_buf = vec![0u8; m_resp_len];
        client.read_exact(&mut m_resp_buf).await.unwrap();

        let mut m_resp_bytes = Bytes::from(m_resp_buf);
        let m_resp_header = ResponseHeader::decode(&mut m_resp_bytes).unwrap();
        assert_eq!(m_resp_header.correlation_id, 778);

        let meta_resp = MetadataResponse::decode(&mut m_resp_bytes, 0).unwrap();
        assert_eq!(meta_resp.brokers.len(), 1);
        assert_eq!(meta_resp.topics.len(), 1);
        assert_eq!(meta_resp.topics[0].name, "tcp-telemetry");
    }
}
