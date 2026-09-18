//! # oxideMq Compatibility Tests (Tier 1)
//!
//! Pure in-memory integration & Kafka protocol compatibility tests executing in milliseconds.

#[cfg(test)]
mod tests {
    use bytes::{BufMut, Bytes, BytesMut};
    use oxidemq_broker::acl::AclAuthorizer;
    use oxidemq_broker::sasl::{SaslAuthenticator, SaslMechanism};
    use oxidemq_broker::{BrokerEngine, ClusterState, GroupCoordinator};
    use oxidemq_core::prelude::*;
    use oxidemq_protocol::header::{RequestHeader, ResponseHeader};
    use oxidemq_protocol::messages::{
        AclCreation, ApiVersionsRequest, ApiVersionsResponse, CreatableTopic, CreateAclsRequest,
        CreateAclsResponse, CreateTopicsConfig, CreateTopicsRequest, CreateTopicsResponse,
        DeleteAclsFilter, DeleteAclsRequest, DeleteAclsResponse, DeleteTopicsRequest,
        DeleteTopicsResponse, DescribeAclsRequest, DescribeAclsResponse, FetchPartition,
        FetchRequest, FetchResponse, FetchTopic, FindCoordinatorRequest, FindCoordinatorResponse,
        ListOffsetsPartition, ListOffsetsRequest, ListOffsetsResponse, ListOffsetsTopic,
        MetadataRequest, MetadataResponse, OffsetCommitPartition, OffsetCommitRequest,
        OffsetCommitResponse, OffsetCommitTopic, OffsetFetchRequest, OffsetFetchResponse,
        OffsetFetchTopic, PartitionProduceData, ProduceRequest, ProduceResponse,
        SaslAuthenticateRequest, SaslAuthenticateResponse, SaslHandshakeRequest,
        SaslHandshakeResponse, TopicProduceData,
    };
    use oxidemq_protocol::{
        AclOperation, AclPermissionType, AclResourcePatternType, AclResourceType, ApiKey,
        KafkaErrorCode,
    };
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

    #[tokio::test]
    async fn test_e2e_sasl_authentication_over_tcp() {
        use base64::prelude::*;
        use oxidemq_broker::sasl::{SaslAuthenticator, SaslMechanism};
        use oxidemq_protocol::messages::{
            SaslAuthenticateRequest, SaslAuthenticateResponse, SaslHandshakeRequest,
            SaslHandshakeResponse,
        };
        use ring::digest;
        use ring::hmac;
        use ring::pbkdf2;
        use std::num::NonZeroU32;

        let wal = Arc::new(MemoryWal::new());
        let storage = Arc::new(MemoryObjectStorage::new());
        let log_cache = Arc::new(LogCache::new(10 * 1024 * 1024));
        let block_cache = Arc::new(BlockCache::new(10 * 1024 * 1024));
        let state = Arc::new(ClusterState::new(
            1,
            "127.0.0.1",
            9092,
            "sasl-test-cluster",
            wal,
            storage,
            log_cache,
            block_cache,
        ));
        let coord = Arc::new(GroupCoordinator::new());
        let auth = Arc::new(SaslAuthenticator::new(
            vec![SaslMechanism::Plain, SaslMechanism::ScramSha256],
            true, // require SASL
        ));
        auth.add_user("kafka-client", "topsecret123");
        let engine = Arc::new(BrokerEngine::new(state, coord).with_authenticator(auth));

        // Bind TCP listener
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let engine_clone = Arc::clone(&engine);
        tokio::spawn(async move {
            loop {
                if let Ok((stream, _)) = listener.accept().await {
                    let eng = Arc::clone(&engine_clone);
                    tokio::spawn(async move {
                        let _ = eng.process_connection(stream).await;
                    });
                }
            }
        });

        // 1. Client 1: Unauthenticated request should fail when require_sasl is true
        {
            let mut client1 = TcpStream::connect(addr).await.unwrap();
            let meta_header = RequestHeader::new(ApiKey::Metadata, 0, 101, Some("unauth-client"));
            let mut meta_req_buf = BytesMut::new();
            meta_header.encode(&mut meta_req_buf);
            let meta_req = MetadataRequest {
                topics: Some(vec!["secret-topic".to_string()]),
                allow_auto_topic_creation: true,
            };
            meta_req.encode(&mut meta_req_buf, 0);

            let mut meta_frame = BytesMut::new();
            meta_frame.put_i32(meta_req_buf.len() as i32);
            meta_frame.put_slice(&meta_req_buf);
            client1.write_all(&meta_frame).await.unwrap();
            client1.flush().await.unwrap();

            // The connection should be terminated or error returned
            let read_res = client1.read_i32().await;
            assert!(read_res.is_err());
        }

        // 2. Client 2: SASL PLAIN authentication over TCP
        {
            let mut client2 = TcpStream::connect(addr).await.unwrap();

            // ApiVersions allowed before auth
            let api_ver_hdr = RequestHeader::new(ApiKey::ApiVersions, 0, 201, Some("plain-client"));
            let mut api_ver_buf = BytesMut::new();
            api_ver_hdr.encode(&mut api_ver_buf);
            let api_req = ApiVersionsRequest::default();
            api_req.encode(&mut api_ver_buf, 0);
            let mut frame = BytesMut::new();
            frame.put_i32(api_ver_buf.len() as i32);
            frame.put_slice(&api_ver_buf);
            client2.write_all(&frame).await.unwrap();
            client2.flush().await.unwrap();

            let resp_len = client2.read_i32().await.unwrap() as usize;
            let mut resp_buf = vec![0u8; resp_len];
            client2.read_exact(&mut resp_buf).await.unwrap();
            let mut resp_bytes = Bytes::from(resp_buf);
            let _ = ResponseHeader::decode(&mut resp_bytes).unwrap();
            let api_resp = ApiVersionsResponse::decode(&mut resp_bytes, 0).unwrap();
            assert_eq!(api_resp.error_code, KafkaErrorCode::None);

            // SaslHandshake
            let hs_hdr = RequestHeader::new(ApiKey::SaslHandshake, 0, 202, Some("plain-client"));
            let mut hs_buf = BytesMut::new();
            hs_hdr.encode(&mut hs_buf);
            let hs_req = SaslHandshakeRequest {
                mechanism: "PLAIN".into(),
            };
            hs_req.encode(&mut hs_buf, 0);
            let mut hs_frame = BytesMut::new();
            hs_frame.put_i32(hs_buf.len() as i32);
            hs_frame.put_slice(&hs_buf);
            client2.write_all(&hs_frame).await.unwrap();
            client2.flush().await.unwrap();

            let hs_len = client2.read_i32().await.unwrap() as usize;
            let mut hs_resp_buf = vec![0u8; hs_len];
            client2.read_exact(&mut hs_resp_buf).await.unwrap();
            let mut hs_bytes = Bytes::from(hs_resp_buf);
            let _ = ResponseHeader::decode(&mut hs_bytes).unwrap();
            let hs_resp = SaslHandshakeResponse::decode(&mut hs_bytes, 0).unwrap();
            assert_eq!(hs_resp.error_code, KafkaErrorCode::None);

            // SaslAuthenticate with PLAIN
            let auth_hdr =
                RequestHeader::new(ApiKey::SaslAuthenticate, 0, 203, Some("plain-client"));
            let mut auth_buf = BytesMut::new();
            auth_hdr.encode(&mut auth_buf);
            let auth_req = SaslAuthenticateRequest {
                auth_bytes: Bytes::from_static(b"\0kafka-client\0topsecret123"),
            };
            auth_req.encode(&mut auth_buf, 0);
            let mut auth_frame = BytesMut::new();
            auth_frame.put_i32(auth_buf.len() as i32);
            auth_frame.put_slice(&auth_buf);
            client2.write_all(&auth_frame).await.unwrap();
            client2.flush().await.unwrap();

            let auth_len = client2.read_i32().await.unwrap() as usize;
            let mut auth_resp_buf = vec![0u8; auth_len];
            client2.read_exact(&mut auth_resp_buf).await.unwrap();
            let mut auth_bytes = Bytes::from(auth_resp_buf);
            let _ = ResponseHeader::decode(&mut auth_bytes).unwrap();
            let auth_resp = SaslAuthenticateResponse::decode(&mut auth_bytes, 0).unwrap();
            assert_eq!(auth_resp.error_code, KafkaErrorCode::None);

            // Now Metadata succeeds over the authenticated connection
            let meta_hdr = RequestHeader::new(ApiKey::Metadata, 0, 204, Some("plain-client"));
            let mut meta_buf = BytesMut::new();
            meta_hdr.encode(&mut meta_buf);
            let meta_req = MetadataRequest {
                topics: Some(vec!["authenticated-topic".to_string()]),
                allow_auto_topic_creation: true,
            };
            meta_req.encode(&mut meta_buf, 0);
            let mut m_frame = BytesMut::new();
            m_frame.put_i32(meta_buf.len() as i32);
            m_frame.put_slice(&meta_buf);
            client2.write_all(&m_frame).await.unwrap();
            client2.flush().await.unwrap();

            let m_len = client2.read_i32().await.unwrap() as usize;
            let mut m_buf = vec![0u8; m_len];
            client2.read_exact(&mut m_buf).await.unwrap();
            let mut m_bytes = Bytes::from(m_buf);
            let _ = ResponseHeader::decode(&mut m_bytes).unwrap();
            let meta_resp = MetadataResponse::decode(&mut m_bytes, 0).unwrap();
            assert_eq!(meta_resp.topics[0].name, "authenticated-topic");
        }

        // 3. Client 3: SASL SCRAM-SHA-256 authentication over TCP
        {
            let mut client3 = TcpStream::connect(addr).await.unwrap();

            // SaslHandshake
            let hs_hdr = RequestHeader::new(ApiKey::SaslHandshake, 0, 301, Some("scram-client"));
            let mut hs_buf = BytesMut::new();
            hs_hdr.encode(&mut hs_buf);
            let hs_req = SaslHandshakeRequest {
                mechanism: "SCRAM-SHA-256".into(),
            };
            hs_req.encode(&mut hs_buf, 0);
            let mut hs_frame = BytesMut::new();
            hs_frame.put_i32(hs_buf.len() as i32);
            hs_frame.put_slice(&hs_buf);
            client3.write_all(&hs_frame).await.unwrap();
            client3.flush().await.unwrap();

            let hs_len = client3.read_i32().await.unwrap() as usize;
            let mut hs_resp_buf = vec![0u8; hs_len];
            client3.read_exact(&mut hs_resp_buf).await.unwrap();
            let mut hs_bytes = Bytes::from(hs_resp_buf);
            let _ = ResponseHeader::decode(&mut hs_bytes).unwrap();
            let hs_resp = SaslHandshakeResponse::decode(&mut hs_bytes, 0).unwrap();
            assert_eq!(hs_resp.error_code, KafkaErrorCode::None);

            // SaslAuthenticate Round 1: client-first
            let client_nonce = "clientNonceRandom789";
            let client_first = format!("n,,n=kafka-client,r={}", client_nonce);
            let auth1_hdr =
                RequestHeader::new(ApiKey::SaslAuthenticate, 0, 302, Some("scram-client"));
            let mut auth1_buf = BytesMut::new();
            auth1_hdr.encode(&mut auth1_buf);
            let auth1_req = SaslAuthenticateRequest {
                auth_bytes: Bytes::from(client_first),
            };
            auth1_req.encode(&mut auth1_buf, 0);
            let mut auth1_frame = BytesMut::new();
            auth1_frame.put_i32(auth1_buf.len() as i32);
            auth1_frame.put_slice(&auth1_buf);
            client3.write_all(&auth1_frame).await.unwrap();
            client3.flush().await.unwrap();

            let auth1_len = client3.read_i32().await.unwrap() as usize;
            let mut auth1_resp_buf = vec![0u8; auth1_len];
            client3.read_exact(&mut auth1_resp_buf).await.unwrap();
            let mut auth1_bytes = Bytes::from(auth1_resp_buf);
            let _ = ResponseHeader::decode(&mut auth1_bytes).unwrap();
            let auth1_resp = SaslAuthenticateResponse::decode(&mut auth1_bytes, 0).unwrap();
            assert_eq!(auth1_resp.error_code, KafkaErrorCode::None);

            let server_first_str = std::str::from_utf8(&auth1_resp.auth_bytes).unwrap();
            let mut full_nonce = "";
            let mut salt_b64 = "";
            for part in server_first_str.split(',') {
                if let Some(val) = part.strip_prefix("r=") {
                    full_nonce = val;
                } else if let Some(val) = part.strip_prefix("s=") {
                    salt_b64 = val;
                }
            }
            let salt = BASE64_STANDARD.decode(salt_b64).unwrap();

            // Client computes proof
            let client_final_without_proof = format!("c=biws,r={}", full_nonce);
            let auth_message = format!(
                "n=kafka-client,r={},{},{}",
                client_nonce, server_first_str, client_final_without_proof
            );

            let mut salted_password = [0u8; 32];
            pbkdf2::derive(
                pbkdf2::PBKDF2_HMAC_SHA256,
                NonZeroU32::new(4096).unwrap(),
                &salt,
                b"topsecret123",
                &mut salted_password,
            );

            let s_key = hmac::Key::new(hmac::HMAC_SHA256, &salted_password);
            let client_key = hmac::sign(&s_key, b"Client Key");
            let stored_key = digest::digest(&digest::SHA256, client_key.as_ref());
            let stored_key_hmac = hmac::Key::new(hmac::HMAC_SHA256, stored_key.as_ref());
            let client_sig = hmac::sign(&stored_key_hmac, auth_message.as_bytes());

            let client_proof: Vec<u8> = client_key
                .as_ref()
                .iter()
                .zip(client_sig.as_ref().iter())
                .map(|(k, s)| k ^ s)
                .collect();

            let client_final = format!(
                "{},p={}",
                client_final_without_proof,
                BASE64_STANDARD.encode(&client_proof)
            );

            // SaslAuthenticate Round 2: client-final
            let auth2_hdr =
                RequestHeader::new(ApiKey::SaslAuthenticate, 0, 303, Some("scram-client"));
            let mut auth2_buf = BytesMut::new();
            auth2_hdr.encode(&mut auth2_buf);
            let auth2_req = SaslAuthenticateRequest {
                auth_bytes: Bytes::from(client_final),
            };
            auth2_req.encode(&mut auth2_buf, 0);
            let mut auth2_frame = BytesMut::new();
            auth2_frame.put_i32(auth2_buf.len() as i32);
            auth2_frame.put_slice(&auth2_buf);
            client3.write_all(&auth2_frame).await.unwrap();
            client3.flush().await.unwrap();

            let auth2_len = client3.read_i32().await.unwrap() as usize;
            let mut auth2_resp_buf = vec![0u8; auth2_len];
            client3.read_exact(&mut auth2_resp_buf).await.unwrap();
            let mut auth2_bytes = Bytes::from(auth2_resp_buf);
            let _ = ResponseHeader::decode(&mut auth2_bytes).unwrap();
            let auth2_resp = SaslAuthenticateResponse::decode(&mut auth2_bytes, 0).unwrap();
            assert_eq!(auth2_resp.error_code, KafkaErrorCode::None);

            let server_final_str = std::str::from_utf8(&auth2_resp.auth_bytes).unwrap();
            assert!(server_final_str.starts_with("v="));

            // Produce a record over authenticated connection
            let prod_hdr = RequestHeader::new(ApiKey::Produce, 0, 304, Some("scram-client"));
            let mut prod_buf = BytesMut::new();
            prod_hdr.encode(&mut prod_buf);
            let prod_req = ProduceRequest {
                acks: 1,
                timeout_ms: 5000,
                topic_data: vec![TopicProduceData {
                    topic: "authenticated-topic".into(),
                    partitions: vec![PartitionProduceData {
                        partition: 0,
                        records: Bytes::from_static(b"authenticated-secret-message"),
                    }],
                }],
            };
            prod_req.encode(&mut prod_buf, 0);
            let mut prod_frame = BytesMut::new();
            prod_frame.put_i32(prod_buf.len() as i32);
            prod_frame.put_slice(&prod_buf);
            client3.write_all(&prod_frame).await.unwrap();
            client3.flush().await.unwrap();

            let p_len = client3.read_i32().await.unwrap() as usize;
            let mut p_buf = vec![0u8; p_len];
            client3.read_exact(&mut p_buf).await.unwrap();
            let mut p_bytes = Bytes::from(p_buf);
            let _ = ResponseHeader::decode(&mut p_bytes).unwrap();
            let prod_resp = ProduceResponse::decode(&mut p_bytes, 0).unwrap();
            assert_eq!(
                prod_resp.responses[0].partitions[0].error_code,
                KafkaErrorCode::None
            );
        }
    }

    #[tokio::test]
    async fn test_e2e_kafka_acls_and_rbac_over_tcp() {
        let wal = Arc::new(MemoryWal::new());
        let storage = Arc::new(MemoryObjectStorage::new());
        let log_cache = Arc::new(LogCache::new(10 * 1024 * 1024));
        let block_cache = Arc::new(BlockCache::new(10 * 1024 * 1024));
        let state = Arc::new(ClusterState::new(
            1,
            "127.0.0.1",
            9092,
            "compat-cluster-acl",
            wal,
            storage,
            log_cache,
            block_cache,
        ));
        let coord = Arc::new(GroupCoordinator::new());

        let auth = Arc::new(SaslAuthenticator::new(
            vec![SaslMechanism::Plain],
            true, // require SASL
        ));
        auth.add_user("admin", "admin-secret");
        auth.add_user("alice", "alice-secret");

        let mut super_users = std::collections::HashSet::new();
        super_users.insert("User:admin".to_string());
        let authorizer = Arc::new(AclAuthorizer::new(
            true, // enable_acls
            super_users,
            false, // deny if no ACL found
        ));

        let engine = Arc::new(
            BrokerEngine::new(state, coord)
                .with_authenticator(auth)
                .with_authorizer(authorizer),
        );

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let engine_clone = Arc::clone(&engine);
        tokio::spawn(async move {
            loop {
                if let Ok((stream, _)) = listener.accept().await {
                    let eng = Arc::clone(&engine_clone);
                    tokio::spawn(async move {
                        let _ = eng.process_connection(stream).await;
                    });
                }
            }
        });

        // Helper async function to authenticate with SASL PLAIN over a TcpStream
        async fn authenticate_plain(stream: &mut TcpStream, user: &str, pass: &str) {
            // 1. SaslHandshake
            let hs_hdr = RequestHeader::new(ApiKey::SaslHandshake, 0, 1, Some(user));
            let mut hs_buf = BytesMut::new();
            hs_hdr.encode(&mut hs_buf);
            let hs_req = SaslHandshakeRequest {
                mechanism: "PLAIN".into(),
            };
            hs_req.encode(&mut hs_buf, 0);
            let mut hs_frame = BytesMut::new();
            hs_frame.put_i32(hs_buf.len() as i32);
            hs_frame.put_slice(&hs_buf);
            stream.write_all(&hs_frame).await.unwrap();
            stream.flush().await.unwrap();

            let hs_len = stream.read_i32().await.unwrap() as usize;
            let mut hs_resp_buf = vec![0u8; hs_len];
            stream.read_exact(&mut hs_resp_buf).await.unwrap();
            let mut hs_bytes = Bytes::from(hs_resp_buf);
            let _ = ResponseHeader::decode(&mut hs_bytes).unwrap();
            let hs_resp = SaslHandshakeResponse::decode(&mut hs_bytes, 0).unwrap();
            assert_eq!(hs_resp.error_code, KafkaErrorCode::None);

            // 2. SaslAuthenticate
            let auth_payload = format!("\0{}\0{}", user, pass);
            let auth_hdr = RequestHeader::new(ApiKey::SaslAuthenticate, 0, 2, Some(user));
            let mut auth_buf = BytesMut::new();
            auth_hdr.encode(&mut auth_buf);
            let auth_req = SaslAuthenticateRequest {
                auth_bytes: Bytes::from(auth_payload),
            };
            auth_req.encode(&mut auth_buf, 0);
            let mut auth_frame = BytesMut::new();
            auth_frame.put_i32(auth_buf.len() as i32);
            auth_frame.put_slice(&auth_buf);
            stream.write_all(&auth_frame).await.unwrap();
            stream.flush().await.unwrap();

            let auth_len = stream.read_i32().await.unwrap() as usize;
            let mut auth_resp_buf = vec![0u8; auth_len];
            stream.read_exact(&mut auth_resp_buf).await.unwrap();
            let mut auth_bytes = Bytes::from(auth_resp_buf);
            let _ = ResponseHeader::decode(&mut auth_bytes).unwrap();
            let auth_resp = SaslAuthenticateResponse::decode(&mut auth_bytes, 0).unwrap();
            assert_eq!(auth_resp.error_code, KafkaErrorCode::None);
        }

        // 1. Connect Alice over TCP & authenticate
        let mut alice_stream = TcpStream::connect(addr).await.unwrap();
        authenticate_plain(&mut alice_stream, "alice", "alice-secret").await;

        // Alice attempts to produce to "orders" -> fails with TopicAuthorizationFailed
        let prod_hdr = RequestHeader::new(ApiKey::Produce, 0, 10, Some("alice"));
        let mut prod_buf = BytesMut::new();
        prod_hdr.encode(&mut prod_buf);
        let prod_req = ProduceRequest {
            acks: 1,
            timeout_ms: 1000,
            topic_data: vec![TopicProduceData {
                topic: "orders".to_string(),
                partitions: vec![PartitionProduceData {
                    partition: 0,
                    records: Bytes::from_static(b"order-data-1"),
                }],
            }],
        };
        prod_req.encode(&mut prod_buf, 0);
        let mut prod_frame = BytesMut::new();
        prod_frame.put_i32(prod_buf.len() as i32);
        prod_frame.put_slice(&prod_buf);
        alice_stream.write_all(&prod_frame).await.unwrap();
        alice_stream.flush().await.unwrap();

        let p_len = alice_stream.read_i32().await.unwrap() as usize;
        let mut p_buf = vec![0u8; p_len];
        alice_stream.read_exact(&mut p_buf).await.unwrap();
        let mut p_bytes = Bytes::from(p_buf);
        let _ = ResponseHeader::decode(&mut p_bytes).unwrap();
        let p_resp = ProduceResponse::decode(&mut p_bytes, 0).unwrap();
        assert_eq!(
            p_resp.responses[0].partitions[0].error_code,
            KafkaErrorCode::TopicAuthorizationFailed
        );

        // 2. Connect Admin over TCP & authenticate
        let mut admin_stream = TcpStream::connect(addr).await.unwrap();
        authenticate_plain(&mut admin_stream, "admin", "admin-secret").await;

        // Admin creates ACL granting Alice Write on topic "orders"
        let create_acls_hdr = RequestHeader::new(ApiKey::CreateAcls, 0, 20, Some("admin"));
        let mut create_buf = BytesMut::new();
        create_acls_hdr.encode(&mut create_buf);
        let create_req = CreateAclsRequest {
            creations: vec![AclCreation {
                resource_type: AclResourceType::Topic as i8,
                resource_name: "orders".to_string(),
                resource_pattern_type: AclResourcePatternType::Literal as i8,
                principal: "User:alice".to_string(),
                host: "*".to_string(),
                operation: AclOperation::Write as i8,
                permission_type: AclPermissionType::Allow as i8,
            }],
        };
        create_req.encode(&mut create_buf, 0);
        let mut create_frame = BytesMut::new();
        create_frame.put_i32(create_buf.len() as i32);
        create_frame.put_slice(&create_buf);
        admin_stream.write_all(&create_frame).await.unwrap();
        admin_stream.flush().await.unwrap();

        let c_len = admin_stream.read_i32().await.unwrap() as usize;
        let mut c_buf = vec![0u8; c_len];
        admin_stream.read_exact(&mut c_buf).await.unwrap();
        let mut c_bytes = Bytes::from(c_buf);
        let _ = ResponseHeader::decode(&mut c_bytes).unwrap();
        let create_resp = CreateAclsResponse::decode(&mut c_bytes, 0).unwrap();
        assert_eq!(create_resp.results[0].error_code, KafkaErrorCode::None);

        // 3. Alice retries Produce over her existing TCP connection -> succeeds!
        let mut prod2_buf = BytesMut::new();
        prod_hdr.encode(&mut prod2_buf);
        prod_req.encode(&mut prod2_buf, 0);
        let mut prod2_frame = BytesMut::new();
        prod2_frame.put_i32(prod2_buf.len() as i32);
        prod2_frame.put_slice(&prod2_buf);
        alice_stream.write_all(&prod2_frame).await.unwrap();
        alice_stream.flush().await.unwrap();

        let p2_len = alice_stream.read_i32().await.unwrap() as usize;
        let mut p2_buf = vec![0u8; p2_len];
        alice_stream.read_exact(&mut p2_buf).await.unwrap();
        let mut p2_bytes = Bytes::from(p2_buf);
        let _ = ResponseHeader::decode(&mut p2_bytes).unwrap();
        let p2_resp = ProduceResponse::decode(&mut p2_bytes, 0).unwrap();
        assert_eq!(
            p2_resp.responses[0].partitions[0].error_code,
            KafkaErrorCode::None
        );
        assert_eq!(p2_resp.responses[0].partitions[0].base_offset, 0);

        // 4. Alice attempts Fetch from "orders" -> fails with TopicAuthorizationFailed (she has Write, not Read)
        let fetch_hdr = RequestHeader::new(ApiKey::Fetch, 0, 30, Some("alice"));
        let mut fetch_buf = BytesMut::new();
        fetch_hdr.encode(&mut fetch_buf);
        let fetch_req = FetchRequest {
            max_wait_ms: 1000,
            min_bytes: 1,
            max_bytes: 1024,
            isolation_level: 0,
            topics: vec![FetchTopic {
                topic: "orders".to_string(),
                partitions: vec![FetchPartition {
                    partition: 0,
                    fetch_offset: 0,
                    partition_max_bytes: 1024,
                }],
            }],
        };
        fetch_req.encode(&mut fetch_buf, 0);
        let mut fetch_frame = BytesMut::new();
        fetch_frame.put_i32(fetch_buf.len() as i32);
        fetch_frame.put_slice(&fetch_buf);
        alice_stream.write_all(&fetch_frame).await.unwrap();
        alice_stream.flush().await.unwrap();

        let f_len = alice_stream.read_i32().await.unwrap() as usize;
        let mut f_buf = vec![0u8; f_len];
        alice_stream.read_exact(&mut f_buf).await.unwrap();
        let mut f_bytes = Bytes::from(f_buf);
        let _ = ResponseHeader::decode(&mut f_bytes).unwrap();
        let f_resp = FetchResponse::decode(&mut f_bytes, 0).unwrap();
        assert_eq!(
            f_resp.responses[0].partitions[0].error_code,
            KafkaErrorCode::TopicAuthorizationFailed
        );

        // 5. Admin creates ACL granting Alice Read on topic "orders"
        let create_read_hdr = RequestHeader::new(ApiKey::CreateAcls, 0, 31, Some("admin"));
        let mut cr_buf = BytesMut::new();
        create_read_hdr.encode(&mut cr_buf);
        let create_read_req = CreateAclsRequest {
            creations: vec![AclCreation {
                resource_type: AclResourceType::Topic as i8,
                resource_name: "orders".to_string(),
                resource_pattern_type: AclResourcePatternType::Literal as i8,
                principal: "User:alice".to_string(),
                host: "*".to_string(),
                operation: AclOperation::Read as i8,
                permission_type: AclPermissionType::Allow as i8,
            }],
        };
        create_read_req.encode(&mut cr_buf, 0);
        let mut cr_frame = BytesMut::new();
        cr_frame.put_i32(cr_buf.len() as i32);
        cr_frame.put_slice(&cr_buf);
        admin_stream.write_all(&cr_frame).await.unwrap();
        admin_stream.flush().await.unwrap();

        let cr_len = admin_stream.read_i32().await.unwrap() as usize;
        let mut cr_buf_resp = vec![0u8; cr_len];
        admin_stream.read_exact(&mut cr_buf_resp).await.unwrap();
        let mut cr_bytes = Bytes::from(cr_buf_resp);
        let _ = ResponseHeader::decode(&mut cr_bytes).unwrap();
        let cr_resp = CreateAclsResponse::decode(&mut cr_bytes, 0).unwrap();
        assert_eq!(cr_resp.results[0].error_code, KafkaErrorCode::None);

        // 6. Alice retries Fetch over her connection -> succeeds and receives records!
        let mut fetch2_buf = BytesMut::new();
        fetch_hdr.encode(&mut fetch2_buf);
        fetch_req.encode(&mut fetch2_buf, 0);
        let mut fetch2_frame = BytesMut::new();
        fetch2_frame.put_i32(fetch2_buf.len() as i32);
        fetch2_frame.put_slice(&fetch2_buf);
        alice_stream.write_all(&fetch2_frame).await.unwrap();
        alice_stream.flush().await.unwrap();

        let f2_len = alice_stream.read_i32().await.unwrap() as usize;
        let mut f2_buf = vec![0u8; f2_len];
        alice_stream.read_exact(&mut f2_buf).await.unwrap();
        let mut f2_bytes = Bytes::from(f2_buf);
        let _ = ResponseHeader::decode(&mut f2_bytes).unwrap();
        let f2_resp = FetchResponse::decode(&mut f2_bytes, 0).unwrap();
        assert_eq!(
            f2_resp.responses[0].partitions[0].error_code,
            KafkaErrorCode::None
        );
        assert!(!f2_resp.responses[0].partitions[0].records.is_empty());

        // 7. Admin calls DescribeAcls -> returns Alice's 2 ACLs
        let desc_hdr = RequestHeader::new(ApiKey::DescribeAcls, 0, 40, Some("admin"));
        let mut desc_buf = BytesMut::new();
        desc_hdr.encode(&mut desc_buf);
        let desc_req = DescribeAclsRequest {
            resource_type_filter: 1, // Any
            resource_name_filter: Some("orders".to_string()),
            resource_pattern_type_filter: 1,
            principal_filter: None,
            host_filter: None,
            operation: 1,
            permission_type: 1,
        };
        desc_req.encode(&mut desc_buf, 0);
        let mut desc_frame = BytesMut::new();
        desc_frame.put_i32(desc_buf.len() as i32);
        desc_frame.put_slice(&desc_buf);
        admin_stream.write_all(&desc_frame).await.unwrap();
        admin_stream.flush().await.unwrap();

        let d_len = admin_stream.read_i32().await.unwrap() as usize;
        let mut d_buf = vec![0u8; d_len];
        admin_stream.read_exact(&mut d_buf).await.unwrap();
        let mut d_bytes = Bytes::from(d_buf);
        let _ = ResponseHeader::decode(&mut d_bytes).unwrap();
        let desc_resp = DescribeAclsResponse::decode(&mut d_bytes, 0).unwrap();
        assert_eq!(desc_resp.error_code, KafkaErrorCode::None);
        assert_eq!(desc_resp.resources.len(), 1);
        assert_eq!(desc_resp.resources[0].acls.len(), 2);

        // 8. Admin calls DeleteAcls -> removes both ACLs
        let del_hdr = RequestHeader::new(ApiKey::DeleteAcls, 0, 50, Some("admin"));
        let mut del_buf = BytesMut::new();
        del_hdr.encode(&mut del_buf);
        let del_req = DeleteAclsRequest {
            filters: vec![DeleteAclsFilter {
                resource_type_filter: 1,
                resource_name_filter: Some("orders".to_string()),
                resource_pattern_type_filter: 1,
                principal_filter: None,
                host_filter: None,
                operation: 1,
                permission_type: 1,
            }],
        };
        del_req.encode(&mut del_buf, 0);
        let mut del_frame = BytesMut::new();
        del_frame.put_i32(del_buf.len() as i32);
        del_frame.put_slice(&del_buf);
        admin_stream.write_all(&del_frame).await.unwrap();
        admin_stream.flush().await.unwrap();

        let del_len = admin_stream.read_i32().await.unwrap() as usize;
        let mut del_buf_resp = vec![0u8; del_len];
        admin_stream.read_exact(&mut del_buf_resp).await.unwrap();
        let mut del_bytes = Bytes::from(del_buf_resp);
        let _ = ResponseHeader::decode(&mut del_bytes).unwrap();
        let del_resp = DeleteAclsResponse::decode(&mut del_bytes, 0).unwrap();
        assert_eq!(del_resp.filter_results[0].error_code, KafkaErrorCode::None);
        assert_eq!(del_resp.filter_results[0].matching_acls.len(), 2);

        // 9. Alice produces again -> rejected with TopicAuthorizationFailed
        let mut prod3_buf = BytesMut::new();
        prod_hdr.encode(&mut prod3_buf);
        prod_req.encode(&mut prod3_buf, 0);
        let mut prod3_frame = BytesMut::new();
        prod3_frame.put_i32(prod3_buf.len() as i32);
        prod3_frame.put_slice(&prod3_buf);
        alice_stream.write_all(&prod3_frame).await.unwrap();
        alice_stream.flush().await.unwrap();

        let p3_len = alice_stream.read_i32().await.unwrap() as usize;
        let mut p3_buf = vec![0u8; p3_len];
        alice_stream.read_exact(&mut p3_buf).await.unwrap();
        let mut p3_bytes = Bytes::from(p3_buf);
        let _ = ResponseHeader::decode(&mut p3_bytes).unwrap();
        let p3_resp = ProduceResponse::decode(&mut p3_bytes, 0).unwrap();
        assert_eq!(
            p3_resp.responses[0].partitions[0].error_code,
            KafkaErrorCode::TopicAuthorizationFailed
        );
    }

    #[tokio::test]
    async fn test_e2e_create_and_delete_topics_over_tcp() {
        let engine = Arc::new(setup_test_engine());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let engine_clone = Arc::clone(&engine);
        tokio::spawn(async move {
            loop {
                if let Ok((stream, _)) = listener.accept().await {
                    let eng = Arc::clone(&engine_clone);
                    tokio::spawn(async move {
                        let _ = eng.process_connection(stream).await;
                    });
                }
            }
        });

        let mut stream = TcpStream::connect(addr).await.unwrap();

        // 1. ApiVersions negotiation
        let av_hdr = RequestHeader::new(ApiKey::ApiVersions, 0, 1, Some("topic-mgr"));
        let mut av_buf = BytesMut::new();
        av_hdr.encode(&mut av_buf);
        let av_req = ApiVersionsRequest::default();
        av_req.encode(&mut av_buf, 0);
        let mut av_frame = BytesMut::new();
        av_frame.put_i32(av_buf.len() as i32);
        av_frame.put_slice(&av_buf);
        stream.write_all(&av_frame).await.unwrap();
        stream.flush().await.unwrap();

        let av_len = stream.read_i32().await.unwrap() as usize;
        let mut av_resp_buf = vec![0u8; av_len];
        stream.read_exact(&mut av_resp_buf).await.unwrap();
        let mut av_bytes = Bytes::from(av_resp_buf);
        let _ = ResponseHeader::decode(&mut av_bytes).unwrap();
        let av_resp = ApiVersionsResponse::decode(&mut av_bytes, 0).unwrap();
        assert_eq!(av_resp.error_code, KafkaErrorCode::None);

        // 2. CreateTopics: "telemetry-events" with 3 partitions
        let ct_hdr = RequestHeader::new(ApiKey::CreateTopics, 1, 2, Some("topic-mgr"));
        let mut ct_buf = BytesMut::new();
        ct_hdr.encode(&mut ct_buf);
        let ct_req = CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: "telemetry-events".into(),
                num_partitions: 3,
                replication_factor: 1,
                assignments: Vec::new(),
                configs: vec![CreateTopicsConfig {
                    name: "retention.ms".into(),
                    value: Some("300000".into()),
                }],
            }],
            timeout_ms: 5000,
            validate_only: false,
        };
        ct_req.encode(&mut ct_buf, 1);
        let mut ct_frame = BytesMut::new();
        ct_frame.put_i32(ct_buf.len() as i32);
        ct_frame.put_slice(&ct_buf);
        stream.write_all(&ct_frame).await.unwrap();
        stream.flush().await.unwrap();

        let ct_len = stream.read_i32().await.unwrap() as usize;
        let mut ct_resp_buf = vec![0u8; ct_len];
        stream.read_exact(&mut ct_resp_buf).await.unwrap();
        let mut ct_bytes = Bytes::from(ct_resp_buf);
        let _ = ResponseHeader::decode(&mut ct_bytes).unwrap();
        let ct_resp = CreateTopicsResponse::decode(&mut ct_bytes, 1).unwrap();
        assert_eq!(ct_resp.topics.len(), 1);
        assert_eq!(ct_resp.topics[0].name, "telemetry-events");
        assert_eq!(ct_resp.topics[0].error_code, KafkaErrorCode::None);

        // 3. Metadata without auto-create: verify topic and its 3 partitions
        let meta_hdr = RequestHeader::new(ApiKey::Metadata, 1, 3, Some("topic-mgr"));
        let mut meta_buf = BytesMut::new();
        meta_hdr.encode(&mut meta_buf);
        let meta_req = MetadataRequest {
            topics: Some(vec!["telemetry-events".into()]),
            allow_auto_topic_creation: false,
        };
        meta_req.encode(&mut meta_buf, 1);
        let mut meta_frame = BytesMut::new();
        meta_frame.put_i32(meta_buf.len() as i32);
        meta_frame.put_slice(&meta_buf);
        stream.write_all(&meta_frame).await.unwrap();
        stream.flush().await.unwrap();

        let meta_len = stream.read_i32().await.unwrap() as usize;
        let mut meta_resp_buf = vec![0u8; meta_len];
        stream.read_exact(&mut meta_resp_buf).await.unwrap();
        let mut meta_bytes = Bytes::from(meta_resp_buf);
        let _ = ResponseHeader::decode(&mut meta_bytes).unwrap();
        let meta_resp = MetadataResponse::decode(&mut meta_bytes, 1).unwrap();
        assert_eq!(meta_resp.topics.len(), 1);
        assert_eq!(meta_resp.topics[0].name, "telemetry-events");
        assert_eq!(meta_resp.topics[0].error_code, KafkaErrorCode::None);
        assert_eq!(meta_resp.topics[0].partitions.len(), 3);
        assert_eq!(meta_resp.topics[0].partitions[0].partition_index, 0);
        assert_eq!(meta_resp.topics[0].partitions[1].partition_index, 1);
        assert_eq!(meta_resp.topics[0].partitions[2].partition_index, 2);

        // 4. Duplicate CreateTopics -> TopicAlreadyExists
        let dup_hdr = RequestHeader::new(ApiKey::CreateTopics, 1, 4, Some("topic-mgr"));
        let mut dup_buf = BytesMut::new();
        dup_hdr.encode(&mut dup_buf);
        ct_req.encode(&mut dup_buf, 1);
        let mut dup_frame = BytesMut::new();
        dup_frame.put_i32(dup_buf.len() as i32);
        dup_frame.put_slice(&dup_buf);
        stream.write_all(&dup_frame).await.unwrap();
        stream.flush().await.unwrap();

        let dup_len = stream.read_i32().await.unwrap() as usize;
        let mut dup_resp_buf = vec![0u8; dup_len];
        stream.read_exact(&mut dup_resp_buf).await.unwrap();
        let mut dup_bytes = Bytes::from(dup_resp_buf);
        let _ = ResponseHeader::decode(&mut dup_bytes).unwrap();
        let dup_resp = CreateTopicsResponse::decode(&mut dup_bytes, 1).unwrap();
        assert_eq!(
            dup_resp.topics[0].error_code,
            KafkaErrorCode::TopicAlreadyExists
        );

        // 5. Produce to partition 2 of created topic
        let prod_hdr = RequestHeader::new(ApiKey::Produce, 0, 5, Some("topic-mgr"));
        let mut prod_buf = BytesMut::new();
        prod_hdr.encode(&mut prod_buf);
        let records_payload = Bytes::from_static(b"sample telemetry batch data");
        let prod_req = ProduceRequest {
            acks: 1,
            timeout_ms: 1000,
            topic_data: vec![TopicProduceData {
                topic: "telemetry-events".into(),
                partitions: vec![PartitionProduceData {
                    partition: 2,
                    records: records_payload.clone(),
                }],
            }],
        };
        prod_req.encode(&mut prod_buf, 0);
        let mut prod_frame = BytesMut::new();
        prod_frame.put_i32(prod_buf.len() as i32);
        prod_frame.put_slice(&prod_buf);
        stream.write_all(&prod_frame).await.unwrap();
        stream.flush().await.unwrap();

        let prod_len = stream.read_i32().await.unwrap() as usize;
        let mut prod_resp_buf = vec![0u8; prod_len];
        stream.read_exact(&mut prod_resp_buf).await.unwrap();
        let mut prod_bytes = Bytes::from(prod_resp_buf);
        let _ = ResponseHeader::decode(&mut prod_bytes).unwrap();
        let prod_resp = ProduceResponse::decode(&mut prod_bytes, 0).unwrap();
        assert_eq!(
            prod_resp.responses[0].partitions[0].error_code,
            KafkaErrorCode::None
        );
        assert_eq!(prod_resp.responses[0].partitions[0].partition, 2);

        // 6. Fetch from partition 2
        let fetch_hdr = RequestHeader::new(ApiKey::Fetch, 0, 6, Some("topic-mgr"));
        let mut fetch_buf = BytesMut::new();
        fetch_hdr.encode(&mut fetch_buf);
        let fetch_req = FetchRequest {
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 65536,
            isolation_level: 0,
            topics: vec![FetchTopic {
                topic: "telemetry-events".into(),
                partitions: vec![FetchPartition {
                    partition: 2,
                    fetch_offset: 0,
                    partition_max_bytes: 65536,
                }],
            }],
        };
        fetch_req.encode(&mut fetch_buf, 0);
        let mut fetch_frame = BytesMut::new();
        fetch_frame.put_i32(fetch_buf.len() as i32);
        fetch_frame.put_slice(&fetch_buf);
        stream.write_all(&fetch_frame).await.unwrap();
        stream.flush().await.unwrap();

        let fetch_len = stream.read_i32().await.unwrap() as usize;
        let mut fetch_resp_buf = vec![0u8; fetch_len];
        stream.read_exact(&mut fetch_resp_buf).await.unwrap();
        let mut fetch_bytes = Bytes::from(fetch_resp_buf);
        let _ = ResponseHeader::decode(&mut fetch_bytes).unwrap();
        let fetch_resp = FetchResponse::decode(&mut fetch_bytes, 0).unwrap();
        assert_eq!(
            fetch_resp.responses[0].partitions[0].error_code,
            KafkaErrorCode::None
        );
        assert_eq!(
            fetch_resp.responses[0].partitions[0].records,
            records_payload
        );

        // 7. DeleteTopics: remove "telemetry-events"
        let del_hdr = RequestHeader::new(ApiKey::DeleteTopics, 1, 7, Some("topic-mgr"));
        let mut del_buf = BytesMut::new();
        del_hdr.encode(&mut del_buf);
        let del_req = DeleteTopicsRequest {
            topic_names: vec!["telemetry-events".into()],
            timeout_ms: 5000,
        };
        del_req.encode(&mut del_buf, 1);
        let mut del_frame = BytesMut::new();
        del_frame.put_i32(del_buf.len() as i32);
        del_frame.put_slice(&del_buf);
        stream.write_all(&del_frame).await.unwrap();
        stream.flush().await.unwrap();

        let del_len = stream.read_i32().await.unwrap() as usize;
        let mut del_resp_buf = vec![0u8; del_len];
        stream.read_exact(&mut del_resp_buf).await.unwrap();
        let mut del_bytes = Bytes::from(del_resp_buf);
        let _ = ResponseHeader::decode(&mut del_bytes).unwrap();
        let del_resp = DeleteTopicsResponse::decode(&mut del_bytes, 1).unwrap();
        assert_eq!(del_resp.responses.len(), 1);
        assert_eq!(del_resp.responses[0].name, "telemetry-events");
        assert_eq!(del_resp.responses[0].error_code, KafkaErrorCode::None);

        // 8. Metadata query without auto-create -> UnknownTopicOrPartition
        let meta_hdr2 = RequestHeader::new(ApiKey::Metadata, 1, 8, Some("topic-mgr"));
        let mut meta_buf2 = BytesMut::new();
        meta_hdr2.encode(&mut meta_buf2);
        meta_req.encode(&mut meta_buf2, 1);
        let mut meta_frame2 = BytesMut::new();
        meta_frame2.put_i32(meta_buf2.len() as i32);
        meta_frame2.put_slice(&meta_buf2);
        stream.write_all(&meta_frame2).await.unwrap();
        stream.flush().await.unwrap();

        let meta2_len = stream.read_i32().await.unwrap() as usize;
        let mut meta2_resp_buf = vec![0u8; meta2_len];
        stream.read_exact(&mut meta2_resp_buf).await.unwrap();
        let mut meta2_bytes = Bytes::from(meta2_resp_buf);
        let _ = ResponseHeader::decode(&mut meta2_bytes).unwrap();
        let meta2_resp = MetadataResponse::decode(&mut meta2_bytes, 1).unwrap();
        assert_eq!(meta2_resp.topics.len(), 1);
        assert_eq!(meta2_resp.topics[0].name, "telemetry-events");
        assert_eq!(
            meta2_resp.topics[0].error_code,
            KafkaErrorCode::UnknownTopicOrPartition
        );
        assert_eq!(meta2_resp.topics[0].partitions.len(), 0);

        // 9. Second DeleteTopics -> UnknownTopicOrPartition
        let del_hdr2 = RequestHeader::new(ApiKey::DeleteTopics, 1, 9, Some("topic-mgr"));
        let mut del_buf2 = BytesMut::new();
        del_hdr2.encode(&mut del_buf2);
        del_req.encode(&mut del_buf2, 1);
        let mut del_frame2 = BytesMut::new();
        del_frame2.put_i32(del_buf2.len() as i32);
        del_frame2.put_slice(&del_buf2);
        stream.write_all(&del_frame2).await.unwrap();
        stream.flush().await.unwrap();

        let del2_len = stream.read_i32().await.unwrap() as usize;
        let mut del2_resp_buf = vec![0u8; del2_len];
        stream.read_exact(&mut del2_resp_buf).await.unwrap();
        let mut del2_bytes = Bytes::from(del2_resp_buf);
        let _ = ResponseHeader::decode(&mut del2_bytes).unwrap();
        let del2_resp = DeleteTopicsResponse::decode(&mut del2_bytes, 1).unwrap();
        assert_eq!(
            del2_resp.responses[0].error_code,
            KafkaErrorCode::UnknownTopicOrPartition
        );
    }
}
