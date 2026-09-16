use crate::acl::AclAuthorizer;
use crate::chaos::{ChaosEngine, FaultTarget};
use crate::coordinator::GroupCoordinator;
use crate::router::ClusterState;
use crate::sasl::{ConnectionAuthState, SaslAuthenticator, SaslMechanism, ScramServerSession};
use crate::schema_registry::SchemaRegistry;
use crate::transaction::TransactionCoordinator;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use oxidemq_core::error::{OxideMqError, Result};
use oxidemq_core::types::{CompressionCodec, TopicPartition};
use oxidemq_protocol::header::{RequestHeader, ResponseHeader};
use oxidemq_protocol::messages::{
    AclCreationResult, AddOffsetsToTxnRequest, AddOffsetsToTxnResponse,
    AddPartitionsToTxnPartitionResult, AddPartitionsToTxnRequest, AddPartitionsToTxnResponse,
    AddPartitionsToTxnTopicResult, ApiVersionsRequest, ApiVersionsResponse, CreateAclsRequest,
    CreateAclsResponse, DeleteAclsFilterResult, DeleteAclsRequest, DeleteAclsResponse,
    DescribeAclsRequest, DescribeAclsResponse, EndTxnRequest, EndTxnResponse,
    FetchPartitionResponse, FetchRequest, FetchResponse, FetchTopicResponse,
    FindCoordinatorRequest, FindCoordinatorResponse, HeartbeatRequest, HeartbeatResponse,
    InitProducerIdRequest, InitProducerIdResponse, LeaveGroupRequest, LeaveGroupResponse,
    ListOffsetsPartitionResponse, ListOffsetsRequest, ListOffsetsResponse,
    ListOffsetsTopicResponse, MetadataRequest, OffsetCommitPartitionResponse, OffsetCommitRequest,
    OffsetCommitResponse, OffsetCommitTopicResponse, OffsetFetchPartitionResponse,
    OffsetFetchRequest, OffsetFetchResponse, OffsetFetchTopicResponse, PartitionProduceResponse,
    ProduceRequest, ProduceResponse, SaslAuthenticateRequest, SaslAuthenticateResponse,
    SaslHandshakeRequest, SaslHandshakeResponse, TopicProduceResponse,
};
use oxidemq_protocol::{AclOperation, AclResourceType, ApiKey, KafkaErrorCode};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tracing::{debug, error, trace};

/// Central broker request execution engine.
#[derive(Clone)]
pub struct BrokerEngine {
    cluster_state: Arc<ClusterState>,
    coordinator: Arc<GroupCoordinator>,
    txn_coordinator: Arc<TransactionCoordinator>,
    schema_registry: Arc<SchemaRegistry>,
    authenticator: Arc<SaslAuthenticator>,
    authorizer: Arc<AclAuthorizer>,
    chaos: Arc<ChaosEngine>,
    enable_schema_validation: bool,
}

impl BrokerEngine {
    pub fn new(cluster_state: Arc<ClusterState>, coordinator: Arc<GroupCoordinator>) -> Self {
        Self {
            cluster_state,
            coordinator,
            txn_coordinator: Arc::new(TransactionCoordinator::new()),
            schema_registry: Arc::new(SchemaRegistry::new()),
            authenticator: Arc::new(SaslAuthenticator::default()),
            authorizer: Arc::new(AclAuthorizer::default()),
            chaos: Arc::new(ChaosEngine::new()),
            enable_schema_validation: false,
        }
    }

    pub fn with_chaos(mut self, chaos: Arc<ChaosEngine>) -> Self {
        self.chaos = chaos;
        self
    }

    pub fn with_txn_coordinator(mut self, txn_coordinator: Arc<TransactionCoordinator>) -> Self {
        self.txn_coordinator = txn_coordinator;
        self
    }

    pub fn with_schema_registry(mut self, schema_registry: Arc<SchemaRegistry>) -> Self {
        self.schema_registry = schema_registry;
        self
    }

    pub fn with_authenticator(mut self, authenticator: Arc<SaslAuthenticator>) -> Self {
        self.authenticator = authenticator;
        self
    }

    pub fn with_authorizer(mut self, authorizer: Arc<AclAuthorizer>) -> Self {
        self.authorizer = authorizer;
        self
    }

    pub fn with_schema_validation(mut self, enabled: bool) -> Self {
        self.enable_schema_validation = enabled;
        self
    }

    pub fn authorizer(&self) -> &Arc<AclAuthorizer> {
        &self.authorizer
    }

    pub fn authenticator(&self) -> &Arc<SaslAuthenticator> {
        &self.authenticator
    }

    pub fn cluster_state(&self) -> &Arc<ClusterState> {
        &self.cluster_state
    }

    pub fn coordinator(&self) -> &Arc<GroupCoordinator> {
        &self.coordinator
    }

    pub fn txn_coordinator(&self) -> &Arc<TransactionCoordinator> {
        &self.txn_coordinator
    }

    pub fn schema_registry(&self) -> &Arc<SchemaRegistry> {
        &self.schema_registry
    }

    pub fn is_schema_validation_enabled(&self) -> bool {
        self.enable_schema_validation
    }

    pub fn chaos(&self) -> &Arc<ChaosEngine> {
        &self.chaos
    }

    /// Dispatches and processes an individual Kafka request given its header and body payload.
    /// Returns the serialized ResponseHeader + ResponsePayload (unframed).
    pub fn handle_request(&self, header: &RequestHeader, body: &mut Bytes) -> Result<BytesMut> {
        let mut session = ConnectionAuthState::Authenticated {
            principal: "ANONYMOUS".to_string(),
        };
        self.handle_connection_request(&mut session, header, body)
    }

    /// Dispatches and processes a Kafka request within an active connection session.
    pub fn handle_connection_request(
        &self,
        session: &mut ConnectionAuthState,
        header: &RequestHeader,
        body: &mut Bytes,
    ) -> Result<BytesMut> {
        let mut out = BytesMut::with_capacity(1024);
        let resp_header = ResponseHeader::new(header.correlation_id);
        resp_header.encode(&mut out);

        // Security check: if SASL is required and connection is not authenticated, reject non-auth requests
        if self.authenticator.is_sasl_required()
            && !session.is_authenticated()
            && header.api_key != ApiKey::ApiVersions
            && header.api_key != ApiKey::SaslHandshake
            && header.api_key != ApiKey::SaslAuthenticate
        {
            return Err(OxideMqError::Protocol(format!(
                "Rejected unauthenticated request {:?}: SASL authentication required on this broker",
                header.api_key
            )));
        }

        let principal = session.principal().unwrap_or("ANONYMOUS");
        let client_host = "*";

        match header.api_key {
            ApiKey::ApiVersions => {
                let _req = ApiVersionsRequest::decode(body, header.api_version)?;
                let resp = ApiVersionsResponse::default_supported();
                resp.encode(&mut out, header.api_version);
            }
            ApiKey::SaslHandshake => {
                let req = SaslHandshakeRequest::decode(body, header.api_version)?;
                let mech = SaslMechanism::from_str_case_insensitive(&req.mechanism);
                let (error_code, enabled_mechanisms) = match mech {
                    Some(m) if self.authenticator.is_mechanism_enabled(m) => {
                        *session = ConnectionAuthState::HandshakeReceived { mechanism: m };
                        (
                            KafkaErrorCode::None,
                            self.authenticator.enabled_mechanism_names(),
                        )
                    }
                    _ => (
                        KafkaErrorCode::UnsupportedSaslMechanism,
                        self.authenticator.enabled_mechanism_names(),
                    ),
                };
                let resp = SaslHandshakeResponse {
                    error_code,
                    enabled_mechanisms,
                };
                resp.encode(&mut out, header.api_version);
            }
            ApiKey::SaslAuthenticate => {
                let req = SaslAuthenticateRequest::decode(body, header.api_version)?;
                match session {
                    ConnectionAuthState::HandshakeReceived {
                        mechanism: SaslMechanism::Plain,
                    } => match self.authenticator.authenticate_plain(&req.auth_bytes) {
                        Ok(user) => {
                            *session = ConnectionAuthState::Authenticated { principal: user };
                            let resp = SaslAuthenticateResponse {
                                error_code: KafkaErrorCode::None,
                                error_message: None,
                                auth_bytes: Bytes::new(),
                                session_lifetime_ms: 0,
                            };
                            resp.encode(&mut out, header.api_version);
                        }
                        Err(err) => {
                            *session = ConnectionAuthState::Failed;
                            let resp = SaslAuthenticateResponse {
                                error_code: err,
                                error_message: Some("SASL PLAIN authentication failed".into()),
                                auth_bytes: Bytes::new(),
                                session_lifetime_ms: 0,
                            };
                            resp.encode(&mut out, header.api_version);
                        }
                    },
                    ConnectionAuthState::HandshakeReceived {
                        mechanism: m @ (SaslMechanism::ScramSha256 | SaslMechanism::ScramSha512),
                    } => {
                        let mut scram_session = ScramServerSession::new(*m);
                        match scram_session.process_client_first(&req.auth_bytes, None, None) {
                            Ok(server_first) => {
                                *session = ConnectionAuthState::ScramChallengeSent {
                                    session: scram_session,
                                };
                                let resp = SaslAuthenticateResponse {
                                    error_code: KafkaErrorCode::None,
                                    error_message: None,
                                    auth_bytes: Bytes::from(server_first),
                                    session_lifetime_ms: 0,
                                };
                                resp.encode(&mut out, header.api_version);
                            }
                            Err(err) => {
                                *session = ConnectionAuthState::Failed;
                                let resp = SaslAuthenticateResponse {
                                    error_code: err,
                                    error_message: Some(
                                        "SCRAM client-first message validation failed".into(),
                                    ),
                                    auth_bytes: Bytes::new(),
                                    session_lifetime_ms: 0,
                                };
                                resp.encode(&mut out, header.api_version);
                            }
                        }
                    }
                    ConnectionAuthState::ScramChallengeSent {
                        session: scram_session,
                    } => {
                        let password = self.authenticator.get_password(&scram_session.username);
                        match password {
                            Some(pass) => {
                                match scram_session.process_client_final(&req.auth_bytes, &pass) {
                                    Ok((user, server_final)) => {
                                        *session =
                                            ConnectionAuthState::Authenticated { principal: user };
                                        let resp = SaslAuthenticateResponse {
                                            error_code: KafkaErrorCode::None,
                                            error_message: None,
                                            auth_bytes: Bytes::from(server_final),
                                            session_lifetime_ms: 0,
                                        };
                                        resp.encode(&mut out, header.api_version);
                                    }
                                    Err(err) => {
                                        *session = ConnectionAuthState::Failed;
                                        let resp = SaslAuthenticateResponse {
                                            error_code: err,
                                            error_message: Some(
                                                "SCRAM proof verification failed".into(),
                                            ),
                                            auth_bytes: Bytes::new(),
                                            session_lifetime_ms: 0,
                                        };
                                        resp.encode(&mut out, header.api_version);
                                    }
                                }
                            }
                            None => {
                                *session = ConnectionAuthState::Failed;
                                let resp = SaslAuthenticateResponse {
                                    error_code: KafkaErrorCode::SaslAuthenticationFailed,
                                    error_message: Some("Unknown user".into()),
                                    auth_bytes: Bytes::new(),
                                    session_lifetime_ms: 0,
                                };
                                resp.encode(&mut out, header.api_version);
                            }
                        }
                    }
                    _ => {
                        let resp = SaslAuthenticateResponse {
                            error_code: KafkaErrorCode::IllegalSaslState,
                            error_message: Some(
                                "SaslAuthenticate request received in unexpected state".into(),
                            ),
                            auth_bytes: Bytes::new(),
                            session_lifetime_ms: 0,
                        };
                        resp.encode(&mut out, header.api_version);
                    }
                }
            }
            ApiKey::Metadata => {
                let req = MetadataRequest::decode(body, header.api_version)?;
                let mut resp = self.cluster_state.build_metadata(req.topics.as_deref());
                for topic in &mut resp.topics {
                    if !self.authorizer.authorize(
                        principal,
                        client_host,
                        AclResourceType::Topic,
                        &topic.name,
                        AclOperation::Describe,
                    ) {
                        topic.error_code = KafkaErrorCode::TopicAuthorizationFailed;
                        topic.partitions.clear();
                    }
                }
                resp.encode(&mut out, header.api_version);
            }
            ApiKey::Produce => {
                let req = ProduceRequest::decode(body, header.api_version)?;
                let mut topic_responses = Vec::with_capacity(req.topic_data.len());
                let fault = self.chaos.check_fault(FaultTarget::Produce);

                for t in req.topic_data {
                    let is_authorized = self.authorizer.authorize(
                        principal,
                        client_host,
                        AclResourceType::Topic,
                        &t.topic,
                        AclOperation::Write,
                    );
                    let mut part_responses = Vec::with_capacity(t.partitions.len());
                    for p in t.partitions {
                        if !is_authorized {
                            part_responses.push(PartitionProduceResponse {
                                partition: p.partition,
                                error_code: KafkaErrorCode::TopicAuthorizationFailed,
                                base_offset: -1,
                                log_append_time_ms: -1,
                                log_start_offset: 0,
                            });
                            continue;
                        }
                        let tp = TopicPartition::new(&t.topic, p.partition);
                        let partition = self.cluster_state.get_or_create_partition(&tp);

                        let (err, base_off, append_time, start_off) = if let Err(e) = &fault {
                            error!("Chaos fault injected on Produce for {}: {}", tp, e);
                            (KafkaErrorCode::UnknownServer, -1, -1, 0)
                        } else {
                            // Validate and decompress if compressed, ensuring payload integrity
                            match p.decompressed_records() {
                                Err(e) => {
                                    error!(
                                        "Corrupted compressed record batch produced to {}: {}",
                                        tp, e
                                    );
                                    (KafkaErrorCode::CorruptMessage, -1, -1, 0)
                                }
                                Ok(decompressed) => {
                                    if self.enable_schema_validation
                                        && !self.validate_record_payloads(&decompressed)
                                    {
                                        error!(
                                            "Schema validation failed for record produced to {}",
                                            tp
                                        );
                                        (KafkaErrorCode::InvalidRecord, -1, -1, 0)
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
                                    }
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
                    let is_topic_authorized = self.authorizer.authorize(
                        principal,
                        client_host,
                        AclResourceType::Topic,
                        &t.topic,
                        AclOperation::Read,
                    );
                    let mut part_responses = Vec::with_capacity(t.partitions.len());
                    for p in t.partitions {
                        if !is_topic_authorized {
                            part_responses.push(FetchPartitionResponse {
                                partition_index: p.partition,
                                error_code: KafkaErrorCode::TopicAuthorizationFailed,
                                high_watermark: -1,
                                last_stable_offset: -1,
                                records: Bytes::new(),
                            });
                            continue;
                        }

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
                                    let aborted = partition.aborted_transactions();
                                    let lso = partition.last_stable_offset();

                                    for (off, batch_bytes) in batches {
                                        if batch_bytes.len() >= 61 && batch_bytes[16] == 2 {
                                            let attr = i16::from_be_bytes(
                                                batch_bytes[21..23].try_into().unwrap(),
                                            );
                                            let is_ctrl = (attr & 0x0020) != 0;
                                            let is_tx = (attr & 0x0010) != 0;
                                            let pid = i64::from_be_bytes(
                                                batch_bytes[39..47].try_into().unwrap(),
                                            );

                                            // 1. Never expose control batches to consumer applications
                                            if is_ctrl {
                                                continue;
                                            }

                                            // 2. ReadCommitted isolation level:
                                            if req.isolation_level == 1 {
                                                // Do not expose uncommitted batches at or beyond LSO
                                                if off >= lso {
                                                    continue;
                                                }
                                                // Do not expose messages from aborted transactions
                                                if is_tx
                                                    && aborted.iter().any(|&(apid, _)| apid == pid)
                                                {
                                                    continue;
                                                }
                                            }
                                        }
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
                        let lso = partition.last_stable_offset();
                        let mut resp_part = FetchPartitionResponse {
                            partition_index: p.partition,
                            error_code: err,
                            high_watermark: hw,
                            last_stable_offset: lso,
                            records: records_bytes,
                        };
                        if let Ok(comp_str) = std::env::var("OXIDEMQ_FETCH_COMPRESSION") {
                            let codec = match comp_str.to_lowercase().as_str() {
                                "gzip" => CompressionCodec::Gzip,
                                "snappy" => CompressionCodec::Snappy,
                                "lz4" => CompressionCodec::Lz4,
                                "zstd" => CompressionCodec::Zstd,
                                _ => CompressionCodec::None,
                            };
                            let _ = resp_part.compress_with(codec);
                        }
                        part_responses.push(resp_part);
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
                let req = FindCoordinatorRequest::decode(body, header.api_version)?;
                let is_authorized = match req.key_type {
                    0 => self.authorizer.authorize(
                        principal,
                        client_host,
                        AclResourceType::Group,
                        &req.key,
                        AclOperation::Describe,
                    ),
                    1 => self.authorizer.authorize(
                        principal,
                        client_host,
                        AclResourceType::TransactionalId,
                        &req.key,
                        AclOperation::Describe,
                    ),
                    _ => true,
                };

                let (error_code, node_id, host, port) = if !is_authorized {
                    let err = match req.key_type {
                        0 => KafkaErrorCode::GroupAuthorizationFailed,
                        1 => KafkaErrorCode::TransactionalIdAuthorizationFailed,
                        _ => KafkaErrorCode::ClusterAuthorizationFailed,
                    };
                    (err, -1, String::new(), -1)
                } else {
                    (
                        KafkaErrorCode::None,
                        self.cluster_state.node_id(),
                        self.cluster_state.host().to_string(),
                        self.cluster_state.port(),
                    )
                };

                let resp = FindCoordinatorResponse {
                    throttle_time_ms: 0,
                    error_code,
                    error_message: None,
                    node_id,
                    host,
                    port,
                };
                resp.encode(&mut out, header.api_version);
            }
            ApiKey::ListOffsets => {
                let req = ListOffsetsRequest::decode(body, header.api_version)?;
                let mut topic_responses = Vec::with_capacity(req.topics.len());

                for t in req.topics {
                    let is_authorized = self.authorizer.authorize(
                        principal,
                        client_host,
                        AclResourceType::Topic,
                        &t.topic,
                        AclOperation::Describe,
                    );
                    let mut part_responses = Vec::with_capacity(t.partitions.len());
                    for p in t.partitions {
                        if !is_authorized {
                            part_responses.push(ListOffsetsPartitionResponse {
                                partition: p.partition,
                                error_code: KafkaErrorCode::TopicAuthorizationFailed,
                                timestamp: -1,
                                offset: -1,
                                leader_epoch: 0,
                            });
                            continue;
                        }

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
                            leader_epoch: 0,
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
                let is_authorized = self.authorizer.authorize(
                    principal,
                    client_host,
                    AclResourceType::Group,
                    &req.group_id,
                    AclOperation::Read,
                );
                let mut topic_responses = Vec::with_capacity(req.topics.len());

                for t in req.topics {
                    let mut part_responses = Vec::with_capacity(t.partitions.len());
                    for p in t.partitions {
                        if !is_authorized {
                            part_responses.push(OffsetCommitPartitionResponse {
                                partition: p.partition,
                                error_code: KafkaErrorCode::GroupAuthorizationFailed,
                            });
                            continue;
                        }

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
                let is_authorized = self.authorizer.authorize(
                    principal,
                    client_host,
                    AclResourceType::Group,
                    &req.group_id,
                    AclOperation::Describe,
                );
                if !is_authorized {
                    let resp = OffsetFetchResponse {
                        throttle_time_ms: 0,
                        topics: Vec::new(),
                        error_code: KafkaErrorCode::GroupAuthorizationFailed,
                    };
                    resp.encode(&mut out, header.api_version);
                } else {
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
            }
            ApiKey::Heartbeat => {
                let req = HeartbeatRequest::decode(body, header.api_version)?;
                let is_authorized = self.authorizer.authorize(
                    principal,
                    client_host,
                    AclResourceType::Group,
                    &req.group_id,
                    AclOperation::Read,
                );
                let err = if !is_authorized {
                    KafkaErrorCode::GroupAuthorizationFailed
                } else {
                    self.coordinator.handle_heartbeat(
                        &req.group_id,
                        req.generation_id,
                        &req.member_id,
                    )
                };
                let resp = HeartbeatResponse {
                    throttle_time_ms: 0,
                    error_code: err,
                };
                resp.encode(&mut out, header.api_version);
            }
            ApiKey::LeaveGroup => {
                let req = LeaveGroupRequest::decode(body, header.api_version)?;
                let is_authorized = self.authorizer.authorize(
                    principal,
                    client_host,
                    AclResourceType::Group,
                    &req.group_id,
                    AclOperation::Read,
                );
                let err = if !is_authorized {
                    KafkaErrorCode::GroupAuthorizationFailed
                } else {
                    self.coordinator
                        .handle_leave_group(&req.group_id, &req.member_id)
                };
                let resp = LeaveGroupResponse {
                    throttle_time_ms: 0,
                    error_code: err,
                };
                resp.encode(&mut out, header.api_version);
            }
            ApiKey::InitProducerId => {
                let req = InitProducerIdRequest::decode(body, header.api_version)?;
                let is_authorized = if let Some(ref txn_id) = req.transactional_id {
                    self.authorizer.authorize(
                        principal,
                        client_host,
                        AclResourceType::TransactionalId,
                        txn_id,
                        AclOperation::Write,
                    )
                } else {
                    true
                };

                let (error_code, producer_id, producer_epoch) = if !is_authorized {
                    (KafkaErrorCode::TransactionalIdAuthorizationFailed, -1, -1)
                } else {
                    match self.txn_coordinator.init_producer_id(
                        req.transactional_id.as_deref(),
                        req.transaction_timeout_ms,
                    ) {
                        Ok((pid, ep)) => (KafkaErrorCode::None, pid, ep),
                        Err(code) => (code, -1, -1),
                    }
                };

                let resp = InitProducerIdResponse {
                    throttle_time_ms: 0,
                    error_code,
                    producer_id,
                    producer_epoch,
                };
                resp.encode(&mut out, header.api_version);
            }
            ApiKey::AddPartitionsToTxn => {
                let req = AddPartitionsToTxnRequest::decode(body, header.api_version)?;
                let is_authorized = self.authorizer.authorize(
                    principal,
                    client_host,
                    AclResourceType::TransactionalId,
                    &req.transactional_id,
                    AclOperation::Write,
                );

                let overall_err = if !is_authorized {
                    KafkaErrorCode::TransactionalIdAuthorizationFailed
                } else {
                    let mut all_tps = Vec::new();
                    for t in &req.topics {
                        for &p in &t.partitions {
                            all_tps.push(TopicPartition::new(&t.name, p));
                        }
                    }

                    match self.txn_coordinator.add_partitions_to_txn(
                        &req.transactional_id,
                        req.producer_id,
                        req.producer_epoch,
                        all_tps,
                    ) {
                        Ok(()) => KafkaErrorCode::None,
                        Err(code) => code,
                    }
                };

                let mut topic_results = Vec::with_capacity(req.topics.len());
                for t in req.topics {
                    let mut part_results = Vec::with_capacity(t.partitions.len());
                    for p in t.partitions {
                        part_results.push(AddPartitionsToTxnPartitionResult {
                            partition_index: p,
                            error_code: overall_err,
                        });
                    }
                    topic_results.push(AddPartitionsToTxnTopicResult {
                        name: t.name,
                        results: part_results,
                    });
                }

                let resp = AddPartitionsToTxnResponse {
                    throttle_time_ms: 0,
                    errors: topic_results,
                };
                resp.encode(&mut out, header.api_version);
            }
            ApiKey::AddOffsetsToTxn => {
                let req = AddOffsetsToTxnRequest::decode(body, header.api_version)?;
                let is_authorized = self.authorizer.authorize(
                    principal,
                    client_host,
                    AclResourceType::TransactionalId,
                    &req.transactional_id,
                    AclOperation::Write,
                );

                let error_code = if !is_authorized {
                    KafkaErrorCode::TransactionalIdAuthorizationFailed
                } else {
                    match self.txn_coordinator.add_offsets_to_txn(
                        &req.transactional_id,
                        req.producer_id,
                        req.producer_epoch,
                        &req.group_id,
                    ) {
                        Ok(()) => KafkaErrorCode::None,
                        Err(code) => code,
                    }
                };
                let resp = AddOffsetsToTxnResponse {
                    throttle_time_ms: 0,
                    error_code,
                };
                resp.encode(&mut out, header.api_version);
            }
            ApiKey::EndTxn => {
                let req = EndTxnRequest::decode(body, header.api_version)?;
                let is_authorized = self.authorizer.authorize(
                    principal,
                    client_host,
                    AclResourceType::TransactionalId,
                    &req.transactional_id,
                    AclOperation::Write,
                );

                let error_code = if !is_authorized {
                    KafkaErrorCode::TransactionalIdAuthorizationFailed
                } else {
                    match self.txn_coordinator.end_txn(
                        &req.transactional_id,
                        req.producer_id,
                        req.producer_epoch,
                        req.committed,
                        &self.cluster_state,
                    ) {
                        Ok(()) => KafkaErrorCode::None,
                        Err(code) => code,
                    }
                };
                let resp = EndTxnResponse {
                    throttle_time_ms: 0,
                    error_code,
                };
                resp.encode(&mut out, header.api_version);
            }
            ApiKey::DescribeAcls => {
                let req = DescribeAclsRequest::decode(body, header.api_version)?;
                let is_authorized = self.authorizer.authorize(
                    principal,
                    client_host,
                    AclResourceType::Cluster,
                    "kafka-cluster",
                    AclOperation::Describe,
                );
                let (error_code, error_message, resources) = if !is_authorized {
                    (
                        KafkaErrorCode::ClusterAuthorizationFailed,
                        Some("Cluster authorization failed: Describe required on Cluster:kafka-cluster".into()),
                        Vec::new(),
                    )
                } else {
                    let res = self.authorizer.describe_acls(&req);
                    (KafkaErrorCode::None, None, res)
                };
                let resp = DescribeAclsResponse {
                    throttle_time_ms: 0,
                    error_code,
                    error_message,
                    resources,
                };
                resp.encode(&mut out, header.api_version);
            }
            ApiKey::CreateAcls => {
                let req = CreateAclsRequest::decode(body, header.api_version)?;
                let is_authorized = self.authorizer.authorize(
                    principal,
                    client_host,
                    AclResourceType::Cluster,
                    "kafka-cluster",
                    AclOperation::Alter,
                );
                let results = if !is_authorized {
                    req.creations
                        .iter()
                        .map(|_| AclCreationResult {
                            error_code: KafkaErrorCode::ClusterAuthorizationFailed,
                            error_message: Some("Cluster authorization failed: Alter required on Cluster:kafka-cluster".into()),
                        })
                        .collect()
                } else {
                    self.authorizer.create_acls(&req.creations)
                };
                let resp = CreateAclsResponse {
                    throttle_time_ms: 0,
                    results,
                };
                resp.encode(&mut out, header.api_version);
            }
            ApiKey::DeleteAcls => {
                let req = DeleteAclsRequest::decode(body, header.api_version)?;
                let is_authorized = self.authorizer.authorize(
                    principal,
                    client_host,
                    AclResourceType::Cluster,
                    "kafka-cluster",
                    AclOperation::Alter,
                );
                let filter_results = if !is_authorized {
                    req.filters
                        .iter()
                        .map(|_| DeleteAclsFilterResult {
                            error_code: KafkaErrorCode::ClusterAuthorizationFailed,
                            error_message: Some("Cluster authorization failed: Alter required on Cluster:kafka-cluster".into()),
                            matching_acls: Vec::new(),
                        })
                        .collect()
                } else {
                    self.authorizer.delete_acls(&req.filters)
                };
                let resp = DeleteAclsResponse {
                    throttle_time_ms: 0,
                    filter_results,
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
    pub fn handle_frame(&self, frame: Bytes) -> Result<BytesMut> {
        let mut session = ConnectionAuthState::Authenticated {
            principal: "ANONYMOUS".to_string(),
        };
        self.handle_connection_frame(&mut session, frame)
    }

    /// Handles a raw 4-byte length-prefixed frame within an active connection session.
    pub fn handle_connection_frame(
        &self,
        session: &mut ConnectionAuthState,
        mut frame: Bytes,
    ) -> Result<BytesMut> {
        let header = RequestHeader::decode(&mut frame)?;
        trace!(
            "Received Kafka request api_key={:?} version={} corr_id={}",
            header.api_key,
            header.api_version,
            header.correlation_id
        );

        let response_payload = self.handle_connection_request(session, &header, &mut frame)?;
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
        let mut session = ConnectionAuthState::Unauthenticated;
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

            let response_frame =
                match self.handle_connection_frame(&mut session, Bytes::from(frame_buf)) {
                    Ok(f) => f,
                    Err(e) => {
                        error!("Error handling Kafka frame: {:?}", e);
                        return Err(e);
                    }
                };
            stream.write_all(&response_frame).await?;
            stream.flush().await?;

            if matches!(session, ConnectionAuthState::Failed) {
                debug!("Closing connection following SASL authentication failure");
                break;
            }
        }

        Ok(())
    }

    /// Validates record payloads against the Schema Registry catalog if schema validation is enabled.
    pub fn validate_record_payloads(&self, records_bytes: &[u8]) -> bool {
        if records_bytes.is_empty() {
            return true;
        }

        // Direct Confluent magic byte payload (0x00 + 4-byte BE schema_id)
        if records_bytes.len() >= 5 && records_bytes[0] == 0x00 {
            return self
                .schema_registry
                .validate_magic_byte_payload(records_bytes)
                .map(|opt| opt.is_some())
                .unwrap_or(false);
        }

        // Check if it's a RecordBatch v2 (magic byte at offset 16 is 2)
        if records_bytes.len() >= 61 && records_bytes[16] == 2 {
            let mut buf = bytes::Bytes::copy_from_slice(records_bytes);
            buf.advance(61); // Advance past batch header to records

            while buf.has_remaining() {
                // Record length (varint)
                let rec_len = match oxidemq_protocol::parser::KafkaDecoder::read_varint(&mut buf) {
                    Ok(l) if l > 0 => l as usize,
                    _ => break,
                };
                if buf.remaining() < rec_len {
                    break;
                }
                let mut rec_buf = buf.copy_to_bytes(rec_len);
                // attributes (1 byte)
                if rec_buf.is_empty() {
                    break;
                }
                rec_buf.advance(1);
                // timestamp delta (varint)
                if oxidemq_protocol::parser::KafkaDecoder::read_varint(&mut rec_buf).is_err() {
                    break;
                }
                // offset delta (varint)
                if oxidemq_protocol::parser::KafkaDecoder::read_varint(&mut rec_buf).is_err() {
                    break;
                }
                // key length (varint)
                let key_len =
                    match oxidemq_protocol::parser::KafkaDecoder::read_varint(&mut rec_buf) {
                        Ok(l) => l,
                        Err(_) => break,
                    };
                if key_len > 0 {
                    if rec_buf.remaining() < key_len as usize {
                        break;
                    }
                    rec_buf.advance(key_len as usize);
                }
                // value length (varint)
                let val_len =
                    match oxidemq_protocol::parser::KafkaDecoder::read_varint(&mut rec_buf) {
                        Ok(l) => l,
                        Err(_) => break,
                    };
                if val_len > 0 {
                    if rec_buf.remaining() < val_len as usize {
                        break;
                    }
                    let val_bytes = rec_buf.copy_to_bytes(val_len as usize);
                    match self.schema_registry.validate_record(&val_bytes, true) {
                        Ok(Some(_)) => {}
                        _ => return false,
                    }
                }
            }
            return true;
        }

        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxidemq_protocol::messages::{
        AddPartitionsToTxnTopic, PartitionProduceData, TopicProduceData,
    };
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
            isolation_level: 0,
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

    #[tokio::test]
    async fn test_engine_all_requests_and_duplex_connection() {
        let engine = create_test_engine();
        assert_eq!(engine.cluster_state().node_id(), 1);
        assert_eq!(engine.coordinator().group_count(), 0);
        assert!(engine.chaos().is_enabled());

        // Metadata request
        let meta_header = RequestHeader::new(ApiKey::Metadata, 1, 301, Some("test"));
        let meta_req = MetadataRequest {
            topics: Some(vec!["orders".into()]),
            allow_auto_topic_creation: true,
        };
        let mut b = BytesMut::new();
        meta_req.encode(&mut b, 1);
        let resp = engine
            .handle_request(&meta_header, &mut b.freeze())
            .unwrap();
        assert!(!resp.is_empty());

        // ListOffsets request
        let lo_header = RequestHeader::new(ApiKey::ListOffsets, 1, 302, Some("test"));
        let lo_req = ListOffsetsRequest {
            replica_id: -1,
            isolation_level: 0,
            topics: vec![oxidemq_protocol::ListOffsetsTopic {
                topic: "orders".into(),
                partitions: vec![oxidemq_protocol::ListOffsetsPartition {
                    partition: 0,
                    current_leader_epoch: 0,
                    timestamp: -1,
                }],
            }],
        };
        let mut b = BytesMut::new();
        lo_req.encode(&mut b, 1);
        let resp = engine.handle_request(&lo_header, &mut b.freeze()).unwrap();
        assert!(!resp.is_empty());

        // FindCoordinator request
        let fc_header = RequestHeader::new(ApiKey::FindCoordinator, 1, 303, Some("test"));
        let fc_req = FindCoordinatorRequest {
            key: "test-group".into(),
            key_type: 0,
        };
        let mut b = BytesMut::new();
        fc_req.encode(&mut b, 1);
        let resp = engine.handle_request(&fc_header, &mut b.freeze()).unwrap();
        assert!(!resp.is_empty());

        // OffsetCommit request
        let oc_header = RequestHeader::new(ApiKey::OffsetCommit, 1, 304, Some("test"));
        let oc_req = OffsetCommitRequest {
            group_id: "test-group".into(),
            generation_id: 1,
            member_id: "m-1".into(),
            topics: vec![oxidemq_protocol::OffsetCommitTopic {
                topic: "orders".into(),
                partitions: vec![oxidemq_protocol::OffsetCommitPartition {
                    partition: 0,
                    committed_offset: 5,
                    metadata: None,
                }],
            }],
        };
        let mut b = BytesMut::new();
        oc_req.encode(&mut b, 1);
        let resp = engine.handle_request(&oc_header, &mut b.freeze()).unwrap();
        assert!(!resp.is_empty());

        // OffsetFetch request
        let of_header = RequestHeader::new(ApiKey::OffsetFetch, 1, 305, Some("test"));
        let of_req = OffsetFetchRequest {
            group_id: "test-group".into(),
            topics: Some(vec![oxidemq_protocol::OffsetFetchTopic {
                topic: "orders".into(),
                partitions: vec![0],
            }]),
        };
        let mut b = BytesMut::new();
        of_req.encode(&mut b, 1);
        let resp = engine.handle_request(&of_header, &mut b.freeze()).unwrap();
        assert!(!resp.is_empty());

        // Heartbeat request
        let hb_header = RequestHeader::new(ApiKey::Heartbeat, 1, 306, Some("test"));
        let hb_req = HeartbeatRequest {
            group_id: "test-group".into(),
            generation_id: 1,
            member_id: "m-1".into(),
        };
        let mut b = BytesMut::new();
        hb_req.encode(&mut b, 1);
        let resp = engine.handle_request(&hb_header, &mut b.freeze()).unwrap();
        assert!(!resp.is_empty());

        // LeaveGroup request
        let lg_header = RequestHeader::new(ApiKey::LeaveGroup, 1, 307, Some("test"));
        let lg_req = LeaveGroupRequest {
            group_id: "test-group".into(),
            member_id: "m-1".into(),
        };
        let mut b = BytesMut::new();
        lg_req.encode(&mut b, 1);
        let resp = engine.handle_request(&lg_header, &mut b.freeze()).unwrap();
        assert!(!resp.is_empty());

        // Unsupported key
        let bad_header = RequestHeader::new(ApiKey::CreateTopics, 1, 999, Some("test"));
        let mut empty = Bytes::new();
        assert!(engine.handle_request(&bad_header, &mut empty).is_err());

        // Full duplex connection test
        let (mut client, server) = tokio::io::duplex(4096);
        let engine_clone = engine.clone();
        tokio::spawn(async move {
            let _ = engine_clone.process_connection(server).await;
        });

        // Build framed ApiVersions request
        let av_hdr = RequestHeader::new(ApiKey::ApiVersions, 0, 777, Some("duplex-client"));
        let mut req_body = BytesMut::new();
        av_hdr.encode(&mut req_body);
        let mut frame = BytesMut::new();
        frame.put_i32(req_body.len() as i32);
        frame.put_slice(&req_body);

        client.write_all(&frame).await.unwrap();
        client.flush().await.unwrap();

        let resp_len = client.read_i32().await.unwrap() as usize;
        assert!(resp_len > 0);
        let mut resp_buf = vec![0u8; resp_len];
        client.read_exact(&mut resp_buf).await.unwrap();
        let mut resp_bytes = Bytes::from(resp_buf);
        let hdr = ResponseHeader::decode(&mut resp_bytes).unwrap();
        assert_eq!(hdr.correlation_id, 777);

        drop(client);
    }

    #[tokio::test]
    async fn test_transactional_pipeline_commit_and_abort() {
        use oxidemq_core::types::Record;
        use oxidemq_protocol::messages::encode_record_batch_v2;

        let wal = Arc::new(MemoryWal::new());
        let storage = Arc::new(MemoryObjectStorage::new());
        let log_cache = Arc::new(LogCache::new(1024 * 1024));
        let block_cache = Arc::new(BlockCache::new(1024 * 1024));
        let cluster_state = Arc::new(ClusterState::new(
            0,
            "127.0.0.1",
            9092,
            "test-cluster".to_string(),
            wal,
            storage,
            log_cache,
            block_cache,
        ));
        let coordinator = Arc::new(GroupCoordinator::new());
        let engine = BrokerEngine::new(cluster_state, coordinator);

        // 1. InitProducerId
        let init_hdr = RequestHeader::new(ApiKey::InitProducerId, 0, 1001, Some("txn-client"));
        let init_req = InitProducerIdRequest {
            transactional_id: Some("orders-tx".into()),
            transaction_timeout_ms: 60000,
            producer_id: -1,
            producer_epoch: -1,
        };
        let mut init_buf = BytesMut::new();
        init_req.encode(&mut init_buf, 0);
        let mut resp_bytes = engine
            .handle_request(&init_hdr, &mut init_buf.freeze())
            .unwrap()
            .freeze();
        let _ = ResponseHeader::decode(&mut resp_bytes).unwrap();
        let init_resp = InitProducerIdResponse::decode(&mut resp_bytes, 0).unwrap();
        assert_eq!(init_resp.error_code, KafkaErrorCode::None);
        let pid = init_resp.producer_id;
        let epoch = init_resp.producer_epoch;

        // 2. AddPartitionsToTxn
        let add_p_hdr = RequestHeader::new(ApiKey::AddPartitionsToTxn, 0, 1002, Some("txn-client"));
        let add_p_req = AddPartitionsToTxnRequest {
            transactional_id: "orders-tx".into(),
            producer_id: pid,
            producer_epoch: epoch,
            topics: vec![AddPartitionsToTxnTopic {
                name: "txn-orders".into(),
                partitions: vec![0],
            }],
        };
        let mut add_p_buf = BytesMut::new();
        add_p_req.encode(&mut add_p_buf, 0);
        let mut resp_bytes = engine
            .handle_request(&add_p_hdr, &mut add_p_buf.freeze())
            .unwrap()
            .freeze();
        let _ = ResponseHeader::decode(&mut resp_bytes).unwrap();
        let add_p_resp = AddPartitionsToTxnResponse::decode(&mut resp_bytes, 0).unwrap();
        assert_eq!(
            add_p_resp.errors[0].results[0].error_code,
            KafkaErrorCode::None
        );

        // 3. Produce a transactional record
        let record = Record::new(
            0,
            1000,
            Some(Bytes::from("tx-key-1")),
            Some(Bytes::from("tx-val-1")),
        );
        let mut raw_batch = BytesMut::from(
            encode_record_batch_v2(0, &[record], CompressionCodec::None)
                .unwrap()
                .as_ref(),
        );
        let orig_attr = i16::from_be_bytes(raw_batch[21..23].try_into().unwrap());
        raw_batch[21..23].copy_from_slice(&(orig_attr | 0x0010).to_be_bytes());
        raw_batch[39..47].copy_from_slice(&pid.to_be_bytes());
        raw_batch[47..49].copy_from_slice(&epoch.to_be_bytes());
        let crc = oxidemq_core::compute_crc32c(&raw_batch[21..]);
        raw_batch[17..21].copy_from_slice(&crc.to_be_bytes());

        let prod_hdr = RequestHeader::new(ApiKey::Produce, 0, 1003, Some("txn-client"));
        let prod_req = ProduceRequest {
            acks: -1,
            timeout_ms: 5000,
            topic_data: vec![TopicProduceData {
                topic: "txn-orders".into(),
                partitions: vec![PartitionProduceData {
                    partition: 0,
                    records: raw_batch.freeze(),
                }],
            }],
        };
        let mut prod_buf = BytesMut::new();
        prod_req.encode(&mut prod_buf, 0);
        let _ = engine
            .handle_request(&prod_hdr, &mut prod_buf.freeze())
            .unwrap();

        // 4. Fetch with ReadCommitted before EndTxn: should receive NOTHING (uncommitted!)
        let fetch_committed_hdr =
            RequestHeader::new(ApiKey::Fetch, 4, 1004, Some("read-committed-consumer"));
        let fetch_committed_req = FetchRequest {
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1048576,
            isolation_level: 1, // READ_COMMITTED
            topics: vec![oxidemq_protocol::FetchTopic {
                topic: "txn-orders".into(),
                partitions: vec![oxidemq_protocol::FetchPartition {
                    partition: 0,
                    fetch_offset: 0,
                    partition_max_bytes: 65536,
                }],
            }],
        };
        let mut f_buf = BytesMut::new();
        fetch_committed_req.encode(&mut f_buf, 4);
        let mut resp_bytes = engine
            .handle_request(&fetch_committed_hdr, &mut f_buf.freeze())
            .unwrap()
            .freeze();
        let _ = ResponseHeader::decode(&mut resp_bytes).unwrap();
        let fetch_resp = FetchResponse::decode(&mut resp_bytes, 4).unwrap();
        assert_eq!(fetch_resp.responses[0].partitions[0].records.len(), 0);

        // 5. Fetch with ReadUncommitted before EndTxn: should receive the message!
        let fetch_uncommitted_hdr =
            RequestHeader::new(ApiKey::Fetch, 4, 1005, Some("read-uncommitted-consumer"));
        let mut fetch_uncommitted_req = fetch_committed_req.clone();
        fetch_uncommitted_req.isolation_level = 0; // READ_UNCOMMITTED
        let mut f_buf2 = BytesMut::new();
        fetch_uncommitted_req.encode(&mut f_buf2, 4);
        let mut resp_bytes2 = engine
            .handle_request(&fetch_uncommitted_hdr, &mut f_buf2.freeze())
            .unwrap()
            .freeze();
        let _ = ResponseHeader::decode(&mut resp_bytes2).unwrap();
        let fetch_resp2 = FetchResponse::decode(&mut resp_bytes2, 4).unwrap();
        assert!(!fetch_resp2.responses[0].partitions[0].records.is_empty());

        // 6. Commit transaction via EndTxn
        let end_hdr = RequestHeader::new(ApiKey::EndTxn, 0, 1006, Some("txn-client"));
        let end_req = EndTxnRequest {
            transactional_id: "orders-tx".into(),
            producer_id: pid,
            producer_epoch: epoch,
            committed: true,
        };
        let mut end_buf = BytesMut::new();
        end_req.encode(&mut end_buf, 0);
        let mut resp_bytes = engine
            .handle_request(&end_hdr, &mut end_buf.freeze())
            .unwrap()
            .freeze();
        let _ = ResponseHeader::decode(&mut resp_bytes).unwrap();
        let end_resp = EndTxnResponse::decode(&mut resp_bytes, 0).unwrap();
        assert_eq!(end_resp.error_code, KafkaErrorCode::None);

        // 7. Fetch with ReadCommitted AFTER Commit: now returns the message!
        let mut f_buf3 = BytesMut::new();
        fetch_committed_req.encode(&mut f_buf3, 4);
        let mut resp_bytes3 = engine
            .handle_request(&fetch_committed_hdr, &mut f_buf3.freeze())
            .unwrap()
            .freeze();
        let _ = ResponseHeader::decode(&mut resp_bytes3).unwrap();
        let fetch_resp3 = FetchResponse::decode(&mut resp_bytes3, 4).unwrap();
        assert!(!fetch_resp3.responses[0].partitions[0].records.is_empty());
    }

    #[tokio::test]
    async fn test_producer_epoch_fencing() {
        let wal = Arc::new(MemoryWal::new());
        let storage = Arc::new(MemoryObjectStorage::new());
        let log_cache = Arc::new(LogCache::new(1024 * 1024));
        let block_cache = Arc::new(BlockCache::new(1024 * 1024));
        let cluster_state = Arc::new(ClusterState::new(
            0,
            "127.0.0.1",
            9092,
            "test-cluster".to_string(),
            wal,
            storage,
            log_cache,
            block_cache,
        ));
        let coordinator = Arc::new(GroupCoordinator::new());
        let engine = BrokerEngine::new(cluster_state, coordinator);

        // First producer initializes "my-tx"
        let init_hdr = RequestHeader::new(ApiKey::InitProducerId, 0, 1, Some("p1"));
        let init_req = InitProducerIdRequest {
            transactional_id: Some("my-tx".into()),
            transaction_timeout_ms: 60000,
            producer_id: -1,
            producer_epoch: -1,
        };
        let mut buf = BytesMut::new();
        init_req.encode(&mut buf, 0);
        let mut resp_bytes = engine
            .handle_request(&init_hdr, &mut buf.freeze())
            .unwrap()
            .freeze();
        let _ = ResponseHeader::decode(&mut resp_bytes).unwrap();
        let p1_resp = InitProducerIdResponse::decode(&mut resp_bytes, 0).unwrap();
        assert_eq!(p1_resp.producer_epoch, 0);

        // Second producer (or rebooted producer) initializes "my-tx" -> epoch bumped to 1
        let mut buf2 = BytesMut::new();
        init_req.encode(&mut buf2, 0);
        let mut resp_bytes2 = engine
            .handle_request(&init_hdr, &mut buf2.freeze())
            .unwrap()
            .freeze();
        let _ = ResponseHeader::decode(&mut resp_bytes2).unwrap();
        let p2_resp = InitProducerIdResponse::decode(&mut resp_bytes2, 0).unwrap();
        assert_eq!(p2_resp.producer_epoch, 1);

        // Old producer with epoch 0 attempts AddPartitionsToTxn -> Must be FENCED!
        let add_p_hdr = RequestHeader::new(ApiKey::AddPartitionsToTxn, 0, 2, Some("p1"));
        let add_p_req = AddPartitionsToTxnRequest {
            transactional_id: "my-tx".into(),
            producer_id: p1_resp.producer_id,
            producer_epoch: 0,
            topics: vec![AddPartitionsToTxnTopic {
                name: "topic-fence".into(),
                partitions: vec![0],
            }],
        };
        let mut add_buf = BytesMut::new();
        add_p_req.encode(&mut add_buf, 0);
        let mut resp_bytes = engine
            .handle_request(&add_p_hdr, &mut add_buf.freeze())
            .unwrap()
            .freeze();
        let _ = ResponseHeader::decode(&mut resp_bytes).unwrap();
        let add_resp = AddPartitionsToTxnResponse::decode(&mut resp_bytes, 0).unwrap();
        assert_eq!(
            add_resp.errors[0].results[0].error_code,
            KafkaErrorCode::ProducerFenced
        );
    }

    #[tokio::test]
    async fn test_produce_schema_validation() {
        let registry = Arc::new(SchemaRegistry::new());
        let schema_id = registry
            .register_schema(
                "test-schema-topic-value",
                r#"{"type":"record","name":"User","fields":[]}"#,
                None,
                vec![],
            )
            .unwrap();

        let engine = create_test_engine()
            .with_schema_registry(registry)
            .with_schema_validation(true);

        // 1. Produce with valid Confluent magic byte payload -> should succeed
        let mut valid_payload = Vec::new();
        valid_payload.push(0x00);
        valid_payload.extend_from_slice(&schema_id.to_be_bytes());
        valid_payload.extend_from_slice(b"payload content");

        let prod_hdr = RequestHeader::new(ApiKey::Produce, 0, 999, Some("schema-client"));
        let prod_req = ProduceRequest {
            acks: 1,
            timeout_ms: 1000,
            topic_data: vec![TopicProduceData {
                topic: "test-schema-topic".into(),
                partitions: vec![PartitionProduceData {
                    partition: 0,
                    records: Bytes::from(valid_payload),
                }],
            }],
        };
        let mut buf = BytesMut::new();
        prod_req.encode(&mut buf, 0);
        let mut resp_bytes = engine
            .handle_request(&prod_hdr, &mut buf.freeze())
            .unwrap()
            .freeze();
        let _ = ResponseHeader::decode(&mut resp_bytes).unwrap();
        let resp = ProduceResponse::decode(&mut resp_bytes, 0).unwrap();
        assert_eq!(
            resp.responses[0].partitions[0].error_code,
            KafkaErrorCode::None
        );

        // 2. Produce with invalid schema ID -> should return InvalidRecord
        let mut invalid_payload = Vec::new();
        invalid_payload.push(0x00);
        invalid_payload.extend_from_slice(&9999_i32.to_be_bytes());
        invalid_payload.extend_from_slice(b"payload content");

        let prod_req_invalid = ProduceRequest {
            acks: 1,
            timeout_ms: 1000,
            topic_data: vec![TopicProduceData {
                topic: "test-schema-topic".into(),
                partitions: vec![PartitionProduceData {
                    partition: 0,
                    records: Bytes::from(invalid_payload),
                }],
            }],
        };
        let mut buf_inv = BytesMut::new();
        prod_req_invalid.encode(&mut buf_inv, 0);
        let mut resp_bytes_inv = engine
            .handle_request(&prod_hdr, &mut buf_inv.freeze())
            .unwrap()
            .freeze();
        let _ = ResponseHeader::decode(&mut resp_bytes_inv).unwrap();
        let resp_inv = ProduceResponse::decode(&mut resp_bytes_inv, 0).unwrap();
        assert_eq!(
            resp_inv.responses[0].partitions[0].error_code,
            KafkaErrorCode::InvalidRecord
        );
    }

    #[test]
    fn test_sasl_plain_connection_flow() {
        let wal = Arc::new(MemoryWal::new());
        let storage = Arc::new(MemoryObjectStorage::new());
        let log_cache = Arc::new(LogCache::new(1024 * 1024));
        let block_cache = Arc::new(BlockCache::new(1024 * 1024));
        let cs = Arc::new(ClusterState::new(
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
        let auth = Arc::new(SaslAuthenticator::new(
            vec![SaslMechanism::Plain, SaslMechanism::ScramSha256],
            true, // require SASL
        ));
        auth.add_user("admin", "admin-secret");

        let engine = BrokerEngine::new(cs, coord).with_authenticator(auth);
        let mut session = ConnectionAuthState::Unauthenticated;

        // 1. ApiVersions is allowed before authentication
        let api_ver_hdr = RequestHeader::new(ApiKey::ApiVersions, 0, 1, Some("client"));
        let empty_req = BytesMut::new();
        let api_ver_resp = engine
            .handle_connection_request(&mut session, &api_ver_hdr, &mut empty_req.freeze())
            .unwrap();
        assert!(!api_ver_resp.is_empty());

        // 2. Metadata without authentication should be rejected
        let meta_hdr = RequestHeader::new(ApiKey::Metadata, 0, 2, Some("client"));
        let mut meta_body = BytesMut::new();
        let meta_req = MetadataRequest {
            topics: None,
            allow_auto_topic_creation: true,
        };
        meta_req.encode(&mut meta_body, 0);
        assert!(engine
            .handle_connection_request(&mut session, &meta_hdr, &mut meta_body.freeze())
            .is_err());

        // 3. SaslHandshake with PLAIN
        let hs_hdr = RequestHeader::new(ApiKey::SaslHandshake, 0, 3, Some("client"));
        let mut hs_body = BytesMut::new();
        let hs_req = SaslHandshakeRequest {
            mechanism: "PLAIN".into(),
        };
        hs_req.encode(&mut hs_body, 0);
        let mut hs_resp_bytes = engine
            .handle_connection_request(&mut session, &hs_hdr, &mut hs_body.freeze())
            .unwrap()
            .freeze();
        let _ = ResponseHeader::decode(&mut hs_resp_bytes).unwrap();
        let hs_resp = SaslHandshakeResponse::decode(&mut hs_resp_bytes, 0).unwrap();
        assert_eq!(hs_resp.error_code, KafkaErrorCode::None);
        assert!(hs_resp.enabled_mechanisms.contains(&"PLAIN".to_string()));
        assert!(matches!(
            session,
            ConnectionAuthState::HandshakeReceived {
                mechanism: SaslMechanism::Plain
            }
        ));

        // 4. SaslAuthenticate with PLAIN (valid credentials)
        let auth_hdr = RequestHeader::new(ApiKey::SaslAuthenticate, 0, 4, Some("client"));
        let mut auth_body = BytesMut::new();
        let auth_req = SaslAuthenticateRequest {
            auth_bytes: Bytes::from_static(b"\0admin\0admin-secret"),
        };
        auth_req.encode(&mut auth_body, 0);
        let mut auth_resp_bytes = engine
            .handle_connection_request(&mut session, &auth_hdr, &mut auth_body.freeze())
            .unwrap()
            .freeze();
        let _ = ResponseHeader::decode(&mut auth_resp_bytes).unwrap();
        let auth_resp = SaslAuthenticateResponse::decode(&mut auth_resp_bytes, 0).unwrap();
        assert_eq!(auth_resp.error_code, KafkaErrorCode::None);
        assert!(session.is_authenticated());
        assert_eq!(session.principal(), Some("admin"));

        // 5. Metadata now succeeds because connection is authenticated
        let mut meta_body2 = BytesMut::new();
        meta_req.encode(&mut meta_body2, 0);
        let meta_resp = engine
            .handle_connection_request(&mut session, &meta_hdr, &mut meta_body2.freeze())
            .unwrap();
        assert!(!meta_resp.is_empty());
    }

    #[test]
    fn test_sasl_scram_sha256_connection_flow() {
        use base64::prelude::*;
        use ring::digest;
        use ring::hmac;
        use ring::pbkdf2;
        use std::num::NonZeroU32;

        let wal = Arc::new(MemoryWal::new());
        let storage = Arc::new(MemoryObjectStorage::new());
        let log_cache = Arc::new(LogCache::new(1024 * 1024));
        let block_cache = Arc::new(BlockCache::new(1024 * 1024));
        let cs = Arc::new(ClusterState::new(
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
        let auth = Arc::new(SaslAuthenticator::new(
            vec![SaslMechanism::ScramSha256],
            false,
        ));
        auth.add_user("scram-user", "topsecret");

        let engine = BrokerEngine::new(cs, coord).with_authenticator(auth);
        let mut session = ConnectionAuthState::Unauthenticated;

        // 1. SaslHandshake with SCRAM-SHA-256
        let hs_hdr = RequestHeader::new(ApiKey::SaslHandshake, 0, 1, Some("client"));
        let mut hs_body = BytesMut::new();
        let hs_req = SaslHandshakeRequest {
            mechanism: "SCRAM-SHA-256".into(),
        };
        hs_req.encode(&mut hs_body, 0);
        let _ = engine
            .handle_connection_request(&mut session, &hs_hdr, &mut hs_body.freeze())
            .unwrap();
        assert!(matches!(
            session,
            ConnectionAuthState::HandshakeReceived {
                mechanism: SaslMechanism::ScramSha256
            }
        ));

        // 2. SaslAuthenticate Round 1 (client-first-message)
        let client_nonce = "clientNonce12345";
        let client_first = format!("n,,n=scram-user,r={}", client_nonce);
        let auth_hdr1 = RequestHeader::new(ApiKey::SaslAuthenticate, 0, 2, Some("client"));
        let mut auth_body1 = BytesMut::new();
        let auth_req1 = SaslAuthenticateRequest {
            auth_bytes: Bytes::from(client_first),
        };
        auth_req1.encode(&mut auth_body1, 0);
        let mut resp1_bytes = engine
            .handle_connection_request(&mut session, &auth_hdr1, &mut auth_body1.freeze())
            .unwrap()
            .freeze();
        let _ = ResponseHeader::decode(&mut resp1_bytes).unwrap();
        let auth_resp1 = SaslAuthenticateResponse::decode(&mut resp1_bytes, 0).unwrap();
        assert_eq!(auth_resp1.error_code, KafkaErrorCode::None);

        let server_first_str = std::str::from_utf8(&auth_resp1.auth_bytes).unwrap();
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

        // 3. Client computes proof
        let client_final_without_proof = format!("c=biws,r={}", full_nonce);
        let auth_message = format!(
            "n=scram-user,r={},{},{}",
            client_nonce, server_first_str, client_final_without_proof
        );

        let mut salted_password = [0u8; 32];
        pbkdf2::derive(
            pbkdf2::PBKDF2_HMAC_SHA256,
            NonZeroU32::new(4096).unwrap(),
            &salt,
            b"topsecret",
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

        // 4. SaslAuthenticate Round 2 (client-final-message)
        let auth_hdr2 = RequestHeader::new(ApiKey::SaslAuthenticate, 0, 3, Some("client"));
        let mut auth_body2 = BytesMut::new();
        let auth_req2 = SaslAuthenticateRequest {
            auth_bytes: Bytes::from(client_final),
        };
        auth_req2.encode(&mut auth_body2, 0);
        let mut resp2_bytes = engine
            .handle_connection_request(&mut session, &auth_hdr2, &mut auth_body2.freeze())
            .unwrap()
            .freeze();
        let _ = ResponseHeader::decode(&mut resp2_bytes).unwrap();
        let auth_resp2 = SaslAuthenticateResponse::decode(&mut resp2_bytes, 0).unwrap();
        assert_eq!(auth_resp2.error_code, KafkaErrorCode::None);
        assert!(session.is_authenticated());
        assert_eq!(session.principal(), Some("scram-user"));
        let server_final_str = std::str::from_utf8(&auth_resp2.auth_bytes).unwrap();
        assert!(server_final_str.starts_with("v="));
    }

    #[test]
    fn test_engine_acl_authorization_flow() {
        let mut super_users = std::collections::HashSet::new();
        super_users.insert("User:admin".to_string());
        let authorizer = Arc::new(AclAuthorizer::new(true, super_users, false));

        let engine = create_test_engine().with_authorizer(authorizer);

        let mut alice_session = ConnectionAuthState::Authenticated {
            principal: "alice".to_string(),
        };
        let mut admin_session = ConnectionAuthState::Authenticated {
            principal: "admin".to_string(),
        };
        let mut bob_session = ConnectionAuthState::Authenticated {
            principal: "bob".to_string(),
        };

        // 1. Alice attempts to produce to "secure-topic" -> rejected (TopicAuthorizationFailed)
        let produce_req = ProduceRequest {
            acks: 1,
            timeout_ms: 1000,
            topic_data: vec![oxidemq_protocol::TopicProduceData {
                topic: "secure-topic".to_string(),
                partitions: vec![oxidemq_protocol::PartitionProduceData {
                    partition: 0,
                    records: Bytes::from_static(b"secret payload"),
                }],
            }],
        };
        let produce_hdr = RequestHeader::new(ApiKey::Produce, 0, 10, Some("alice"));
        let mut p_buf = BytesMut::new();
        produce_req.encode(&mut p_buf, 0);
        let mut p_resp = engine
            .handle_connection_request(&mut alice_session, &produce_hdr, &mut p_buf.freeze())
            .unwrap()
            .freeze();
        let _ = ResponseHeader::decode(&mut p_resp).unwrap();
        let produce_resp = ProduceResponse::decode(&mut p_resp, 0).unwrap();
        assert_eq!(
            produce_resp.responses[0].partitions[0].error_code,
            KafkaErrorCode::TopicAuthorizationFailed
        );

        // 2. Bob attempts to call CreateAcls -> rejected (ClusterAuthorizationFailed)
        let create_acls_req = CreateAclsRequest {
            creations: vec![oxidemq_protocol::AclCreation {
                resource_type: AclResourceType::Topic as i8,
                resource_name: "secure-topic".to_string(),
                resource_pattern_type: oxidemq_protocol::AclResourcePatternType::Literal as i8,
                principal: "User:alice".to_string(),
                host: "*".to_string(),
                operation: AclOperation::Write as i8,
                permission_type: oxidemq_protocol::AclPermissionType::Allow as i8,
            }],
        };
        let create_hdr = RequestHeader::new(ApiKey::CreateAcls, 0, 11, Some("bob"));
        let mut c_buf = BytesMut::new();
        create_acls_req.encode(&mut c_buf, 0);
        let mut c_resp = engine
            .handle_connection_request(&mut bob_session, &create_hdr, &mut c_buf.freeze())
            .unwrap()
            .freeze();
        let _ = ResponseHeader::decode(&mut c_resp).unwrap();
        let create_resp = CreateAclsResponse::decode(&mut c_resp, 0).unwrap();
        assert_eq!(
            create_resp.results[0].error_code,
            KafkaErrorCode::ClusterAuthorizationFailed
        );

        // 3. Admin calls CreateAcls granting Write to Alice on "secure-topic" -> succeeds
        let mut c_buf_admin = BytesMut::new();
        create_acls_req.encode(&mut c_buf_admin, 0);
        let mut c_resp_admin = engine
            .handle_connection_request(&mut admin_session, &create_hdr, &mut c_buf_admin.freeze())
            .unwrap()
            .freeze();
        let _ = ResponseHeader::decode(&mut c_resp_admin).unwrap();
        let create_resp_admin = CreateAclsResponse::decode(&mut c_resp_admin, 0).unwrap();
        assert_eq!(
            create_resp_admin.results[0].error_code,
            KafkaErrorCode::None
        );

        // 4. Alice produces again -> succeeds!
        let mut p_buf2 = BytesMut::new();
        produce_req.encode(&mut p_buf2, 0);
        let mut p_resp2 = engine
            .handle_connection_request(&mut alice_session, &produce_hdr, &mut p_buf2.freeze())
            .unwrap()
            .freeze();
        let _ = ResponseHeader::decode(&mut p_resp2).unwrap();
        let produce_resp2 = ProduceResponse::decode(&mut p_resp2, 0).unwrap();
        assert_eq!(
            produce_resp2.responses[0].partitions[0].error_code,
            KafkaErrorCode::None
        );

        // 5. Alice attempts to Fetch -> fails with TopicAuthorizationFailed (she has Write, not Read)
        let fetch_req = FetchRequest {
            max_wait_ms: 100,
            min_bytes: 1,
            max_bytes: 1024,
            isolation_level: 0,
            topics: vec![oxidemq_protocol::FetchTopic {
                topic: "secure-topic".to_string(),
                partitions: vec![oxidemq_protocol::FetchPartition {
                    partition: 0,
                    fetch_offset: 0,
                    partition_max_bytes: 1024,
                }],
            }],
        };
        let fetch_hdr = RequestHeader::new(ApiKey::Fetch, 0, 12, Some("alice"));
        let mut f_buf = BytesMut::new();
        fetch_req.encode(&mut f_buf, 0);
        let mut f_resp = engine
            .handle_connection_request(&mut alice_session, &fetch_hdr, &mut f_buf.freeze())
            .unwrap()
            .freeze();
        let _ = ResponseHeader::decode(&mut f_resp).unwrap();
        let fetch_resp = FetchResponse::decode(&mut f_resp, 0).unwrap();
        assert_eq!(
            fetch_resp.responses[0].partitions[0].error_code,
            KafkaErrorCode::TopicAuthorizationFailed
        );

        // 6. Admin calls DescribeAcls -> returns Alice's ACL
        let describe_req = DescribeAclsRequest {
            resource_type_filter: 1, // Any
            resource_name_filter: None,
            resource_pattern_type_filter: 1, // Any
            principal_filter: None,
            host_filter: None,
            operation: 1,       // Any
            permission_type: 1, // Any
        };
        let desc_hdr = RequestHeader::new(ApiKey::DescribeAcls, 0, 13, Some("admin"));
        let mut d_buf = BytesMut::new();
        describe_req.encode(&mut d_buf, 0);
        let mut d_resp = engine
            .handle_connection_request(&mut admin_session, &desc_hdr, &mut d_buf.freeze())
            .unwrap()
            .freeze();
        let _ = ResponseHeader::decode(&mut d_resp).unwrap();
        let desc_resp = DescribeAclsResponse::decode(&mut d_resp, 0).unwrap();
        assert_eq!(desc_resp.error_code, KafkaErrorCode::None);
        assert_eq!(desc_resp.resources.len(), 1);
        assert_eq!(desc_resp.resources[0].resource_name, "secure-topic");

        // 7. Admin calls DeleteAcls -> deletes Alice's ACL
        let delete_req = DeleteAclsRequest {
            filters: vec![oxidemq_protocol::DeleteAclsFilter {
                resource_type_filter: 1,
                resource_name_filter: Some("secure-topic".to_string()),
                resource_pattern_type_filter: 1,
                principal_filter: None,
                host_filter: None,
                operation: 1,
                permission_type: 1,
            }],
        };
        let del_hdr = RequestHeader::new(ApiKey::DeleteAcls, 0, 14, Some("admin"));
        let mut del_buf = BytesMut::new();
        delete_req.encode(&mut del_buf, 0);
        let mut del_resp = engine
            .handle_connection_request(&mut admin_session, &del_hdr, &mut del_buf.freeze())
            .unwrap()
            .freeze();
        let _ = ResponseHeader::decode(&mut del_resp).unwrap();
        let del_resp_decoded = DeleteAclsResponse::decode(&mut del_resp, 0).unwrap();
        assert_eq!(del_resp_decoded.filter_results.len(), 1);
        assert_eq!(
            del_resp_decoded.filter_results[0].error_code,
            KafkaErrorCode::None
        );
        assert_eq!(del_resp_decoded.filter_results[0].matching_acls.len(), 1);

        // 8. Alice produces again -> fails (ACL was deleted)
        let mut p_buf3 = BytesMut::new();
        produce_req.encode(&mut p_buf3, 0);
        let mut p_resp3 = engine
            .handle_connection_request(&mut alice_session, &produce_hdr, &mut p_buf3.freeze())
            .unwrap()
            .freeze();
        let _ = ResponseHeader::decode(&mut p_resp3).unwrap();
        let produce_resp3 = ProduceResponse::decode(&mut p_resp3, 0).unwrap();
        assert_eq!(
            produce_resp3.responses[0].partitions[0].error_code,
            KafkaErrorCode::TopicAuthorizationFailed
        );
    }
}
