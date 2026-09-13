/// Standard Apache Kafka protocol error codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(i16)]
pub enum KafkaErrorCode {
    #[default]
    None = 0,
    OffsetOutOfRange = 1,
    CorruptMessage = 2,
    UnknownTopicOrPartition = 3,
    InvalidFetchSize = 4,
    LeaderNotAvailable = 5,
    NotLeaderOrFollower = 6,
    RequestTimedOut = 7,
    BrokerNotAvailable = 8,
    ReplicaNotAvailable = 9,
    MessageTooLarge = 10,
    StaleControllerEpoch = 11,
    OffsetMetadataTooLarge = 12,
    NetworkException = 13,
    CoordinatorLoadInProgress = 14,
    CoordinatorNotAvailable = 15,
    NotCoordinator = 16,
    InvalidTopicException = 17,
    RecordListTooLarge = 18,
    NotEnoughReplicas = 19,
    NotEnoughReplicasAfterAppend = 20,
    InvalidRequiredAcks = 21,
    IllegalGeneration = 22,
    InconsistentGroupProtocol = 23,
    InvalidGroupId = 24,
    UnknownMemberId = 25,
    InvalidSessionTimeout = 26,
    RebalanceInProgress = 27,
    InvalidCommitOffsetSize = 28,
    TopicAuthorizationFailed = 29,
    GroupAuthorizationFailed = 30,
    ClusterAuthorizationFailed = 31,
    InvalidTimestamp = 32,
    UnsupportedSaslMechanism = 33,
    IllegalSaslState = 34,
    UnsupportedVersion = 35,
    TopicAlreadyExists = 36,
    InvalidPartitions = 37,
    InvalidReplicationFactor = 38,
    InvalidProducerEpoch = 47,
    InvalidTxnState = 48,
    InvalidProducerIdMapping = 49,
    ConcurrentTransactions = 51,
    ProducerFenced = 52,
    TransactionalIdAuthorizationFailed = 53,
    InvalidRecord = 87,
    UnknownServer = -1,
}

impl KafkaErrorCode {
    pub fn code(self) -> i16 {
        self as i16
    }

    pub fn from_i16(val: i16) -> Self {
        match val {
            0 => Self::None,
            1 => Self::OffsetOutOfRange,
            2 => Self::CorruptMessage,
            3 => Self::UnknownTopicOrPartition,
            4 => Self::InvalidFetchSize,
            5 => Self::LeaderNotAvailable,
            6 => Self::NotLeaderOrFollower,
            7 => Self::RequestTimedOut,
            8 => Self::BrokerNotAvailable,
            9 => Self::ReplicaNotAvailable,
            10 => Self::MessageTooLarge,
            11 => Self::StaleControllerEpoch,
            12 => Self::OffsetMetadataTooLarge,
            13 => Self::NetworkException,
            14 => Self::CoordinatorLoadInProgress,
            15 => Self::CoordinatorNotAvailable,
            16 => Self::NotCoordinator,
            17 => Self::InvalidTopicException,
            18 => Self::RecordListTooLarge,
            19 => Self::NotEnoughReplicas,
            20 => Self::NotEnoughReplicasAfterAppend,
            21 => Self::InvalidRequiredAcks,
            22 => Self::IllegalGeneration,
            23 => Self::InconsistentGroupProtocol,
            24 => Self::InvalidGroupId,
            25 => Self::UnknownMemberId,
            26 => Self::InvalidSessionTimeout,
            27 => Self::RebalanceInProgress,
            28 => Self::InvalidCommitOffsetSize,
            29 => Self::TopicAuthorizationFailed,
            30 => Self::GroupAuthorizationFailed,
            31 => Self::ClusterAuthorizationFailed,
            32 => Self::InvalidTimestamp,
            33 => Self::UnsupportedSaslMechanism,
            34 => Self::IllegalSaslState,
            35 => Self::UnsupportedVersion,
            36 => Self::TopicAlreadyExists,
            37 => Self::InvalidPartitions,
            38 => Self::InvalidReplicationFactor,
            47 => Self::InvalidProducerEpoch,
            48 => Self::InvalidTxnState,
            49 => Self::InvalidProducerIdMapping,
            51 => Self::ConcurrentTransactions,
            52 => Self::ProducerFenced,
            53 => Self::TransactionalIdAuthorizationFailed,
            87 => Self::InvalidRecord,
            _ => Self::UnknownServer,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kafka_error_codes() {
        let codes = [
            (KafkaErrorCode::None, 0),
            (KafkaErrorCode::OffsetOutOfRange, 1),
            (KafkaErrorCode::CorruptMessage, 2),
            (KafkaErrorCode::UnknownTopicOrPartition, 3),
            (KafkaErrorCode::InvalidFetchSize, 4),
            (KafkaErrorCode::LeaderNotAvailable, 5),
            (KafkaErrorCode::NotLeaderOrFollower, 6),
            (KafkaErrorCode::RequestTimedOut, 7),
            (KafkaErrorCode::BrokerNotAvailable, 8),
            (KafkaErrorCode::ReplicaNotAvailable, 9),
            (KafkaErrorCode::MessageTooLarge, 10),
            (KafkaErrorCode::StaleControllerEpoch, 11),
            (KafkaErrorCode::OffsetMetadataTooLarge, 12),
            (KafkaErrorCode::NetworkException, 13),
            (KafkaErrorCode::CoordinatorLoadInProgress, 14),
            (KafkaErrorCode::CoordinatorNotAvailable, 15),
            (KafkaErrorCode::NotCoordinator, 16),
            (KafkaErrorCode::InvalidTopicException, 17),
            (KafkaErrorCode::RecordListTooLarge, 18),
            (KafkaErrorCode::NotEnoughReplicas, 19),
            (KafkaErrorCode::NotEnoughReplicasAfterAppend, 20),
            (KafkaErrorCode::InvalidRequiredAcks, 21),
            (KafkaErrorCode::IllegalGeneration, 22),
            (KafkaErrorCode::InconsistentGroupProtocol, 23),
            (KafkaErrorCode::InvalidGroupId, 24),
            (KafkaErrorCode::UnknownMemberId, 25),
            (KafkaErrorCode::InvalidSessionTimeout, 26),
            (KafkaErrorCode::RebalanceInProgress, 27),
            (KafkaErrorCode::InvalidCommitOffsetSize, 28),
            (KafkaErrorCode::TopicAuthorizationFailed, 29),
            (KafkaErrorCode::GroupAuthorizationFailed, 30),
            (KafkaErrorCode::ClusterAuthorizationFailed, 31),
            (KafkaErrorCode::InvalidTimestamp, 32),
            (KafkaErrorCode::UnsupportedSaslMechanism, 33),
            (KafkaErrorCode::IllegalSaslState, 34),
            (KafkaErrorCode::UnsupportedVersion, 35),
            (KafkaErrorCode::TopicAlreadyExists, 36),
            (KafkaErrorCode::InvalidPartitions, 37),
            (KafkaErrorCode::InvalidReplicationFactor, 38),
            (KafkaErrorCode::InvalidProducerEpoch, 47),
            (KafkaErrorCode::InvalidTxnState, 48),
            (KafkaErrorCode::InvalidProducerIdMapping, 49),
            (KafkaErrorCode::ConcurrentTransactions, 51),
            (KafkaErrorCode::ProducerFenced, 52),
            (KafkaErrorCode::TransactionalIdAuthorizationFailed, 53),
            (KafkaErrorCode::InvalidRecord, 87),
            (KafkaErrorCode::UnknownServer, -1),
        ];

        for (err, val) in codes {
            assert_eq!(err.code(), val);
            assert_eq!(KafkaErrorCode::from_i16(val), err);
        }

        // Test unknown code fallback
        assert_eq!(KafkaErrorCode::from_i16(999), KafkaErrorCode::UnknownServer);
        assert_eq!(KafkaErrorCode::default(), KafkaErrorCode::None);
    }
}
