use thiserror::Error;

#[derive(Error, Debug)]
pub enum OxideMqError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Record corrupted: expected CRC 0x{expected_crc:08X}, found 0x{actual_crc:08X}")]
    CorruptedRecord { expected_crc: u32, actual_crc: u32 },

    #[error("Invalid offset: requested {requested}, valid range [{min}, {max}]")]
    InvalidOffset { requested: i64, min: i64, max: i64 },

    #[error("Stream not found: ID {0}")]
    StreamNotFound(u64),

    #[error("Topic not found: {0}")]
    TopicNotFound(String),

    #[error("Partition not found: topic '{topic}', partition {partition}")]
    PartitionNotFound { topic: String, partition: i32 },

    #[error("Storage engine error: {0}")]
    Storage(String),

    #[error("WAL error: {0}")]
    Wal(String),

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("Configuration error: {0}")]
    Configuration(String),

    #[error("Chaos injected failure: {message}")]
    ChaosInjected { message: String },

    #[error("Internal error: {0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, OxideMqError>;
