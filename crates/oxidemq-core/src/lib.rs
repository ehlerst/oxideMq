pub mod bytes_util;
pub mod config;
pub mod error;
pub mod types;

pub use bytes_util::{compute_crc32c, AlignedBuffer, ByteAccumulator};
pub use config::{BrokerConfig, CacheConfig, OxideConfig, S3Config, WalConfig};
pub use error::{OxideMqError, Result};
pub use types::{CompressionCodec, Record, RecordBatch, RecordHeader, StreamId, TopicPartition};

/// Prelude re-exporting common types for ease of use.
pub mod prelude {
    pub use crate::config::OxideConfig;
    pub use crate::error::{OxideMqError, Result};
    pub use crate::types::{Record, RecordBatch, RecordHeader, StreamId, TopicPartition};
}
