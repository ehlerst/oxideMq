//! # oxideMq WAL (Write-Ahead Log)
//!
//! Ultra-low latency, append-only durability engine featuring group commit,
//! crash-recovery, segment rotation, and pluggable storage backends.

pub mod file;
pub mod frame;
pub mod memory;
pub mod recovery;
pub mod segment;

pub use file::FileWal;
pub use frame::{WalRecord, HEADER_SIZE, WAL_MAGIC};
pub use memory::MemoryWal;
pub use recovery::{RecoveryReport, StreamWatermark, WalRecovery};
pub use segment::{WalSegment, WAL_FILE_EXTENSION};

use oxidemq_core::error::Result;

/// A trait defining Write-Ahead Log operations.
pub trait WalEngine: Send + Sync {
    /// Appends a payload for a stream at a given logical offset, returning the assigned monotonic sequence number.
    fn append(&self, stream_id: u64, offset: i64, data: &[u8]) -> Result<u64>;

    /// Flushes all pending writes to durable storage.
    fn flush(&self) -> Result<()>;

    /// Trims stream records that have been confirmed uploaded to object storage up to `up_to_offset`.
    fn trim(&self, stream_id: u64, up_to_offset: i64) -> Result<()>;
}
