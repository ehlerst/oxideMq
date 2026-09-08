//! # oxideMq WAL (Write-Ahead Log)
//!
//! Ultra-low latency, append-only durability engine featuring group commit,
//! direct I/O, and crash-recovery semantics.

use oxidemq_core::Result;

/// A trait defining Write-Ahead Log operations.
pub trait WalEngine: Send + Sync {
    fn append(&self, stream_id: u64, offset: i64, data: &[u8]) -> Result<u64>;
    fn flush(&self) -> Result<()>;
    fn trim(&self, stream_id: u64, up_to_offset: i64) -> Result<()>;
}
