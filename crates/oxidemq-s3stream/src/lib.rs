//! # oxideMq S3Stream Engine
//!
//! Cloud-native streaming storage engine decoupling compute and storage by offloading
//! stream records to S3 with multi-tiered in-memory and LRU caches.

use oxidemq_core::Result;

/// A trait defining stream operations over S3.
pub trait S3StreamStorage: Send + Sync {
    fn stream_id(&self) -> u64;
    fn start_offset(&self) -> i64;
    fn next_offset(&self) -> i64;
    fn trim(&self, new_start_offset: i64) -> Result<()>;
}
