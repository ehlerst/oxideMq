//! # oxideMq S3Stream Engine
//!
//! Cloud-native streaming storage engine decoupling compute and storage by offloading
//! stream records to S3 with multi-tiered in-memory (Tier 1 LogCache) and LRU (Tier 2 BlockCache) caches.

pub mod block_cache;
pub mod client;
pub mod compactor;
pub mod format;
pub mod log_cache;
pub mod stream;
pub mod uploader;

pub use block_cache::{BlockCache, BlockKey};
pub use client::{MemoryObjectStorage, ObjectStorage};
pub use compactor::StreamCompactor;
pub use format::{S3BlockIndex, S3DataBlock, S3ObjectCodec};
pub use log_cache::LogCache;
pub use stream::{S3ObjectMeta, S3Stream};
pub use uploader::BatchUploader;

use oxidemq_core::Result;

/// A trait defining stream operations over S3.
pub trait S3StreamStorage: Send + Sync {
    fn stream_id(&self) -> u64;
    fn start_offset(&self) -> i64;
    fn next_offset(&self) -> i64;
    fn trim(&self, new_start_offset: i64) -> Result<()>;
}
