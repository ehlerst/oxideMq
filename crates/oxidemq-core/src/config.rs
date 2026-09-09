use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Root configuration for an oxideMq node.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OxideConfig {
    pub broker: BrokerConfig,
    pub wal: WalConfig,
    pub s3: S3Config,
    pub cache: CacheConfig,
}

/// Broker networking and cluster parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerConfig {
    pub node_id: i32,
    pub cluster_id: String,
    pub host: String,
    pub kafka_port: u16,
    pub admin_port: u16,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            node_id: 1,
            cluster_id: "oxidemq-cluster-local".to_string(),
            host: "0.0.0.0".to_string(),
            kafka_port: 9092,
            admin_port: 8082,
        }
    }
}

/// Write-Ahead Log (WAL) engine tuning.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalConfig {
    pub dir: PathBuf,
    pub max_segment_size_bytes: usize,
    pub group_commit_window_micros: u64,
    pub max_batch_records: usize,
    pub sync_to_disk: bool,
    pub direct_io: bool,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("./data/wal"),
            max_segment_size_bytes: 64 * 1024 * 1024, // 64 MB
            group_commit_window_micros: 500,          // 500 microseconds
            max_batch_records: 4096,
            sync_to_disk: true,
            direct_io: false,
        }
    }
}

/// Cloud Object Storage (S3 / RustStack S3) configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Config {
    pub endpoint_url: Option<String>,
    pub bucket: String,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub force_path_style: bool,
    pub max_object_size_bytes: usize,
    pub compactor_interval_secs: u64,
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            endpoint_url: Some("http://localhost:4566".to_string()),
            bucket: "oxidemq-streams".to_string(),
            region: "us-east-1".to_string(),
            access_key_id: "test".to_string(),
            secret_access_key: "test".to_string(),
            force_path_style: true,
            max_object_size_bytes: 32 * 1024 * 1024, // 32 MB
            compactor_interval_secs: 30,
        }
    }
}

/// Multi-tier caching configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    pub log_cache_size_bytes: usize,
    pub block_cache_size_bytes: usize,
    pub prefetch_batch_count: usize,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            log_cache_size_bytes: 256 * 1024 * 1024, // 256 MB memory log cache
            block_cache_size_bytes: 512 * 1024 * 1024, // 512 MB LRU block cache
            prefetch_batch_count: 4,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config_lifecycle() {
        let config = OxideConfig::default();
        assert_eq!(config.broker.node_id, 1);
        assert_eq!(config.broker.cluster_id, "oxidemq-cluster-local");
        assert_eq!(config.broker.kafka_port, 9092);
        assert_eq!(config.broker.admin_port, 8082);
        assert_eq!(config.broker.host, "0.0.0.0");

        assert_eq!(config.wal.max_batch_records, 4096);
        assert_eq!(config.wal.max_segment_size_bytes, 64 * 1024 * 1024);
        assert_eq!(config.wal.group_commit_window_micros, 500);
        assert!(config.wal.sync_to_disk);
        assert!(!config.wal.direct_io);

        assert_eq!(config.s3.bucket, "oxidemq-streams");
        assert_eq!(config.s3.region, "us-east-1");
        assert_eq!(config.s3.max_object_size_bytes, 32 * 1024 * 1024);
        assert_eq!(config.s3.compactor_interval_secs, 30);
        assert!(config.s3.force_path_style);

        assert_eq!(config.cache.log_cache_size_bytes, 256 * 1024 * 1024);
        assert_eq!(config.cache.block_cache_size_bytes, 512 * 1024 * 1024);
        assert_eq!(config.cache.prefetch_batch_count, 4);
    }

    #[test]
    fn test_config_json_roundtrip() {
        let mut config = OxideConfig::default();
        config.broker.node_id = 42;
        config.broker.kafka_port = 19092;
        config.wal.sync_to_disk = false;
        config.s3.bucket = "custom-bucket".to_string();

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: OxideConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.broker.node_id, 42);
        assert_eq!(deserialized.broker.kafka_port, 19092);
        assert!(!deserialized.wal.sync_to_disk);
        assert_eq!(deserialized.s3.bucket, "custom-bucket");
    }
}
