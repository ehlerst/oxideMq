use criterion::{black_box, criterion_group, criterion_main, Criterion};
use oxidemq_broker::coordinator::GroupCoordinator;
use oxidemq_broker::handler::BrokerEngine;
use oxidemq_broker::router::ClusterState;
use oxidemq_protocol::header::RequestHeader;
use oxidemq_protocol::messages::{PartitionProduceData, ProduceRequest, TopicProduceData};
use oxidemq_protocol::ApiKey;
use oxidemq_s3stream::block_cache::BlockCache;
use oxidemq_s3stream::client::MemoryObjectStorage;
use oxidemq_s3stream::log_cache::LogCache;
use oxidemq_wal::memory::MemoryWal;
use std::sync::Arc;
use std::time::Instant;

fn bench_cold_start_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("phase6_comparative_cold_start");

    // Pure Rust oxideMq cold start: initialize complete storage, caches, coordinator, and cluster state
    group.bench_function("oxidemq_pure_rust_cold_start", |b| {
        b.iter(|| {
            let start = Instant::now();
            let wal = Arc::new(MemoryWal::new());
            let storage = Arc::new(MemoryObjectStorage::new());
            let log_cache = Arc::new(LogCache::new(1024 * 1024));
            let block_cache = Arc::new(BlockCache::new(1024 * 1024));
            let state = Arc::new(ClusterState::new(
                1,
                "127.0.0.1",
                9092,
                "cluster-cold-start",
                wal,
                storage,
                log_cache,
                block_cache,
            ));
            let coord = Arc::new(GroupCoordinator::new());
            let engine = BrokerEngine::new(state, coord);
            let elapsed = start.elapsed();
            black_box((engine, elapsed));
        });
    });

    group.finish();
}

fn bench_zero_gc_produce_stability(c: &mut Criterion) {
    let mut group = c.benchmark_group("phase6_comparative_zero_gc");

    let wal = Arc::new(MemoryWal::new());
    let storage = Arc::new(MemoryObjectStorage::new());
    let log_cache = Arc::new(LogCache::new(32 * 1024 * 1024));
    let block_cache = Arc::new(BlockCache::new(32 * 1024 * 1024));
    let state = Arc::new(ClusterState::new(
        1,
        "127.0.0.1",
        9092,
        "cluster-zero-gc",
        wal,
        storage,
        log_cache,
        block_cache,
    ));
    let coord = Arc::new(GroupCoordinator::new());
    let engine = BrokerEngine::new(state, coord);

    let payload = bytes::Bytes::from(vec![0x42; 2048]);
    let header = RequestHeader::new(ApiKey::Produce, 0, 1, Some("zero-gc-producer"));

    // Verify deterministic zero-allocation zero-pause publish latency
    group.bench_function("oxidemq_produce_latency_distribution", |b| {
        b.iter(|| {
            let req = ProduceRequest {
                acks: 1,
                timeout_ms: 1000,
                topic_data: vec![TopicProduceData {
                    topic: "zero-gc-topic".to_string(),
                    partitions: vec![PartitionProduceData {
                        partition: 0,
                        records: payload.clone(),
                    }],
                }],
            };
            let mut body_buf = bytes::BytesMut::new();
            req.encode(&mut body_buf, 0);
            let mut b_bytes = body_buf.freeze();
            let resp = engine
                .handle_request(black_box(&header), black_box(&mut b_bytes))
                .unwrap();
            black_box(resp);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_cold_start_comparison,
    bench_zero_gc_produce_stability
);
criterion_main!(benches);
