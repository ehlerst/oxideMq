use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
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

fn bench_head_to_head_produce_1kb(c: &mut Criterion) {
    let mut group = c.benchmark_group("phase7_head_to_head_produce");
    let record_size = 1024usize;
    group.throughput(Throughput::Bytes(record_size as u64));

    let wal = Arc::new(MemoryWal::new());
    let storage = Arc::new(MemoryObjectStorage::new());
    let log_cache = Arc::new(LogCache::new(64 * 1024 * 1024));
    let block_cache = Arc::new(BlockCache::new(64 * 1024 * 1024));
    let state = Arc::new(ClusterState::new(
        1,
        "127.0.0.1",
        9092,
        "cluster-phase7-h2h",
        wal,
        storage,
        log_cache,
        block_cache,
    ));
    let coord = Arc::new(GroupCoordinator::new());
    let engine = BrokerEngine::new(state, coord);

    let payload = bytes::Bytes::from(vec![0xA5; record_size]);
    let header = RequestHeader::new(ApiKey::Produce, 0, 1001, Some("h2h-producer"));

    // Head-to-head produce throughput & latency benchmark (1KB record size)
    // Compares against AutoMQ: 17,331 records/sec (16.92 MB/s), 116.17 ms avg latency
    group.bench_function("oxidemq_produce_1kb_record", |b| {
        b.iter(|| {
            let req = ProduceRequest {
                acks: 1,
                timeout_ms: 1000,
                topic_data: vec![TopicProduceData {
                    topic: "h2h-bench-topic".to_string(),
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

fn bench_head_to_head_cold_start(c: &mut Criterion) {
    let mut group = c.benchmark_group("phase7_head_to_head_cold_start");

    // Pure Rust oxideMq cold start: full storage engine, WAL, caches, cluster state
    // Compares against AutoMQ: ~18.0 seconds JVM cold start (18,000,000 µs)
    group.bench_function("oxidemq_cold_start_to_ready", |b| {
        b.iter(|| {
            let start = Instant::now();
            let wal = Arc::new(MemoryWal::new());
            let storage = Arc::new(MemoryObjectStorage::new());
            let log_cache = Arc::new(LogCache::new(4 * 1024 * 1024));
            let block_cache = Arc::new(BlockCache::new(4 * 1024 * 1024));
            let state = Arc::new(ClusterState::new(
                1,
                "127.0.0.1",
                9092,
                "h2h-cold-start-cluster",
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

criterion_group!(
    benches,
    bench_head_to_head_produce_1kb,
    bench_head_to_head_cold_start
);
criterion_main!(benches);
