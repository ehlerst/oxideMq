use bytes::{Bytes, BytesMut};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use oxidemq_broker::{BrokerEngine, ClusterState, GroupCoordinator};
use oxidemq_protocol::header::RequestHeader;
use oxidemq_protocol::messages::{
    FetchPartition, FetchRequest, MetadataRequest, PartitionProduceData, ProduceRequest,
    TopicProduceData,
};
use oxidemq_protocol::ApiKey;
use oxidemq_s3stream::block_cache::BlockCache;
use oxidemq_s3stream::client::MemoryObjectStorage;
use oxidemq_s3stream::log_cache::LogCache;
use oxidemq_wal::memory::MemoryWal;
use std::sync::Arc;

fn create_bench_engine() -> BrokerEngine {
    let wal = Arc::new(MemoryWal::new());
    let storage = Arc::new(MemoryObjectStorage::new());
    let log_cache = Arc::new(LogCache::new(64 * 1024 * 1024));
    let block_cache = Arc::new(BlockCache::new(64 * 1024 * 1024));
    let state = Arc::new(ClusterState::new(
        1,
        "127.0.0.1",
        9092,
        "bench-cluster",
        wal,
        storage,
        log_cache,
        block_cache,
    ));
    let coord = Arc::new(GroupCoordinator::new());
    BrokerEngine::new(state, coord)
}

fn bench_broker_produce(c: &mut Criterion) {
    let mut group = c.benchmark_group("phase4_broker_produce");
    let sizes = [1024, 4096, 16384];

    for &size in &sizes {
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::new("produce_batch", size), &size, |b, &s| {
            let engine = create_bench_engine();
            let payload = Bytes::from(vec![0xAA; s]);
            let header = RequestHeader::new(ApiKey::Produce, 0, 1, Some("bench-producer"));

            let req = ProduceRequest {
                acks: 1,
                timeout_ms: 1000,
                topic_data: vec![TopicProduceData {
                    topic: "bench-topic".to_string(),
                    partitions: vec![PartitionProduceData {
                        partition: 0,
                        records: payload.clone(),
                    }],
                }],
            };

            let mut body_buf = BytesMut::new();
            req.encode(&mut body_buf, 0);
            let encoded_body = body_buf.freeze();

            b.iter(|| {
                let mut body = encoded_body.clone();
                let resp = engine
                    .handle_request(black_box(&header), black_box(&mut body))
                    .unwrap();
                black_box(resp);
            });
        });
    }
    group.finish();
}

fn bench_broker_fetch(c: &mut Criterion) {
    let mut group = c.benchmark_group("phase4_broker_fetch");
    let engine = create_bench_engine();

    // Pre-populate with records
    let payload = Bytes::from(vec![0xBB; 4096]);
    let req_header = RequestHeader::new(ApiKey::Produce, 0, 1, Some("bench-producer"));
    for _ in 0..100 {
        let req = ProduceRequest {
            acks: 1,
            timeout_ms: 1000,
            topic_data: vec![TopicProduceData {
                topic: "bench-fetch-topic".to_string(),
                partitions: vec![PartitionProduceData {
                    partition: 0,
                    records: payload.clone(),
                }],
            }],
        };
        let mut body_buf = BytesMut::new();
        req.encode(&mut body_buf, 0);
        let mut b = body_buf.freeze();
        let _ = engine.handle_request(&req_header, &mut b).unwrap();
    }

    group.throughput(Throughput::Bytes(4096));
    group.bench_function("fetch_tail_log_cache", |b| {
        let fetch_header = RequestHeader::new(ApiKey::Fetch, 0, 2, Some("bench-consumer"));
        let fetch_req = FetchRequest {
            max_wait_ms: 100,
            min_bytes: 1,
            max_bytes: 65536,
            topics: vec![oxidemq_protocol::FetchTopic {
                topic: "bench-fetch-topic".to_string(),
                partitions: vec![FetchPartition {
                    partition: 0,
                    fetch_offset: 0,
                    partition_max_bytes: 65536,
                }],
            }],
        };
        let mut fetch_body = BytesMut::new();
        fetch_req.encode(&mut fetch_body, 0);
        let encoded_fetch = fetch_body.freeze();

        b.iter(|| {
            let mut body = encoded_fetch.clone();
            let resp = engine
                .handle_request(black_box(&fetch_header), black_box(&mut body))
                .unwrap();
            black_box(resp);
        });
    });
    group.finish();
}

fn bench_broker_metadata(c: &mut Criterion) {
    let mut group = c.benchmark_group("phase4_broker_metadata");
    let engine = create_bench_engine();
    let header = RequestHeader::new(ApiKey::Metadata, 0, 10, Some("bench-client"));

    let req = MetadataRequest {
        topics: Some(vec!["orders".to_string(), "users".to_string()]),
        allow_auto_topic_creation: true,
    };
    let mut body_buf = BytesMut::new();
    req.encode(&mut body_buf, 0);
    let encoded = body_buf.freeze();

    group.bench_function("metadata_dispatch", |b| {
        b.iter(|| {
            let mut body = encoded.clone();
            let resp = engine
                .handle_request(black_box(&header), black_box(&mut body))
                .unwrap();
            black_box(resp);
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_broker_produce,
    bench_broker_fetch,
    bench_broker_metadata
);
criterion_main!(benches);
