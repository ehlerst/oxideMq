use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use oxidemq_broker::coordinator::GroupCoordinator;
use oxidemq_broker::handler::BrokerEngine;
use oxidemq_broker::router::ClusterState;
use oxidemq_protocol::header::RequestHeader;
use oxidemq_protocol::messages::{
    FetchPartition, FetchRequest, FetchTopic, PartitionProduceData, ProduceRequest,
    TopicProduceData,
};
use oxidemq_protocol::ApiKey;
use oxidemq_s3stream::block_cache::BlockCache;
use oxidemq_s3stream::client::MemoryObjectStorage;
use oxidemq_s3stream::log_cache::LogCache;
use oxidemq_wal::memory::MemoryWal;
use std::sync::Arc;

fn bench_network_saturation_100_partitions(c: &mut Criterion) {
    let mut group = c.benchmark_group("phase9_network_saturation");
    let batch_bytes = 4096usize; // 4KB batch
    group.throughput(Throughput::Bytes(batch_bytes as u64));

    let wal = Arc::new(MemoryWal::new());
    let storage = Arc::new(MemoryObjectStorage::new());
    let log_cache = Arc::new(LogCache::new(128 * 1024 * 1024));
    let block_cache = Arc::new(BlockCache::new(128 * 1024 * 1024));
    let state = Arc::new(ClusterState::new(
        1,
        "127.0.0.1",
        9092,
        "cluster-phase9-saturation",
        wal,
        storage,
        log_cache,
        block_cache,
    ));
    let coord = Arc::new(GroupCoordinator::new());
    let engine = BrokerEngine::new(state, coord);

    let payload = bytes::Bytes::from(vec![0x7Eu8; batch_bytes]);
    let header = RequestHeader::new(ApiKey::Produce, 0, 9001, Some("line-rate-producer"));

    let mut partition_idx = 0;
    group.bench_function("produce_100_partitions_saturation", |b| {
        b.iter(|| {
            let part = partition_idx % 100;
            partition_idx += 1;

            let req = ProduceRequest {
                acks: 1,
                timeout_ms: 1000,
                topic_data: vec![TopicProduceData {
                    topic: "saturation-100-part".to_string(),
                    partitions: vec![PartitionProduceData {
                        partition: part,
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

    let fetch_hdr = RequestHeader::new(ApiKey::Fetch, 0, 9002, Some("line-rate-consumer"));
    group.bench_function("tailing_fetch_100_partitions", |b| {
        b.iter(|| {
            let part = partition_idx % 100;
            partition_idx += 1;

            let req = FetchRequest {
                max_wait_ms: 0,
                min_bytes: 1,
                max_bytes: 65536,
                isolation_level: 0,
                topics: vec![FetchTopic {
                    topic: "saturation-100-part".to_string(),
                    partitions: vec![FetchPartition {
                        partition: part,
                        fetch_offset: 0,
                        partition_max_bytes: 65536,
                    }],
                }],
            };
            let mut body_buf = bytes::BytesMut::new();
            req.encode(&mut body_buf, 0);
            let mut b_bytes = body_buf.freeze();
            let resp = engine
                .handle_request(black_box(&fetch_hdr), black_box(&mut b_bytes))
                .unwrap();
            black_box(resp);
        });
    });

    group.finish();
}

criterion_group!(benches, bench_network_saturation_100_partitions);
criterion_main!(benches);
