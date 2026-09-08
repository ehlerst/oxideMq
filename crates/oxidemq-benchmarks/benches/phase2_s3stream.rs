use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use oxidemq_s3stream::block_cache::{BlockCache, BlockKey};
use oxidemq_s3stream::client::MemoryObjectStorage;
use oxidemq_s3stream::compactor::StreamCompactor;
use oxidemq_s3stream::format::{S3DataBlock, S3ObjectCodec};
use oxidemq_s3stream::log_cache::LogCache;
use oxidemq_s3stream::stream::S3Stream;
use oxidemq_wal::memory::MemoryWal;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

fn bench_log_cache_tail_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("log_cache_tail_reads");
    let cache = LogCache::new(64 * 1024 * 1024);

    let payload = Bytes::from(vec![0x42u8; 1024]); // 1 KB payload
    for i in 0..10_000 {
        cache.put(1, i, payload.clone());
    }

    group.throughput(Throughput::Bytes(1024));

    group.bench_function("fetch_1kb_tailing_hit", |b| {
        let mut offset = 9000;
        b.iter(|| {
            offset = (offset + 1) % 9990;
            let res = cache.read_range(1, black_box(offset), 1024).unwrap();
            black_box(res);
        });
    });

    group.finish();
}

fn bench_block_cache_lru_hits(c: &mut Criterion) {
    let mut group = c.benchmark_group("block_cache_lru");
    let cache = BlockCache::new(64 * 1024 * 1024);

    let payload = Bytes::from(vec![0x77u8; 4096]); // 4 KB block
    for i in 0..1000 {
        let key = BlockKey::new("obj-1", 1, i * 10);
        cache.put(key, payload.clone());
    }

    group.throughput(Throughput::Bytes(4096));

    group.bench_function("get_4kb_block_hit", |b| {
        let mut key_idx = 0;
        b.iter(|| {
            key_idx = (key_idx + 1) % 900;
            let key = BlockKey::new("obj-1", 1, key_idx * 10);
            let res = cache.get(black_box(&key)).unwrap();
            black_box(res);
        });
    });

    group.finish();
}

fn bench_s3_object_codec(c: &mut Criterion) {
    let mut group = c.benchmark_group("s3_object_codec");

    for block_count in [1, 10, 50].iter() {
        let block_payload = Bytes::from(vec![0xAAu8; 4096]);
        let blocks: Vec<S3DataBlock> = (0..*block_count)
            .map(|i| {
                S3DataBlock::new(
                    1,
                    (i * 10) as i64,
                    ((i + 1) * 10 - 1) as i64,
                    10,
                    block_payload.clone(),
                )
            })
            .collect();

        let total_bytes = *block_count * 4096;
        group.throughput(Throughput::Bytes(total_bytes as u64));

        group.bench_with_input(
            BenchmarkId::new("encode_blocks", block_count),
            block_count,
            |b, &_count| {
                b.iter(|| {
                    let encoded = S3ObjectCodec::encode(black_box(&blocks));
                    black_box(encoded);
                });
            },
        );

        let encoded = S3ObjectCodec::encode(&blocks);

        group.bench_with_input(
            BenchmarkId::new("decode_blocks", block_count),
            block_count,
            |b, &_count| {
                b.iter(|| {
                    let decoded = S3ObjectCodec::decode_all(black_box(&encoded)).unwrap();
                    black_box(decoded);
                });
            },
        );
    }

    group.finish();
}

fn bench_s3_stream_append_and_fetch(c: &mut Criterion) {
    let mut group = c.benchmark_group("s3_stream_operations");
    let wal = Arc::new(MemoryWal::new());
    let storage = Arc::new(MemoryObjectStorage::new());
    let log_cache = Arc::new(LogCache::new(64 * 1024 * 1024));
    let block_cache = Arc::new(BlockCache::new(64 * 1024 * 1024));

    let stream = Arc::new(S3Stream::new(100, 0, wal, storage, log_cache, block_cache));
    let payload = Bytes::from(vec![0x99u8; 1024]); // 1 KB
    group.throughput(Throughput::Bytes(1024));

    group.bench_function("stream_append_1kb", |b| {
        b.iter(|| {
            let off = stream.append(black_box(payload.clone())).unwrap();
            black_box(off);
        });
    });

    let fetch_offset = Arc::new(AtomicI64::new(0));
    group.bench_function("stream_fetch_1kb_tailing", |b| {
        b.iter(|| {
            let off = fetch_offset.fetch_add(1, Ordering::Relaxed) % 1000;
            let res = stream.fetch(black_box(off), 1024).unwrap();
            black_box(res);
        });
    });

    group.finish();
}

fn bench_stream_compaction(c: &mut Criterion) {
    let mut group = c.benchmark_group("stream_compaction");

    let record_payload = Bytes::from(vec![0xEEu8; 1024]);
    let total_compaction_bytes = 10 * 1024;
    group.throughput(Throughput::Bytes(total_compaction_bytes as u64));

    group.bench_function("compact_10_small_objects", |b| {
        b.iter_custom(|iters| {
            let mut total_duration = std::time::Duration::ZERO;
            for iter in 0..iters {
                let storage = Arc::new(MemoryObjectStorage::new());
                let wal = Arc::new(MemoryWal::new());
                let log_cache = Arc::new(LogCache::new(64 * 1024 * 1024));
                let block_cache = Arc::new(BlockCache::new(64 * 1024 * 1024));
                let stream =
                    S3Stream::new(200 + iter, 0, wal, storage.clone(), log_cache, block_cache);
                let compactor = StreamCompactor::new(storage, 64 * 1024 * 1024);

                for i in 0..10 {
                    stream
                        .upload_batch(i * 10, (i + 1) * 10 - 1, vec![record_payload.clone()])
                        .unwrap();
                }

                let start = std::time::Instant::now();
                let res = compactor.compact_stream(black_box(&stream)).unwrap();
                total_duration += start.elapsed();
                black_box(res);
            }
            total_duration
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_log_cache_tail_reads,
    bench_block_cache_lru_hits,
    bench_s3_object_codec,
    bench_s3_stream_append_and_fetch,
    bench_stream_compaction
);
criterion_main!(benches);
