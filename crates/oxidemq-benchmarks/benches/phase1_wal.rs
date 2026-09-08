use bytes::{Bytes, BytesMut};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use oxidemq_core::config::WalConfig;
use oxidemq_wal::file::FileWal;
use oxidemq_wal::frame::WalRecord;
use oxidemq_wal::memory::MemoryWal;
use oxidemq_wal::recovery::WalRecovery;
use oxidemq_wal::segment::WalSegment;
use oxidemq_wal::WalEngine;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use tempfile::tempdir;

fn bench_wal_framing(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_framing");
    for size in [256, 1024, 4096, 65536].iter() {
        group.throughput(Throughput::Bytes(*size as u64));
        let payload = Bytes::from(vec![0x42u8; *size]);
        let record = WalRecord::new(1, 10, 100, payload);

        let mut encode_buf = BytesMut::with_capacity(*size + 64);

        group.bench_with_input(BenchmarkId::new("encode_record", size), size, |b, &_s| {
            b.iter(|| {
                encode_buf.clear();
                record.encode(&mut encode_buf);
                black_box(&encode_buf);
            });
        });

        encode_buf.clear();
        record.encode(&mut encode_buf);
        let encoded_bytes = encode_buf.freeze();

        group.bench_with_input(BenchmarkId::new("decode_record", size), size, |b, &_s| {
            b.iter(|| {
                let res = WalRecord::decode(black_box(&encoded_bytes)).unwrap();
                black_box(res);
            });
        });
    }
    group.finish();
}

fn bench_memory_wal_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_wal");
    let payload = vec![0x33u8; 1024]; // 1 KB record
    group.throughput(Throughput::Bytes(payload.len() as u64));

    let wal = Arc::new(MemoryWal::new());
    let offset_counter = Arc::new(AtomicI64::new(0));

    group.bench_function("single_thread_append_1kb", |b| {
        b.iter(|| {
            let off = offset_counter.fetch_add(1, Ordering::Relaxed);
            let seq = wal.append(1, off, black_box(&payload)).unwrap();
            black_box(seq);
        });
    });

    group.finish();
}

fn bench_file_wal_group_commit(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let config = WalConfig {
        dir: dir.path().to_path_buf(),
        max_segment_size_bytes: 64 * 1024 * 1024,
        group_commit_window_micros: 200,
        max_batch_records: 1024,
        sync_to_disk: false, // Benchmark memory-to-OS write throughput
        direct_io: false,
    };

    let wal = Arc::new(FileWal::open(config).unwrap());
    let offset_counter = Arc::new(AtomicI64::new(0));

    let mut group = c.benchmark_group("file_wal_group_commit");
    let payload = vec![0x77u8; 1024]; // 1 KB payload
    group.throughput(Throughput::Bytes(payload.len() as u64));

    group.bench_function("append_1kb_batching", |b| {
        b.iter(|| {
            let off = offset_counter.fetch_add(1, Ordering::Relaxed);
            let seq = wal.append(1, off, black_box(&payload)).unwrap();
            black_box(seq);
        });
    });

    group.finish();
}

fn bench_wal_recovery_throughput(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let mut segment = WalSegment::create(dir.path(), 1).unwrap();

    let payload = Bytes::from(vec![0xAAu8; 1024]);
    let mut encode_buf = BytesMut::with_capacity(1024 + 64);
    let mut total_bytes = 0;

    for i in 0..5000 {
        encode_buf.clear();
        let record = WalRecord::new(i + 1, 1, i as i64, payload.clone());
        record.encode(&mut encode_buf);
        segment.append(&encode_buf).unwrap();
        total_bytes += encode_buf.len();
    }
    segment.sync().unwrap();
    drop(segment);

    let mut group = c.benchmark_group("wal_recovery");
    group.throughput(Throughput::Bytes(total_bytes as u64));

    group.bench_function("scan_and_validate_5000_records", |b| {
        b.iter(|| {
            let report = WalRecovery::recover(black_box(dir.path())).unwrap();
            black_box(report);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_wal_framing,
    bench_memory_wal_throughput,
    bench_file_wal_group_commit,
    bench_wal_recovery_throughput
);
criterion_main!(benches);
