use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use oxidemq_core::bytes_util::{compute_crc32c, AlignedBuffer};
use oxidemq_core::types::{Record, RecordBatch};

fn bench_zero_copy_slicing(c: &mut Criterion) {
    let mut group = c.benchmark_group("zero_copy_slicing");
    for size in [1024, 4096, 65536, 1048576].iter() {
        group.throughput(Throughput::Bytes(*size as u64));

        let original_data = vec![0x55u8; *size];
        let bytes_buffer = Bytes::from(original_data.clone());

        group.bench_with_input(BenchmarkId::new("bytes_slice", size), size, |b, &_s| {
            b.iter(|| {
                let sliced = black_box(&bytes_buffer).slice(128..128 + 256);
                black_box(sliced);
            });
        });

        group.bench_with_input(BenchmarkId::new("vec_clone_copy", size), size, |b, &_s| {
            b.iter(|| {
                let cloned = black_box(&original_data[128..128 + 256]).to_vec();
                black_box(cloned);
            });
        });
    }
    group.finish();
}

fn bench_crc32c_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("crc32c_hardware_accel");
    for size in [1024, 4096, 65536, 1048576].iter() {
        group.throughput(Throughput::Bytes(*size as u64));
        let data = vec![0xAAu8; *size];

        group.bench_with_input(BenchmarkId::new("compute_crc32c", size), size, |b, &_s| {
            b.iter(|| {
                let crc = compute_crc32c(black_box(&data));
                black_box(crc);
            });
        });
    }
    group.finish();
}

fn bench_aligned_buffer_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("aligned_buffer_direct_io");
    for size in [4096, 65536, 262144].iter() {
        group.throughput(Throughput::Bytes(*size as u64));
        let sample_payload = vec![0x7Eu8; *size];

        group.bench_with_input(
            BenchmarkId::new("aligned_alloc_and_fill", size),
            size,
            |b, &s| {
                b.iter(|| {
                    let mut buf = AlignedBuffer::with_default_alignment(s);
                    buf.extend_from_slice(black_box(&sample_payload));
                    black_box(buf);
                });
            },
        );
    }
    group.finish();
}

fn bench_record_batch_construction(c: &mut Criterion) {
    let mut group = c.benchmark_group("record_batch_construction");
    let payload = Bytes::from_static(b"benchmark-message-value-payload-content-128-bytes-long-padding-padding-padding-padding-padding-padding-padding-padding-padding");

    group.bench_function("create_batch_10_records", |b| {
        b.iter(|| {
            let records: Vec<Record> = (0..10)
                .map(|i| Record::new(i, 1000 + i, None, Some(payload.clone())))
                .collect();
            let batch = RecordBatch::new(black_box(0), records, payload.clone());
            black_box(batch);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_zero_copy_slicing,
    bench_crc32c_throughput,
    bench_aligned_buffer_operations,
    bench_record_batch_construction
);
criterion_main!(benches);
