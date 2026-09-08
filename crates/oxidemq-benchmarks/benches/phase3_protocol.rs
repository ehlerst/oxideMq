use bytes::{Bytes, BytesMut};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use oxidemq_protocol::codec::KafkaFrameCodec;
use oxidemq_protocol::header::RequestHeader;
use oxidemq_protocol::messages::{
    FetchPartitionResponse, FetchResponse, FetchTopicResponse, PartitionProduceData,
    ProduceRequest, TopicProduceData,
};
use oxidemq_protocol::{ApiKey, KafkaErrorCode};
use tokio_util::codec::{Decoder, Encoder};

fn bench_header_codec(c: &mut Criterion) {
    let mut group = c.benchmark_group("kafka_header_codec");
    let header = RequestHeader::new(ApiKey::Produce, 7, 12345, Some("console-producer"));
    let mut buf = BytesMut::with_capacity(64);
    header.encode(&mut buf);
    let raw = buf.freeze();

    group.bench_function("encode_request_header", |b| {
        let mut enc_buf = BytesMut::with_capacity(64);
        b.iter(|| {
            enc_buf.clear();
            header.encode(black_box(&mut enc_buf));
            black_box(&enc_buf);
        });
    });

    group.bench_function("decode_request_header", |b| {
        b.iter(|| {
            let mut cursor = raw.clone();
            let decoded = RequestHeader::decode(black_box(&mut cursor)).unwrap();
            black_box(decoded);
        });
    });

    group.finish();
}

fn bench_produce_codec(c: &mut Criterion) {
    let mut group = c.benchmark_group("kafka_produce_codec");

    for p_count in [1, 5, 20].iter() {
        let payload = Bytes::from(vec![0x33u8; 1024]); // 1 KB batch per partition
        let partitions: Vec<PartitionProduceData> = (0..*p_count)
            .map(|p| PartitionProduceData {
                partition: p,
                records: payload.clone(),
            })
            .collect();

        let req = ProduceRequest {
            acks: 1,
            timeout_ms: 1500,
            topic_data: vec![TopicProduceData {
                topic: "orders-topic".to_string(),
                partitions,
            }],
        };

        let total_bytes: usize = (*p_count as usize) * 1024;
        group.throughput(Throughput::Bytes(total_bytes as u64));

        let mut encode_buf = BytesMut::with_capacity(total_bytes + 256);
        req.encode(&mut encode_buf, 7);
        let raw = encode_buf.freeze();

        group.bench_with_input(
            BenchmarkId::new("encode_produce_req", p_count),
            p_count,
            |b, &_count| {
                let mut buf = BytesMut::with_capacity(total_bytes + 256);
                b.iter(|| {
                    buf.clear();
                    req.encode(black_box(&mut buf), 7);
                    black_box(&buf);
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("decode_produce_req", p_count),
            p_count,
            |b, &_count| {
                b.iter(|| {
                    let mut cursor = raw.clone();
                    let decoded = ProduceRequest::decode(black_box(&mut cursor), 7).unwrap();
                    black_box(decoded);
                });
            },
        );
    }

    group.finish();
}

fn bench_fetch_codec(c: &mut Criterion) {
    let mut group = c.benchmark_group("kafka_fetch_codec");

    let payload = Bytes::from(vec![0x77u8; 4096]); // 4 KB fetched batch
    let resp = FetchResponse {
        throttle_time_ms: 0,
        error_code: KafkaErrorCode::None,
        responses: vec![FetchTopicResponse {
            topic: "telemetry".to_string(),
            partitions: vec![FetchPartitionResponse {
                partition_index: 0,
                error_code: KafkaErrorCode::None,
                high_watermark: 1000,
                last_stable_offset: 1000,
                records: payload,
            }],
        }],
    };

    group.throughput(Throughput::Bytes(4096));

    let mut encode_buf = BytesMut::with_capacity(4096 + 128);
    resp.encode(&mut encode_buf, 7);
    let raw = encode_buf.freeze();

    group.bench_function("encode_fetch_response_4kb", |b| {
        let mut buf = BytesMut::with_capacity(4096 + 128);
        b.iter(|| {
            buf.clear();
            resp.encode(black_box(&mut buf), 7);
            black_box(&buf);
        });
    });

    group.bench_function("decode_fetch_response_4kb", |b| {
        b.iter(|| {
            let mut cursor = raw.clone();
            let decoded = FetchResponse::decode(black_box(&mut cursor), 7).unwrap();
            black_box(decoded);
        });
    });

    group.finish();
}

fn bench_tcp_frame_codec(c: &mut Criterion) {
    let mut group = c.benchmark_group("tcp_frame_codec");
    let mut codec = KafkaFrameCodec::new();
    let payload = Bytes::from(vec![0xAAu8; 1024]);
    group.throughput(Throughput::Bytes(1024));

    let mut framed_buf = BytesMut::with_capacity(1024 + 4);
    codec.encode(payload.clone(), &mut framed_buf).unwrap();

    group.bench_function("tcp_encode_frame_1kb", |b| {
        let mut buf = BytesMut::with_capacity(1024 + 4);
        b.iter(|| {
            buf.clear();
            codec.encode(black_box(payload.clone()), &mut buf).unwrap();
            black_box(&buf);
        });
    });

    group.bench_function("tcp_decode_frame_1kb", |b| {
        b.iter(|| {
            let mut buf = framed_buf.clone();
            let frame = codec.decode(black_box(&mut buf)).unwrap();
            black_box(frame);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_header_codec,
    bench_produce_codec,
    bench_fetch_codec,
    bench_tcp_frame_codec
);
criterion_main!(benches);
