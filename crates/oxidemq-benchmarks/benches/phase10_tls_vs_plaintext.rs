use bytes::{BufMut, Bytes, BytesMut};
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use oxidemq_broker::coordinator::GroupCoordinator;
use oxidemq_broker::handler::BrokerEngine;
use oxidemq_broker::router::ClusterState;
use oxidemq_protocol::header::{RequestHeader, ResponseHeader};
use oxidemq_protocol::messages::{
    FetchPartition, FetchRequest, FetchTopic, PartitionProduceData, ProduceRequest,
    ProduceResponse, TopicProduceData,
};
use oxidemq_protocol::ApiKey;
use oxidemq_s3stream::block_cache::BlockCache;
use oxidemq_s3stream::client::MemoryObjectStorage;
use oxidemq_s3stream::log_cache::LogCache;
use oxidemq_server::tls::{create_insecure_tls_connector, create_tls_acceptor, TlsConfig};
use oxidemq_wal::memory::MemoryWal;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Runtime;

struct TestServer {
    plain_addr: SocketAddr,
    tls_addr: SocketAddr,
    _runtime: Arc<Runtime>,
}

fn setup_test_server() -> TestServer {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap(),
    );

    let wal = Arc::new(MemoryWal::new());
    let storage = Arc::new(MemoryObjectStorage::new());
    let log_cache = Arc::new(LogCache::new(64 * 1024 * 1024));
    let block_cache = Arc::new(BlockCache::new(64 * 1024 * 1024));
    let state = Arc::new(ClusterState::new(
        1,
        "127.0.0.1",
        9092,
        "cluster-tls-benchmark".to_string(),
        wal,
        storage,
        log_cache,
        block_cache,
    ));
    let coord = Arc::new(GroupCoordinator::new());
    let engine = Arc::new(BrokerEngine::new(state, coord));

    let (plain_addr, tls_addr) = rt.block_on(async {
        // Plaintext listener
        let plain_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let p_addr = plain_listener.local_addr().unwrap();
        let engine_plain = Arc::clone(&engine);
        tokio::spawn(async move {
            while let Ok((socket, _)) = plain_listener.accept().await {
                let eng = Arc::clone(&engine_plain);
                tokio::spawn(async move {
                    let _ = eng.process_connection(socket).await;
                });
            }
        });

        // TLS listener
        let tls_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let t_addr = tls_listener.local_addr().unwrap();
        let acceptor = create_tls_acceptor(&TlsConfig::default()).unwrap();
        let engine_tls = Arc::clone(&engine);
        tokio::spawn(async move {
            while let Ok((socket, _)) = tls_listener.accept().await {
                let eng = Arc::clone(&engine_tls);
                let acc = acceptor.clone();
                tokio::spawn(async move {
                    if let Ok(tls_stream) = acc.accept(socket).await {
                        let _ = eng.process_connection(tls_stream).await;
                    }
                });
            }
        });

        (p_addr, t_addr)
    });

    TestServer {
        plain_addr,
        tls_addr,
        _runtime: rt,
    }
}

async fn send_produce<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    topic: &str,
    partition: i32,
    payload: &[u8],
    correlation_id: i32,
) {
    let req = ProduceRequest {
        acks: 1,
        timeout_ms: 10000,
        topic_data: vec![TopicProduceData {
            topic: topic.to_string(),
            partitions: vec![PartitionProduceData {
                partition,
                records: Bytes::copy_from_slice(payload),
            }],
        }],
    };

    let header = RequestHeader::new(ApiKey::Produce, 0, correlation_id, Some("bench-client"));
    let mut body = BytesMut::new();
    req.encode(&mut body, 0);

    let mut frame = BytesMut::with_capacity(body.len() + 64);
    header.encode(&mut frame);
    frame.extend_from_slice(&body);

    let mut wire = BytesMut::with_capacity(frame.len() + 4);
    wire.put_i32(frame.len() as i32);
    wire.extend_from_slice(&frame);

    stream.write_all(&wire).await.unwrap();
    stream.flush().await.unwrap();

    let resp_len = stream.read_i32().await.unwrap() as usize;
    let mut resp_buf = vec![0u8; resp_len];
    stream.read_exact(&mut resp_buf).await.unwrap();
    let mut resp_bytes = Bytes::from(resp_buf);
    let _ = ResponseHeader::decode(&mut resp_bytes).unwrap();
    let _ = ProduceResponse::decode(&mut resp_bytes, 0).unwrap();
}

async fn send_fetch<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    topic: &str,
    partition: i32,
    correlation_id: i32,
) {
    let req = FetchRequest {
        max_wait_ms: 0,
        min_bytes: 1,
        max_bytes: 65536,
        isolation_level: 0,
        topics: vec![FetchTopic {
            topic: topic.to_string(),
            partitions: vec![FetchPartition {
                partition,
                fetch_offset: 0,
                partition_max_bytes: 65536,
            }],
        }],
    };

    let header = RequestHeader::new(ApiKey::Fetch, 0, correlation_id, Some("bench-client"));
    let mut body = BytesMut::new();
    req.encode(&mut body, 0);

    let mut frame = BytesMut::with_capacity(body.len() + 64);
    header.encode(&mut frame);
    frame.extend_from_slice(&body);

    let mut wire = BytesMut::with_capacity(frame.len() + 4);
    wire.put_i32(frame.len() as i32);
    wire.extend_from_slice(&frame);

    stream.write_all(&wire).await.unwrap();
    stream.flush().await.unwrap();

    let resp_len = stream.read_i32().await.unwrap() as usize;
    let mut resp_buf = vec![0u8; resp_len];
    stream.read_exact(&mut resp_buf).await.unwrap();
}

fn bench_tls_vs_plaintext(c: &mut Criterion) {
    let server = setup_test_server();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let payload_1kb = Arc::new(vec![0xABu8; 1024]);
    let payload_16kb = Arc::new(vec![0xCDu8; 16384]);

    // =========================================================================
    // 1. Connection & Handshake Establishment Latency
    // =========================================================================
    {
        let mut group = c.benchmark_group("phase10_handshake");
        group.bench_function("tcp_connect_plaintext", |b| {
            b.iter(|| {
                rt.block_on(async {
                    let stream = TcpStream::connect(server.plain_addr).await.unwrap();
                    criterion::black_box(stream);
                });
            });
        });

        group.bench_function("tls_connect_and_handshake", |b| {
            let connector = create_insecure_tls_connector();
            b.iter(|| {
                let conn = connector.clone();
                rt.block_on(async {
                    let tcp = TcpStream::connect(server.tls_addr).await.unwrap();
                    let server_name = "localhost".try_into().unwrap();
                    let tls = conn.connect(server_name, tcp).await.unwrap();
                    criterion::black_box(tls);
                });
            });
        });
        group.finish();
    }

    // =========================================================================
    // 2. Continuous 1KB Record Produce Throughput & Roundtrip Latency
    // =========================================================================
    {
        let mut group = c.benchmark_group("phase10_produce_1kb");
        group.throughput(Throughput::Bytes(1024));

        let p_1kb = Arc::clone(&payload_1kb);
        group.bench_function("plaintext_produce_1kb", |b| {
            let payload = Arc::clone(&p_1kb);
            b.to_async(&rt).iter_custom(|iters| {
                let payload = Arc::clone(&payload);
                async move {
                    let mut stream = TcpStream::connect(server.plain_addr).await.unwrap();
                    let start = std::time::Instant::now();
                    for i in 0..iters {
                        send_produce(&mut stream, "bench-1kb-plain", 0, &payload, i as i32).await;
                    }
                    start.elapsed()
                }
            });
        });

        let p_1kb_tls = Arc::clone(&payload_1kb);
        group.bench_function("tls_produce_1kb", |b| {
            let connector = create_insecure_tls_connector();
            let payload = Arc::clone(&p_1kb_tls);
            b.to_async(&rt).iter_custom(|iters| {
                let conn = connector.clone();
                let payload = Arc::clone(&payload);
                async move {
                    let tcp = TcpStream::connect(server.tls_addr).await.unwrap();
                    let server_name = "localhost".try_into().unwrap();
                    let mut stream = conn.connect(server_name, tcp).await.unwrap();
                    let start = std::time::Instant::now();
                    for i in 0..iters {
                        send_produce(&mut stream, "bench-1kb-tls", 0, &payload, i as i32).await;
                    }
                    start.elapsed()
                }
            });
        });
        group.finish();
    }

    // =========================================================================
    // 3. Bulk 16KB Record Produce (Symmetric Cipher Throughput)
    // =========================================================================
    {
        let mut group = c.benchmark_group("phase10_produce_16kb");
        group.throughput(Throughput::Bytes(16384));

        let p_16kb = Arc::clone(&payload_16kb);
        group.bench_function("plaintext_produce_16kb", |b| {
            let payload = Arc::clone(&p_16kb);
            b.to_async(&rt).iter_custom(|iters| {
                let payload = Arc::clone(&payload);
                async move {
                    let mut stream = TcpStream::connect(server.plain_addr).await.unwrap();
                    let start = std::time::Instant::now();
                    for i in 0..iters {
                        send_produce(&mut stream, "bench-16kb-plain", 0, &payload, i as i32).await;
                    }
                    start.elapsed()
                }
            });
        });

        let p_16kb_tls = Arc::clone(&payload_16kb);
        group.bench_function("tls_produce_16kb", |b| {
            let connector = create_insecure_tls_connector();
            let payload = Arc::clone(&p_16kb_tls);
            b.to_async(&rt).iter_custom(|iters| {
                let conn = connector.clone();
                let payload = Arc::clone(&payload);
                async move {
                    let tcp = TcpStream::connect(server.tls_addr).await.unwrap();
                    let server_name = "localhost".try_into().unwrap();
                    let mut stream = conn.connect(server_name, tcp).await.unwrap();
                    let start = std::time::Instant::now();
                    for i in 0..iters {
                        send_produce(&mut stream, "bench-16kb-tls", 0, &payload, i as i32).await;
                    }
                    start.elapsed()
                }
            });
        });
        group.finish();
    }

    // =========================================================================
    // 4. Fetch Latency (Tailing Consumer)
    // =========================================================================
    {
        let mut group = c.benchmark_group("phase10_fetch_roundtrip");
        let p_fetch = Arc::clone(&payload_1kb);
        group.bench_function("plaintext_fetch", |b| {
            let payload = Arc::clone(&p_fetch);
            b.to_async(&rt).iter_custom(|iters| {
                let payload = Arc::clone(&payload);
                async move {
                    let mut stream = TcpStream::connect(server.plain_addr).await.unwrap();
                    // Ensure data is produced
                    send_produce(&mut stream, "bench-fetch-plain", 0, &payload, 999).await;
                    let start = std::time::Instant::now();
                    for i in 0..iters {
                        send_fetch(&mut stream, "bench-fetch-plain", 0, i as i32).await;
                    }
                    start.elapsed()
                }
            });
        });

        let p_fetch_tls = Arc::clone(&payload_1kb);
        group.bench_function("tls_fetch", |b| {
            let connector = create_insecure_tls_connector();
            let payload = Arc::clone(&p_fetch_tls);
            b.to_async(&rt).iter_custom(|iters| {
                let conn = connector.clone();
                let payload = Arc::clone(&payload);
                async move {
                    let tcp = TcpStream::connect(server.tls_addr).await.unwrap();
                    let server_name = "localhost".try_into().unwrap();
                    let mut stream = conn.connect(server_name, tcp).await.unwrap();
                    // Ensure data is produced
                    send_produce(&mut stream, "bench-fetch-tls", 0, &payload, 999).await;
                    let start = std::time::Instant::now();
                    for i in 0..iters {
                        send_fetch(&mut stream, "bench-fetch-tls", 0, i as i32).await;
                    }
                    start.elapsed()
                }
            });
        });
        group.finish();
    }
}

criterion_group!(benches, bench_tls_vs_plaintext);
criterion_main!(benches);
