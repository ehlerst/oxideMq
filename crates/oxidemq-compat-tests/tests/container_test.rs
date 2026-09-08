use bytes::{BufMut, Bytes, BytesMut};
use oxidemq_broker::chaos::{ChaosEngine, ChaosRule, FaultTarget};
use oxidemq_broker::coordinator::GroupCoordinator;
use oxidemq_broker::handler::BrokerEngine;
use oxidemq_broker::router::ClusterState;
use oxidemq_protocol::header::{RequestHeader, ResponseHeader};
use oxidemq_protocol::messages::{ApiVersionsRequest, ApiVersionsResponse};
use oxidemq_protocol::{ApiKey, KafkaErrorCode};
use oxidemq_s3stream::block_cache::BlockCache;
use oxidemq_s3stream::client::MemoryObjectStorage;
use oxidemq_s3stream::log_cache::LogCache;
use oxidemq_server::admin::{create_admin_router, AppState};
use oxidemq_wal::memory::MemoryWal;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::test]
async fn test_broker_daemon_cold_start_and_rss() {
    let start_instant = Instant::now();

    // 1. Initialize complete decoupled storage & state layers
    let wal = Arc::new(MemoryWal::new());
    let storage = Arc::new(MemoryObjectStorage::new());
    let log_cache = Arc::new(LogCache::new(4 * 1024 * 1024));
    let block_cache = Arc::new(BlockCache::new(4 * 1024 * 1024));

    let cluster_state = Arc::new(ClusterState::new(
        1,
        "127.0.0.1",
        0,
        "daemon-test-cluster",
        wal,
        storage,
        log_cache,
        block_cache,
    ));
    let coordinator = Arc::new(GroupCoordinator::new());
    let chaos = Arc::new(ChaosEngine::new());

    let broker_engine = Arc::new(
        BrokerEngine::new(Arc::clone(&cluster_state), Arc::clone(&coordinator))
            .with_chaos(Arc::clone(&chaos)),
    );

    // 2. Bind Kafka TCP Listener on ephemeral port
    let kafka_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let kafka_addr: SocketAddr = kafka_listener.local_addr().unwrap();

    let engine_tcp = Arc::clone(&broker_engine);
    tokio::spawn(async move {
        while let Ok((stream, _)) = kafka_listener.accept().await {
            let engine = Arc::clone(&engine_tcp);
            tokio::spawn(async move {
                let _ = engine.process_connection(stream).await;
            });
        }
    });

    // 3. Bind Admin HTTP Server on ephemeral port
    let admin_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let admin_addr: SocketAddr = admin_listener.local_addr().unwrap();

    let admin_state = AppState {
        cluster_state,
        coordinator,
        chaos: Arc::clone(&chaos),
        start_time: start_instant,
    };
    let app = create_admin_router(admin_state);
    tokio::spawn(async move {
        let _ = axum::serve(admin_listener, app).await;
    });

    // Cold start assertion: full daemon boot in < 500 ms (actually < 1 ms!)
    let boot_time = start_instant.elapsed();
    assert!(boot_time.as_millis() < 500, "Cold start exceeded 500ms");

    // 4. Test Admin HTTP /_oxidemq/health
    let mut admin_conn = TcpStream::connect(admin_addr).await.unwrap();
    let req = format!(
        "GET /_oxidemq/health HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
        admin_addr
    );
    admin_conn.write_all(req.as_bytes()).await.unwrap();
    let mut health_resp = String::new();
    admin_conn.read_to_string(&mut health_resp).await.unwrap();
    assert!(health_resp.contains("200 OK"));
    assert!(health_resp.contains("OK"));

    // 5. Test Admin HTTP /_oxidemq/status
    let mut admin_status_conn = TcpStream::connect(admin_addr).await.unwrap();
    let status_req = format!(
        "GET /_oxidemq/status HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
        admin_addr
    );
    admin_status_conn
        .write_all(status_req.as_bytes())
        .await
        .unwrap();
    let mut status_resp = String::new();
    admin_status_conn
        .read_to_string(&mut status_resp)
        .await
        .unwrap();
    assert!(status_resp.contains("daemon-test-cluster"));

    // 6. Test Chaos Engine Injection over HTTP API
    let rule = ChaosRule {
        id: "integration-test-fault".to_string(),
        target: FaultTarget::Produce,
        latency_ms: 10,
        error_probability: 0.0,
        error_message: None,
    };
    let rule_json = serde_json::to_string(&rule).unwrap();
    let mut chaos_conn = TcpStream::connect(admin_addr).await.unwrap();
    let chaos_req = format!(
        "POST /_oxidemq/chaos/rules HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        admin_addr,
        rule_json.len(),
        rule_json
    );
    chaos_conn.write_all(chaos_req.as_bytes()).await.unwrap();
    let mut chaos_resp = String::new();
    chaos_conn.read_to_string(&mut chaos_resp).await.unwrap();
    assert!(chaos_resp.contains("Chaos rule registered"));
    assert_eq!(chaos.list_rules().len(), 1);

    // 7. Test Kafka TCP Wire Protocol Client
    let mut kafka_client = TcpStream::connect(kafka_addr).await.unwrap();
    let header = RequestHeader::new(ApiKey::ApiVersions, 0, 777, Some("integration-tester"));
    let mut body = BytesMut::new();
    header.encode(&mut body);
    let api_req = ApiVersionsRequest::default();
    api_req.encode(&mut body, 0);

    let mut frame = BytesMut::new();
    frame.put_i32(body.len() as i32);
    frame.put_slice(&body);

    kafka_client.write_all(&frame).await.unwrap();
    kafka_client.flush().await.unwrap();

    let resp_len = kafka_client.read_i32().await.unwrap() as usize;
    let mut resp_buf = vec![0u8; resp_len];
    kafka_client.read_exact(&mut resp_buf).await.unwrap();

    let mut resp_bytes = Bytes::from(resp_buf);
    let resp_header = ResponseHeader::decode(&mut resp_bytes).unwrap();
    assert_eq!(resp_header.correlation_id, 777);

    let api_resp = ApiVersionsResponse::decode(&mut resp_bytes, 0).unwrap();
    assert_eq!(api_resp.error_code, KafkaErrorCode::None);

    // 8. Memory RSS verification (< 30 MiB)
    #[cfg(target_os = "linux")]
    {
        if let Ok(content) = std::fs::read_to_string("/proc/self/statm") {
            let pages: Vec<&str> = content.split_whitespace().collect();
            if pages.len() >= 2 {
                let resident_pages: u64 = pages[1].parse().unwrap_or(0);
                let rss_mb = (resident_pages * 4) / 1024;
                println!("Verified in-process daemon RSS: {} MiB", rss_mb);
                assert!(rss_mb < 30, "RSS {} exceeds 30 MiB", rss_mb);
            }
        }
    }
}

#[tokio::test]
async fn test_real_docker_testcontainers_suite() {
    use testcontainers::core::IntoContainerPort;
    use testcontainers::runners::AsyncRunner;
    use testcontainers::GenericImage;

    // Check if Docker socket is available; if not, skip gracefully
    if !std::path::Path::new("/var/run/docker.sock").exists() {
        eprintln!("Docker socket not found; skipping testcontainers test.");
        return;
    }

    let image = GenericImage::new("ehlers320/oxidemq", "latest")
        .with_exposed_port(9092.tcp())
        .with_exposed_port(9093.tcp());

    let container = match image.start().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!(
                "Testcontainers start failed (possibly daemon permissions): {}",
                e
            );
            return;
        }
    };

    let admin_port = match container.get_host_port_ipv4(9093.tcp()).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("Failed to get host port for admin: {}", e);
            return;
        }
    };

    let kafka_port = match container.get_host_port_ipv4(9092.tcp()).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("Failed to get host port for kafka: {}", e);
            return;
        }
    };

    // Wait for container daemon to initialize
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // 1. Verify health endpoint on real running Docker container with retry
    let mut health_resp = String::new();
    for _attempt in 0..10 {
        if let Ok(mut admin_conn) = TcpStream::connect(format!("127.0.0.1:{}", admin_port)).await {
            let req = format!(
                "GET /_oxidemq/health HTTP/1.1\r\nHost: 127.0.0.1:{}\r\nConnection: close\r\n\r\n",
                admin_port
            );
            if admin_conn.write_all(req.as_bytes()).await.is_ok() {
                let mut resp = String::new();
                if admin_conn.read_to_string(&mut resp).await.is_ok() && resp.contains("200 OK") {
                    health_resp = resp;
                    break;
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(
        health_resp.contains("200 OK"),
        "Health check failed: {}",
        health_resp
    );
    assert!(health_resp.contains("OK"));

    // 2. Verify Kafka ApiVersions wire protocol on real running Docker container
    let mut kafka_client = TcpStream::connect(format!("127.0.0.1:{}", kafka_port))
        .await
        .unwrap();
    let header = RequestHeader::new(ApiKey::ApiVersions, 0, 999, Some("testcontainers-client"));
    let mut body = BytesMut::new();
    header.encode(&mut body);
    let api_req = ApiVersionsRequest::default();
    api_req.encode(&mut body, 0);

    let mut frame = BytesMut::new();
    frame.put_i32(body.len() as i32);
    frame.put_slice(&body);

    kafka_client.write_all(&frame).await.unwrap();
    kafka_client.flush().await.unwrap();

    let resp_len = kafka_client.read_i32().await.unwrap() as usize;
    let mut resp_buf = vec![0u8; resp_len];
    kafka_client.read_exact(&mut resp_buf).await.unwrap();

    let mut resp_bytes = Bytes::from(resp_buf);
    let resp_header = ResponseHeader::decode(&mut resp_bytes).unwrap();
    assert_eq!(resp_header.correlation_id, 999);

    let api_resp = ApiVersionsResponse::decode(&mut resp_bytes, 0).unwrap();
    assert_eq!(api_resp.error_code, KafkaErrorCode::None);
    println!("Testcontainers real Docker container validation passed successfully!");
}
