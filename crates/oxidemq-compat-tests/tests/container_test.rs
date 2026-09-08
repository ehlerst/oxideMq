use bytes::{BufMut, Bytes, BytesMut};
use oxidemq_protocol::header::{RequestHeader, ResponseHeader};
use oxidemq_protocol::messages::{ApiVersionsRequest, ApiVersionsResponse};
use oxidemq_protocol::{ApiKey, KafkaErrorCode};
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::process::Command;

fn find_oxidemq_binary() -> (String, Vec<String>) {
    let target_debug = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/debug/oxidemq");
    if target_debug.exists() {
        (target_debug.to_string_lossy().to_string(), vec![])
    } else {
        (
            "cargo".to_string(),
            vec![
                "run".to_string(),
                "-q".to_string(),
                "-p".to_string(),
                "oxidemq-server".to_string(),
                "--bin".to_string(),
                "oxidemq".to_string(),
                "--".to_string(),
            ],
        )
    }
}

#[tokio::test]
async fn test_broker_daemon_cold_start_and_rss() {
    let (bin, extra_args) = find_oxidemq_binary();

    // 1. Launch daemon on dedicated ephemeral ports
    let kafka_port = 19092;
    let admin_port = 19093;

    let mut cmd = Command::new(bin);
    for arg in extra_args {
        cmd.arg(arg);
    }

    let mut child = cmd
        .arg("start")
        .arg("--kafka-port")
        .arg(kafka_port.to_string())
        .arg("--admin-port")
        .arg(admin_port.to_string())
        .arg("--host")
        .arg("127.0.0.1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to spawn oxidemq server daemon");

    let pid = child.id().expect("Child process has PID");

    // 2. Poll health endpoint until online (assert cold start is < 2000 ms)
    let admin_addr = format!("127.0.0.1:{}", admin_port);
    let mut online = false;

    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if let Ok(mut stream) = TcpStream::connect(&admin_addr).await {
            let req = format!(
                "GET /_oxidemq/health HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
                admin_addr
            );
            if stream.write_all(req.as_bytes()).await.is_ok() {
                let mut buf = String::new();
                if stream.read_to_string(&mut buf).await.is_ok() && buf.contains("200 OK") {
                    online = true;
                    break;
                }
            }
        }
    }
    assert!(online, "oxideMq broker failed to boot in time");

    // 3. Test Kafka TCP wire protocol socket
    let kafka_addr = format!("127.0.0.1:{}", kafka_port);
    let mut kafka_client = TcpStream::connect(&kafka_addr)
        .await
        .expect("Failed to connect to Kafka port");

    let header = RequestHeader::new(ApiKey::ApiVersions, 0, 999, Some("daemon-tester"));
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

    // 4. Verify memory RSS of running daemon (< 30 MiB)
    #[cfg(target_os = "linux")]
    {
        let statm = std::fs::read_to_string(format!("/proc/{}/statm", pid));
        if let Ok(content) = statm {
            let pages: Vec<&str> = content.split_whitespace().collect();
            if pages.len() >= 2 {
                let resident_pages: u64 = pages[1].parse().unwrap_or(0);
                let page_size_kb = 4; // 4 KB pages
                let rss_mb = (resident_pages * page_size_kb) / 1024;
                println!("Measured oxideMq daemon live RSS: {} MiB", rss_mb);
                assert!(rss_mb < 30, "Daemon RSS {} exceeds 30 MiB", rss_mb);
            }
        }
    }

    // 5. Clean termination
    let _ = child.kill().await;
}
