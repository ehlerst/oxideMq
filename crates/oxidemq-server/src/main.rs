use clap::{Parser, Subcommand};
use oxidemq_broker::chaos::{ChaosEngine, ChaosRule, FaultTarget};
use oxidemq_broker::coordinator::GroupCoordinator;
use oxidemq_broker::handler::BrokerEngine;
use oxidemq_broker::router::ClusterState;
use oxidemq_core::config::OxideConfig;
use oxidemq_s3stream::block_cache::BlockCache;
use oxidemq_s3stream::client::{MemoryObjectStorage, ObjectStorage, S3ClientStorage};
use oxidemq_s3stream::log_cache::LogCache;
use oxidemq_server::admin::{create_admin_router, AppState};
use oxidemq_server::tls::{create_tls_acceptor, TlsConfig};
use oxidemq_wal::memory::MemoryWal;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info};

#[derive(Parser, Debug)]
#[command(
    name = "oxidemq",
    author,
    version,
    about = "Diskless Apache Kafka on S3 in Pure Rust (< 5 MiB RSS, 0 JVM Overhead)"
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    #[arg(short, long, default_value_t = 9092)]
    kafka_port: u16,

    #[arg(long, default_value_t = 9093, env = "OXIDEMQ_SSL_PORT")]
    ssl_port: u16,

    #[arg(long, default_value_t = true, env = "OXIDEMQ_ENABLE_SSL", action = clap::ArgAction::Set)]
    enable_ssl: bool,

    #[arg(long, env = "OXIDEMQ_TLS_CERT")]
    tls_cert: Option<std::path::PathBuf>,

    #[arg(long, env = "OXIDEMQ_TLS_KEY")]
    tls_key: Option<std::path::PathBuf>,

    #[arg(short, long, default_value_t = 8082)]
    admin_port: u16,

    #[arg(long, default_value = "0.0.0.0")]
    host: String,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Start the oxideMq broker daemon (Kafka TCP + Admin Web Console)
    Start {
        #[arg(short, long, default_value_t = 9092)]
        kafka_port: u16,
        #[arg(long, default_value_t = 9093, env = "OXIDEMQ_SSL_PORT")]
        ssl_port: u16,
        #[arg(long, default_value_t = true, env = "OXIDEMQ_ENABLE_SSL", action = clap::ArgAction::Set)]
        enable_ssl: bool,
        #[arg(long, env = "OXIDEMQ_TLS_CERT")]
        tls_cert: Option<std::path::PathBuf>,
        #[arg(long, env = "OXIDEMQ_TLS_KEY")]
        tls_key: Option<std::path::PathBuf>,
        #[arg(short, long, default_value_t = 8082)]
        admin_port: u16,
        #[arg(long, default_value = "0.0.0.0")]
        host: String,
    },
    /// Inspect broker status and health
    Status {
        #[arg(long, default_value = "127.0.0.1:8082")]
        addr: String,
    },
    /// Export cluster state snapshot
    DumpState {
        #[arg(long, default_value = "127.0.0.1:8082")]
        addr: String,
    },
    /// Inject chaos fault rules into a running broker
    Chaos {
        #[arg(long, default_value = "127.0.0.1:8082")]
        addr: String,
        #[arg(long)]
        target: String,
        #[arg(long, default_value_t = 0)]
        latency_ms: u64,
        #[arg(long, default_value_t = 0.0)]
        error_prob: f64,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();
    run_cli_command(cli).await
}

async fn run_cli_command(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    match cli.command {
        Some(Commands::Status { addr }) => {
            let res = http_get(&addr, "/_oxidemq/status").await?;
            println!("{}", res);
        }
        Some(Commands::DumpState { addr }) => {
            let res = http_get(&addr, "/_oxidemq/state/dump").await?;
            println!("{}", res);
        }
        Some(Commands::Chaos {
            addr,
            target,
            latency_ms,
            error_prob,
        }) => {
            let target_enum = match target.to_lowercase().as_str() {
                "produce" => FaultTarget::Produce,
                "fetch" => FaultTarget::Fetch,
                "wal" => FaultTarget::Wal,
                "s3" | "s3storage" => FaultTarget::S3Storage,
                _ => {
                    eprintln!(
                        "Invalid target: {}. Must be Produce, Fetch, Wal, or S3Storage",
                        target
                    );
                    return Ok(());
                }
            };
            let rule_id = format!(
                "cli-chaos-{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis())
                    .unwrap_or(0)
            );
            let rule = ChaosRule {
                id: rule_id,
                target: target_enum,
                latency_ms,
                error_probability: error_prob,
                error_message: None,
            };
            let json_body = serde_json::to_string(&rule)?;
            let res = http_post(&addr, "/_oxidemq/chaos/rules", &json_body).await?;
            println!("{}", res);
        }
        Some(Commands::Start {
            kafka_port,
            ssl_port,
            enable_ssl,
            tls_cert,
            tls_key,
            admin_port,
            host,
        }) => {
            run_server(
                &host, kafka_port, ssl_port, enable_ssl, tls_cert, tls_key, admin_port,
            )
            .await?;
        }
        None => {
            run_server(
                &cli.host,
                cli.kafka_port,
                cli.ssl_port,
                cli.enable_ssl,
                cli.tls_cert,
                cli.tls_key,
                cli.admin_port,
            )
            .await?;
        }
    }

    Ok(())
}

async fn run_server(
    host: &str,
    kafka_port: u16,
    ssl_port: u16,
    enable_ssl: bool,
    tls_cert: Option<PathBuf>,
    tls_key: Option<PathBuf>,
    admin_port: u16,
) -> Result<(), Box<dyn std::error::Error>> {
    let config = OxideConfig::default();
    let start_time = Instant::now();

    info!("=== oxideMq (v{}) Starting ===", env!("CARGO_PKG_VERSION"));
    info!(
        "Node ID: {}, Cluster ID: {}",
        config.broker.node_id, config.broker.cluster_id
    );

    // 1. Initialize Storage, Cache, and State Layers
    let wal = Arc::new(MemoryWal::new());
    let storage: Arc<dyn ObjectStorage> = if let Ok(bucket) = std::env::var("OXIDEMQ_S3_BUCKET") {
        let endpoint = std::env::var("OXIDEMQ_S3_ENDPOINT").ok();
        let region = std::env::var("OXIDEMQ_S3_REGION").ok();
        info!(
            "Initializing S3ClientStorage backend with bucket '{}' (endpoint: {:?})",
            bucket, endpoint
        );
        Arc::new(
            S3ClientStorage::new(bucket, endpoint.as_deref(), region.as_deref())
                .expect("Failed to initialize S3ClientStorage"),
        )
    } else if let Ok(endpoint) = std::env::var("OXIDEMQ_S3_ENDPOINT") {
        info!(
            "Initializing S3ClientStorage backend with endpoint '{}'",
            endpoint
        );
        Arc::new(
            S3ClientStorage::new("oxidemq-data", Some(&endpoint), None)
                .expect("Failed to initialize S3ClientStorage"),
        )
    } else {
        info!("Using in-memory Tier 1 storage backend (MemoryObjectStorage)");
        Arc::new(MemoryObjectStorage::new())
    };
    let log_cache = Arc::new(LogCache::new(config.cache.log_cache_size_bytes));
    let block_cache = Arc::new(BlockCache::new(config.cache.block_cache_size_bytes));

    let advertised_host = if host == "0.0.0.0" {
        std::env::var("OXIDEMQ_ADVERTISED_HOST").unwrap_or_else(|_| "127.0.0.1".to_string())
    } else {
        host.to_string()
    };
    let advertised_port = std::env::var("OXIDEMQ_ADVERTISED_PORT")
        .ok()
        .and_then(|p| p.parse::<i32>().ok())
        .unwrap_or(kafka_port as i32);

    let cluster_state = Arc::new(ClusterState::new(
        config.broker.node_id,
        &advertised_host,
        advertised_port,
        config.broker.cluster_id.clone(),
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

    // 2. Start Kafka TCP Listener
    let kafka_addr: SocketAddr = format!("{}:{}", host, kafka_port).parse()?;
    let kafka_listener = TcpListener::bind(kafka_addr).await?;
    info!("Kafka Wire Protocol TCP listener ready on {}", kafka_addr);

    let engine_for_tcp = Arc::clone(&broker_engine);
    tokio::spawn(async move {
        loop {
            match kafka_listener.accept().await {
                Ok((stream, peer_addr)) => {
                    let engine = Arc::clone(&engine_for_tcp);
                    tokio::spawn(async move {
                        if let Err(e) = engine.process_connection(stream).await {
                            error!("Connection error from {}: {}", peer_addr, e);
                        }
                    });
                }
                Err(e) => {
                    error!("Kafka TCP accept error: {}", e);
                    break;
                }
            }
        }
    });

    // 2b. Start Kafka TLS/SSL Listener (Encrypted Wire-Protocol)
    if enable_ssl {
        let tls_config = TlsConfig {
            cert_path: tls_cert,
            key_path: tls_key,
            auto_generate: true,
        };
        match create_tls_acceptor(&tls_config) {
            Ok(acceptor) => {
                let ssl_addr: SocketAddr = format!("{}:{}", host, ssl_port).parse()?;
                match TcpListener::bind(ssl_addr).await {
                    Ok(ssl_listener) => {
                        info!("Kafka Wire Protocol TLS/SSL listener ready on {}", ssl_addr);
                        let engine_for_ssl = Arc::clone(&broker_engine);
                        tokio::spawn(async move {
                            loop {
                                match ssl_listener.accept().await {
                                    Ok((stream, peer_addr)) => {
                                        let engine = Arc::clone(&engine_for_ssl);
                                        let acceptor = acceptor.clone();
                                        tokio::spawn(async move {
                                            match acceptor.accept(stream).await {
                                                Ok(tls_stream) => {
                                                    if let Err(e) =
                                                        engine.process_connection(tls_stream).await
                                                    {
                                                        error!(
                                                            "TLS connection error from {}: {}",
                                                            peer_addr, e
                                                        );
                                                    }
                                                }
                                                Err(e) => {
                                                    error!(
                                                        "TLS handshake error from {}: {}",
                                                        peer_addr, e
                                                    );
                                                }
                                            }
                                        });
                                    }
                                    Err(e) => {
                                        error!("Kafka SSL TCP accept error: {}", e);
                                        break;
                                    }
                                }
                            }
                        });
                    }
                    Err(e) => {
                        error!("Failed to bind Kafka SSL listener on {}: {}", ssl_addr, e);
                    }
                }
            }
            Err(e) => {
                error!("Failed to initialize TLS acceptor: {}", e);
            }
        }
    }

    // 3. Start Axum Admin & Web Console Server
    let admin_state = AppState {
        cluster_state,
        coordinator,
        chaos,
        start_time,
    };
    let app = create_admin_router(admin_state);
    let admin_addr: SocketAddr = format!("{}:{}", host, admin_port).parse()?;
    let admin_listener = TcpListener::bind(admin_addr).await?;
    info!(
        "Admin API & Dark-Mode Web Console ready on http://{}",
        admin_addr
    );

    tokio::select! {
        res = axum::serve(admin_listener, app) => {
            if let Err(e) = res {
                error!("Admin server error: {}", e);
            }
        }
        _ = tokio::signal::ctrl_c() => {
            info!("Shutdown signal received. Shutting down gracefully...");
        }
    }

    Ok(())
}

async fn http_get(addr: &str, path: &str) -> Result<String, Box<dyn std::error::Error>> {
    let mut stream = TcpStream::connect(addr).await?;
    let req = format!(
        "GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
        path, addr
    );
    stream.write_all(req.as_bytes()).await?;

    let mut resp = String::new();
    stream.read_to_string(&mut resp).await?;

    // Extract body after \r\n\r\n
    if let Some(pos) = resp.find("\r\n\r\n") {
        Ok(resp[pos + 4..].to_string())
    } else {
        Ok(resp)
    }
}

async fn http_post(
    addr: &str,
    path: &str,
    json_body: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut stream = TcpStream::connect(addr).await?;
    let req = format!(
        "POST {} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        path,
        addr,
        json_body.len(),
        json_body
    );
    stream.write_all(req.as_bytes()).await?;

    let mut resp = String::new();
    stream.read_to_string(&mut resp).await?;

    if let Some(pos) = resp.find("\r\n\r\n") {
        Ok(resp[pos + 4..].to_string())
    } else {
        Ok(resp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cli_parsing() {
        let cli_default = Cli::try_parse_from(["oxidemq"]).unwrap();
        assert_eq!(cli_default.kafka_port, 9092);
        assert_eq!(cli_default.ssl_port, 9093);
        assert!(cli_default.enable_ssl);
        assert_eq!(cli_default.admin_port, 8082);
        assert!(cli_default.command.is_none());

        let cli_start = Cli::try_parse_from([
            "oxidemq",
            "start",
            "--kafka-port",
            "9095",
            "--ssl-port",
            "9097",
            "--enable-ssl",
            "false",
            "--admin-port",
            "9096",
            "--host",
            "127.0.0.1",
        ])
        .unwrap();
        match cli_start.command {
            Some(Commands::Start {
                kafka_port,
                ssl_port,
                enable_ssl,
                admin_port,
                host,
                ..
            }) => {
                assert_eq!(kafka_port, 9095);
                assert_eq!(ssl_port, 9097);
                assert!(!enable_ssl);
                assert_eq!(admin_port, 9096);
                assert_eq!(host, "127.0.0.1");
            }
            _ => panic!("Expected Start command"),
        }

        let cli_status =
            Cli::try_parse_from(["oxidemq", "status", "--addr", "10.0.0.1:8082"]).unwrap();
        match cli_status.command {
            Some(Commands::Status { addr }) => {
                assert_eq!(addr, "10.0.0.1:8082");
            }
            _ => panic!("Expected Status command"),
        }

        let cli_dump = Cli::try_parse_from(["oxidemq", "dump-state"]).unwrap();
        match cli_dump.command {
            Some(Commands::DumpState { addr }) => {
                assert_eq!(addr, "127.0.0.1:8082");
            }
            _ => panic!("Expected DumpState command"),
        }

        let cli_chaos = Cli::try_parse_from([
            "oxidemq",
            "chaos",
            "--target",
            "produce",
            "--latency-ms",
            "25",
            "--error-prob",
            "0.5",
        ])
        .unwrap();
        match cli_chaos.command {
            Some(Commands::Chaos {
                target,
                latency_ms,
                error_prob,
                ..
            }) => {
                assert_eq!(target, "produce");
                assert_eq!(latency_ms, 25);
                assert!((error_prob - 0.5).abs() < f64::EPSILON);
            }
            _ => panic!("Expected Chaos command"),
        }
    }

    #[tokio::test]
    async fn test_http_get_and_post() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            for _ in 0..2 {
                if let Ok((mut socket, _)) = listener.accept().await {
                    let mut buf = [0u8; 1024];
                    let n = socket.read(&mut buf).await.unwrap();
                    let req_str = String::from_utf8_lossy(&buf[..n]);
                    let resp = if req_str.starts_with("GET") {
                        "HTTP/1.1 200 OK\r\nContent-Length: 13\r\n\r\n{\"status\":\"ok\"}"
                    } else {
                        "HTTP/1.1 200 OK\r\nContent-Length: 15\r\n\r\n{\"status\":\"done\"}"
                    };
                    socket.write_all(resp.as_bytes()).await.unwrap();
                }
            }
        });

        let get_resp = http_get(&addr.to_string(), "/_oxidemq/status")
            .await
            .unwrap();
        assert_eq!(get_resp, "{\"status\":\"ok\"}");

        let post_resp = http_post(&addr.to_string(), "/_oxidemq/chaos/rules", "{}")
            .await
            .unwrap();
        assert_eq!(post_resp, "{\"status\":\"done\"}");
    }

    #[tokio::test]
    async fn test_run_cli_commands() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            loop {
                if let Ok((mut socket, _)) = listener.accept().await {
                    let mut buf = [0u8; 1024];
                    let _ = socket.read(&mut buf).await;
                    let resp = "HTTP/1.1 200 OK\r\nContent-Length: 13\r\n\r\n{\"status\":\"ok\"}";
                    let _ = socket.write_all(resp.as_bytes()).await;
                }
            }
        });

        let cli_status = Cli {
            command: Some(Commands::Status {
                addr: addr.to_string(),
            }),
            kafka_port: 9092,
            ssl_port: 9093,
            enable_ssl: false,
            tls_cert: None,
            tls_key: None,
            admin_port: 8082,
            host: "127.0.0.1".into(),
        };
        assert!(run_cli_command(cli_status).await.is_ok());

        let cli_dump = Cli {
            command: Some(Commands::DumpState {
                addr: addr.to_string(),
            }),
            kafka_port: 9092,
            ssl_port: 9093,
            enable_ssl: false,
            tls_cert: None,
            tls_key: None,
            admin_port: 8082,
            host: "127.0.0.1".into(),
        };
        assert!(run_cli_command(cli_dump).await.is_ok());

        for target in ["produce", "fetch", "wal", "s3", "invalid"] {
            let cli_chaos = Cli {
                command: Some(Commands::Chaos {
                    addr: addr.to_string(),
                    target: target.into(),
                    latency_ms: 10,
                    error_prob: 0.1,
                }),
                kafka_port: 9092,
                ssl_port: 9093,
                enable_ssl: false,
                tls_cert: None,
                tls_key: None,
                admin_port: 8082,
                host: "127.0.0.1".into(),
            };
            assert!(run_cli_command(cli_chaos).await.is_ok());
        }
    }
}
