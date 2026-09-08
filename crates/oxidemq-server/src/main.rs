use clap::{Parser, Subcommand};
use oxidemq_broker::chaos::{ChaosEngine, ChaosRule, FaultTarget};
use oxidemq_broker::coordinator::GroupCoordinator;
use oxidemq_broker::handler::BrokerEngine;
use oxidemq_broker::router::ClusterState;
use oxidemq_core::config::OxideConfig;
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

    #[arg(short, long, default_value_t = 9093)]
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
        #[arg(short, long, default_value_t = 9093)]
        admin_port: u16,
        #[arg(long, default_value = "0.0.0.0")]
        host: String,
    },
    /// Inspect broker status and health
    Status {
        #[arg(long, default_value = "127.0.0.1:9093")]
        addr: String,
    },
    /// Export cluster state snapshot
    DumpState {
        #[arg(long, default_value = "127.0.0.1:9093")]
        addr: String,
    },
    /// Inject chaos fault rules into a running broker
    Chaos {
        #[arg(long, default_value = "127.0.0.1:9093")]
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
            admin_port,
            host,
        }) => {
            run_server(&host, kafka_port, admin_port).await?;
        }
        None => {
            run_server(&cli.host, cli.kafka_port, cli.admin_port).await?;
        }
    }

    Ok(())
}

async fn run_server(
    host: &str,
    kafka_port: u16,
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
    let storage = Arc::new(MemoryObjectStorage::new());
    let log_cache = Arc::new(LogCache::new(config.cache.log_cache_size_bytes));
    let block_cache = Arc::new(BlockCache::new(config.cache.block_cache_size_bytes));

    let advertised_host = if host == "0.0.0.0" {
        std::env::var("OXIDEMQ_ADVERTISED_HOST").unwrap_or_else(|_| "127.0.0.1".to_string())
    } else {
        host.to_string()
    };

    let cluster_state = Arc::new(ClusterState::new(
        config.broker.node_id,
        &advertised_host,
        kafka_port as i32,
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
