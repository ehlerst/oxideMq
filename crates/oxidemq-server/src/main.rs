use clap::{Parser, Subcommand};
use oxidemq_core::config::OxideConfig;
use std::net::SocketAddr;
use tracing::info;

#[derive(Parser, Debug)]
#[command(
    name = "oxidemq",
    author,
    version,
    about = "Diskless Kafka on S3 in Pure Rust"
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
    /// Start the oxideMq broker daemon
    Start {
        #[arg(short, long, default_value_t = 9092)]
        kafka_port: u16,
        #[arg(short, long, default_value_t = 9093)]
        admin_port: u16,
    },
    /// Inspect broker status and health
    Status {
        #[arg(long, default_value = "http://127.0.0.1:9093")]
        endpoint: String,
    },
    /// Export cluster state snapshot
    DumpState {
        #[arg(long, default_value = "http://127.0.0.1:9093")]
        endpoint: String,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();
    let config = OxideConfig::default();

    info!(
        "Starting oxideMq node {} (cluster: {})",
        config.broker.node_id, config.broker.cluster_id
    );
    info!("Kafka wire protocol port: {}", cli.kafka_port);
    info!("Admin & Web console port: {}", cli.admin_port);

    let app = axum::Router::new()
        .route("/_oxidemq/health", axum::routing::get(|| async { "OK" }))
        .route(
            "/_oxidemq/version",
            axum::routing::get(|| async { env!("CARGO_PKG_VERSION") }),
        );

    let addr: SocketAddr = format!("{}:{}", cli.host, cli.admin_port).parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!("Admin server listening on http://{}", addr);

    // In background or join
    tokio::select! {
        res = axum::serve(listener, app) => {
            if let Err(e) = res {
                eprintln!("Server error: {}", e);
            }
        }
        _ = tokio::signal::ctrl_c() => {
            info!("Shutdown signal received");
        }
    }

    Ok(())
}
