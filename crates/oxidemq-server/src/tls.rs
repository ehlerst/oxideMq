use anyhow::{Context, Result};
use rcgen::generate_simple_self_signed;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::ServerConfig;
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio_rustls::TlsAcceptor;
use tracing::info;

/// TLS configuration for oxideMq encrypted Kafka listeners.
#[derive(Debug, Clone)]
pub struct TlsConfig {
    pub cert_path: Option<PathBuf>,
    pub key_path: Option<PathBuf>,
    pub auto_generate: bool,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            cert_path: None,
            key_path: None,
            auto_generate: true,
        }
    }
}

pub fn create_tls_acceptor(config: &TlsConfig) -> Result<TlsAcceptor> {
    // Install default ring crypto provider if not already set
    let _ = rustls::crypto::ring::default_provider().install_default();

    let (certs, key) =
        if let (Some(cert_file), Some(key_file)) = (&config.cert_path, &config.key_path) {
            info!("Loading TLS certificates from {:?}", cert_file);
            load_certs_and_key(cert_file, key_file)?
        } else if config.auto_generate {
            info!("Generating self-signed TLS certificates for Kafka SSL on port 9093...");
            generate_self_signed_cert()?
        } else {
            anyhow::bail!("TLS enabled but no certificates provided and auto_generate is false");
        };

    let server_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .context("Failed to configure rustls ServerConfig")?;

    Ok(TlsAcceptor::from(Arc::new(server_config)))
}

fn load_certs_and_key(
    cert_path: &Path,
    key_path: &Path,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    let cert_file = File::open(cert_path).context("Failed to open certificate file")?;
    let mut cert_reader = BufReader::new(cert_file);
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_reader)
        .collect::<Result<Vec<_>, _>>()
        .context("Failed to parse PEM certificate")?;

    let key_file = File::open(key_path).context("Failed to open private key file")?;
    let mut key_reader = BufReader::new(key_file);
    let key = rustls_pemfile::private_key(&mut key_reader)
        .context("Failed to parse private key")?
        .ok_or_else(|| anyhow::anyhow!("No private key found in PEM file"))?;

    Ok((certs, key))
}

fn generate_self_signed_cert() -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    let subject_alt_names = vec![
        "localhost".to_string(),
        "127.0.0.1".to_string(),
        "0.0.0.0".to_string(),
    ];
    let certified_key = generate_simple_self_signed(subject_alt_names)
        .context("Failed to generate self-signed certificate")?;

    let cert_der = CertificateDer::from(certified_key.cert.der().to_vec());
    let key_der = PrivateKeyDer::Pkcs8(certified_key.key_pair.serialize_der().into());

    Ok((vec![cert_der], key_der))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_self_signed_cert_generation() {
        let (certs, _key) = generate_self_signed_cert().expect("Cert generation should succeed");
        assert!(!certs.is_empty());
        let config = TlsConfig::default();
        let acceptor = create_tls_acceptor(&config);
        assert!(acceptor.is_ok());
    }

    #[tokio::test]
    async fn test_tls_broker_handshake_and_request() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        use bytes::{BufMut, Bytes, BytesMut};
        use oxidemq_broker::coordinator::GroupCoordinator;
        use oxidemq_broker::handler::BrokerEngine;
        use oxidemq_broker::router::ClusterState;
        use oxidemq_protocol::header::{RequestHeader, ResponseHeader};
        use oxidemq_protocol::ApiKey;
        use oxidemq_s3stream::block_cache::BlockCache;
        use oxidemq_s3stream::client::MemoryObjectStorage;
        use oxidemq_s3stream::log_cache::LogCache;
        use oxidemq_wal::memory::MemoryWal;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::{TcpListener, TcpStream};
        use tokio_rustls::TlsConnector;

        let (certs, key) = generate_self_signed_cert().unwrap();
        let server_config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs.clone(), key)
            .unwrap();
        let acceptor = TlsAcceptor::from(Arc::new(server_config));

        let wal = Arc::new(MemoryWal::new());
        let storage = Arc::new(MemoryObjectStorage::new());
        let log_cache = Arc::new(LogCache::new(1024 * 1024));
        let block_cache = Arc::new(BlockCache::new(1024 * 1024));
        let cluster_state = Arc::new(ClusterState::new(
            0,
            "127.0.0.1",
            9093,
            "test-cluster".to_string(),
            wal,
            storage,
            log_cache,
            block_cache,
        ));
        let coordinator = Arc::new(GroupCoordinator::new());
        let engine = Arc::new(BrokerEngine::new(cluster_state, coordinator));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let engine_clone = Arc::clone(&engine);
        let acceptor_clone = acceptor.clone();
        tokio::spawn(async move {
            if let Ok((tcp_stream, _)) = listener.accept().await {
                if let Ok(tls_stream) = acceptor_clone.accept(tcp_stream).await {
                    let _ = engine_clone.process_connection(tls_stream).await;
                }
            }
        });

        // Client configuration trusting the self-signed cert
        let mut root_store = rustls::RootCertStore::empty();
        for cert in certs {
            root_store.add(cert).unwrap();
        }
        let client_config = rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();
        let connector = TlsConnector::from(Arc::new(client_config));

        let client_tcp = TcpStream::connect(addr).await.unwrap();
        let server_name = "localhost".try_into().unwrap();
        let mut tls_client = connector.connect(server_name, client_tcp).await.unwrap();

        // Send Kafka ApiVersions request over TLS
        let req_header = RequestHeader::new(ApiKey::ApiVersions, 0, 888, Some("tls-test-client"));
        let mut req_body = BytesMut::new();
        req_header.encode(&mut req_body);
        let mut frame = BytesMut::new();
        frame.put_i32(req_body.len() as i32);
        frame.put_slice(&req_body);

        tls_client.write_all(&frame).await.unwrap();
        tls_client.flush().await.unwrap();

        // Read response over TLS
        let resp_len = tls_client.read_i32().await.unwrap() as usize;
        assert!(resp_len > 0);
        let mut resp_buf = vec![0u8; resp_len];
        tls_client.read_exact(&mut resp_buf).await.unwrap();
        let mut resp_bytes = Bytes::from(resp_buf);
        let resp_header = ResponseHeader::decode(&mut resp_bytes).unwrap();
        assert_eq!(resp_header.correlation_id, 888);
    }
}
