use base64::prelude::*;
use oxidemq_protocol::KafkaErrorCode;
use parking_lot::RwLock;
use ring::rand::SecureRandom;
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::Arc;
use tracing::{debug, warn};

/// Supported SASL authentication mechanisms.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SaslMechanism {
    Plain,
    ScramSha256,
    ScramSha512,
}

impl SaslMechanism {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Plain => "PLAIN",
            Self::ScramSha256 => "SCRAM-SHA-256",
            Self::ScramSha512 => "SCRAM-SHA-512",
        }
    }

    pub fn from_str_case_insensitive(s: &str) -> Option<Self> {
        match s.trim().to_ascii_uppercase().as_str() {
            "PLAIN" => Some(Self::Plain),
            "SCRAM-SHA-256" => Some(Self::ScramSha256),
            "SCRAM-SHA-512" => Some(Self::ScramSha512),
            _ => None,
        }
    }
}

/// State of SASL authentication on a single TCP connection.
#[derive(Debug, Clone)]
pub enum ConnectionAuthState {
    /// Connection has not initiated SASL handshake.
    Unauthenticated,
    /// Client completed SaslHandshake with selected mechanism.
    HandshakeReceived { mechanism: SaslMechanism },
    /// First round of SCRAM challenge sent; awaiting client final message.
    ScramChallengeSent { session: ScramServerSession },
    /// Successfully authenticated with principal identity.
    Authenticated { principal: String },
    /// Authentication failed; subsequent requests will be rejected.
    Failed,
}

impl ConnectionAuthState {
    pub fn is_authenticated(&self) -> bool {
        matches!(self, Self::Authenticated { .. })
    }

    pub fn principal(&self) -> Option<&str> {
        match self {
            Self::Authenticated { principal } => Some(principal.as_str()),
            _ => None,
        }
    }
}

/// Active SCRAM challenge-response session state for a connection.
#[derive(Debug, Clone)]
pub struct ScramServerSession {
    pub mechanism: SaslMechanism,
    pub username: String,
    pub client_nonce: String,
    pub server_nonce: String,
    pub full_nonce: String,
    pub salt: Vec<u8>,
    pub iterations: u32,
    pub auth_message_prefix: String,
}

impl ScramServerSession {
    pub fn new(mechanism: SaslMechanism) -> Self {
        Self {
            mechanism,
            username: String::new(),
            client_nonce: String::new(),
            server_nonce: String::new(),
            full_nonce: String::new(),
            salt: Vec::new(),
            iterations: 4096,
            auth_message_prefix: String::new(),
        }
    }

    /// Processes client-first-message (RFC 5802 Section 3).
    /// Returns the server-first-message to be sent back to client.
    pub fn process_client_first(
        &mut self,
        auth_bytes: &[u8],
        salt_override: Option<Vec<u8>>,
        server_nonce_override: Option<&str>,
    ) -> Result<String, KafkaErrorCode> {
        let msg = std::str::from_utf8(auth_bytes).map_err(|_| {
            warn!("Invalid UTF-8 in SCRAM client-first message");
            KafkaErrorCode::SaslAuthenticationFailed
        })?;

        // Locate client-first-message-bare (starts at "n=")
        let bare_idx = msg.find("n=").ok_or_else(|| {
            warn!(
                "Missing 'n=' username attribute in SCRAM client-first message: {}",
                msg
            );
            KafkaErrorCode::SaslAuthenticationFailed
        })?;
        let client_first_bare = &msg[bare_idx..];

        // Parse attributes from client-first-bare
        let mut username = None;
        let mut client_nonce = None;

        for part in client_first_bare.split(',') {
            if let Some(val) = part.strip_prefix("n=") {
                username = Some(val.to_string());
            } else if let Some(val) = part.strip_prefix("r=") {
                client_nonce = Some(val.to_string());
            }
        }

        let username = username.ok_or(KafkaErrorCode::SaslAuthenticationFailed)?;
        let client_nonce = client_nonce.ok_or(KafkaErrorCode::SaslAuthenticationFailed)?;

        // Generate server nonce
        let server_nonce = match server_nonce_override {
            Some(s) => s.to_string(),
            None => {
                let rng = ring::rand::SystemRandom::new();
                let mut nonce_bytes = [0u8; 16];
                rng.fill(&mut nonce_bytes)
                    .map_err(|_| KafkaErrorCode::UnknownServer)?;
                BASE64_STANDARD.encode(nonce_bytes)
            }
        };

        // Generate or use salt
        let salt = match salt_override {
            Some(s) => s,
            None => {
                let rng = ring::rand::SystemRandom::new();
                let mut salt_bytes = [0u8; 16];
                rng.fill(&mut salt_bytes)
                    .map_err(|_| KafkaErrorCode::UnknownServer)?;
                salt_bytes.to_vec()
            }
        };

        let full_nonce = format!("{}{}", client_nonce, server_nonce);
        let salt_b64 = BASE64_STANDARD.encode(&salt);
        let iterations = 4096;

        let server_first_message = format!("r={},s={},i={}", full_nonce, salt_b64, iterations);

        self.username = username;
        self.client_nonce = client_nonce;
        self.server_nonce = server_nonce;
        self.full_nonce = full_nonce;
        self.salt = salt;
        self.iterations = iterations;
        self.auth_message_prefix = format!("{},{}", client_first_bare, server_first_message);

        Ok(server_first_message)
    }

    /// Processes client-final-message (RFC 5802 Section 3).
    /// Returns (authenticated_username, server-final-message) on success.
    pub fn process_client_final(
        &self,
        auth_bytes: &[u8],
        password: &str,
    ) -> Result<(String, String), KafkaErrorCode> {
        let msg = std::str::from_utf8(auth_bytes).map_err(|_| {
            warn!("Invalid UTF-8 in SCRAM client-final message");
            KafkaErrorCode::SaslAuthenticationFailed
        })?;

        // Locate ",p=" proof attribute
        let proof_idx = msg.find(",p=").ok_or_else(|| {
            warn!(
                "Missing ',p=' proof attribute in SCRAM client-final message: {}",
                msg
            );
            KafkaErrorCode::SaslAuthenticationFailed
        })?;

        let client_final_without_proof = &msg[..proof_idx];
        let client_proof_b64 = &msg[proof_idx + 3..];

        // Verify nonce matches
        let mut nonce_match = false;
        for part in client_final_without_proof.split(',') {
            if let Some(val) = part.strip_prefix("r=") {
                if val == self.full_nonce {
                    nonce_match = true;
                }
            }
        }
        if !nonce_match {
            warn!(
                "Nonce mismatch in SCRAM client-final message (expected {}, got {})",
                self.full_nonce, client_final_without_proof
            );
            return Err(KafkaErrorCode::SaslAuthenticationFailed);
        }

        let client_proof = BASE64_STANDARD
            .decode(client_proof_b64.as_bytes())
            .map_err(|_| KafkaErrorCode::SaslAuthenticationFailed)?;

        let auth_message = format!(
            "{},{}",
            self.auth_message_prefix, client_final_without_proof
        );

        match self.mechanism {
            SaslMechanism::ScramSha256 => {
                let mut salted_password = [0u8; 32];
                ring::pbkdf2::derive(
                    ring::pbkdf2::PBKDF2_HMAC_SHA256,
                    NonZeroU32::new(self.iterations).unwrap(),
                    &self.salt,
                    password.as_bytes(),
                    &mut salted_password,
                );

                let s_key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, &salted_password);
                let client_key = ring::hmac::sign(&s_key, b"Client Key");
                let stored_key = ring::digest::digest(&ring::digest::SHA256, client_key.as_ref());
                let stored_key_hmac =
                    ring::hmac::Key::new(ring::hmac::HMAC_SHA256, stored_key.as_ref());
                let client_sig = ring::hmac::sign(&stored_key_hmac, auth_message.as_bytes());

                if client_proof.len() != client_key.as_ref().len() {
                    return Err(KafkaErrorCode::SaslAuthenticationFailed);
                }

                let recovered_client_key: Vec<u8> = client_proof
                    .iter()
                    .zip(client_sig.as_ref().iter())
                    .map(|(p, s)| p ^ s)
                    .collect();

                let cand_stored_key =
                    ring::digest::digest(&ring::digest::SHA256, &recovered_client_key);
                if cand_stored_key.as_ref() != stored_key.as_ref() {
                    warn!(
                        "Client proof validation failed for user '{}'",
                        self.username
                    );
                    return Err(KafkaErrorCode::SaslAuthenticationFailed);
                }

                let server_key = ring::hmac::sign(&s_key, b"Server Key");
                let server_key_hmac =
                    ring::hmac::Key::new(ring::hmac::HMAC_SHA256, server_key.as_ref());
                let server_sig = ring::hmac::sign(&server_key_hmac, auth_message.as_bytes());
                let server_final = format!("v={}", BASE64_STANDARD.encode(server_sig.as_ref()));

                Ok((self.username.clone(), server_final))
            }
            SaslMechanism::ScramSha512 => {
                let mut salted_password = [0u8; 64];
                ring::pbkdf2::derive(
                    ring::pbkdf2::PBKDF2_HMAC_SHA512,
                    NonZeroU32::new(self.iterations).unwrap(),
                    &self.salt,
                    password.as_bytes(),
                    &mut salted_password,
                );

                let s_key = ring::hmac::Key::new(ring::hmac::HMAC_SHA512, &salted_password);
                let client_key = ring::hmac::sign(&s_key, b"Client Key");
                let stored_key = ring::digest::digest(&ring::digest::SHA512, client_key.as_ref());
                let stored_key_hmac =
                    ring::hmac::Key::new(ring::hmac::HMAC_SHA512, stored_key.as_ref());
                let client_sig = ring::hmac::sign(&stored_key_hmac, auth_message.as_bytes());

                if client_proof.len() != client_key.as_ref().len() {
                    return Err(KafkaErrorCode::SaslAuthenticationFailed);
                }

                let recovered_client_key: Vec<u8> = client_proof
                    .iter()
                    .zip(client_sig.as_ref().iter())
                    .map(|(p, s)| p ^ s)
                    .collect();

                let cand_stored_key =
                    ring::digest::digest(&ring::digest::SHA512, &recovered_client_key);
                if cand_stored_key.as_ref() != stored_key.as_ref() {
                    warn!(
                        "Client proof validation failed for user '{}'",
                        self.username
                    );
                    return Err(KafkaErrorCode::SaslAuthenticationFailed);
                }

                let server_key = ring::hmac::sign(&s_key, b"Server Key");
                let server_key_hmac =
                    ring::hmac::Key::new(ring::hmac::HMAC_SHA512, server_key.as_ref());
                let server_sig = ring::hmac::sign(&server_key_hmac, auth_message.as_bytes());
                let server_final = format!("v={}", BASE64_STANDARD.encode(server_sig.as_ref()));

                Ok((self.username.clone(), server_final))
            }
            SaslMechanism::Plain => Err(KafkaErrorCode::UnsupportedSaslMechanism),
        }
    }
}

/// Central SASL Authenticator managing user credentials and mechanism validation.
#[derive(Debug, Clone)]
pub struct SaslAuthenticator {
    users: Arc<RwLock<HashMap<String, String>>>,
    enabled_mechanisms: Vec<SaslMechanism>,
    require_sasl: bool,
}

impl Default for SaslAuthenticator {
    fn default() -> Self {
        Self::new(
            vec![
                SaslMechanism::Plain,
                SaslMechanism::ScramSha256,
                SaslMechanism::ScramSha512,
            ],
            false,
        )
    }
}

impl SaslAuthenticator {
    pub fn new(enabled_mechanisms: Vec<SaslMechanism>, require_sasl: bool) -> Self {
        Self {
            users: Arc::new(RwLock::new(HashMap::new())),
            enabled_mechanisms,
            require_sasl,
        }
    }

    /// Adds a username/password credential pair to the authenticator.
    pub fn add_user(&self, username: impl Into<String>, password: impl Into<String>) {
        let u = username.into();
        let p = password.into();
        self.users.write().insert(u, p);
    }

    /// Adds multiple credentials formatted as "user=pass,admin=secret".
    pub fn load_user_list(&self, spec: &str) {
        for entry in spec.split(',') {
            let entry = entry.trim();
            if let Some((user, pass)) = entry.split_once('=') {
                self.add_user(user.trim(), pass.trim());
            }
        }
    }

    pub fn get_password(&self, username: &str) -> Option<String> {
        self.users.read().get(username).cloned()
    }

    pub fn is_mechanism_enabled(&self, mech: SaslMechanism) -> bool {
        self.enabled_mechanisms.contains(&mech)
    }

    pub fn enabled_mechanisms(&self) -> &[SaslMechanism] {
        &self.enabled_mechanisms
    }

    pub fn enabled_mechanism_names(&self) -> Vec<String> {
        self.enabled_mechanisms
            .iter()
            .map(|m| m.as_str().to_string())
            .collect()
    }

    pub fn is_sasl_required(&self) -> bool {
        self.require_sasl
    }

    /// Authenticates a SASL PLAIN request payload: `[authzid] \0 authcid \0 password` (RFC 4616).
    pub fn authenticate_plain(&self, auth_bytes: &[u8]) -> Result<String, KafkaErrorCode> {
        let parts: Vec<&[u8]> = auth_bytes.split(|&b| b == 0).collect();
        if parts.len() != 3 {
            warn!(
                "Invalid SASL PLAIN payload structure (expected 3 null-separated parts, found {})",
                parts.len()
            );
            return Err(KafkaErrorCode::SaslAuthenticationFailed);
        }

        let username = std::str::from_utf8(parts[1]).map_err(|_| {
            warn!("Invalid UTF-8 in SASL PLAIN username");
            KafkaErrorCode::SaslAuthenticationFailed
        })?;

        let password = std::str::from_utf8(parts[2]).map_err(|_| {
            warn!("Invalid UTF-8 in SASL PLAIN password");
            KafkaErrorCode::SaslAuthenticationFailed
        })?;

        let expected_password = self.get_password(username).ok_or_else(|| {
            warn!("Unknown SASL PLAIN user '{}'", username);
            KafkaErrorCode::SaslAuthenticationFailed
        })?;

        if expected_password != password {
            warn!("Invalid password for SASL PLAIN user '{}'", username);
            return Err(KafkaErrorCode::SaslAuthenticationFailed);
        }

        debug!("Successfully authenticated SASL PLAIN user '{}'", username);
        Ok(username.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sasl_mechanism_parsing() {
        assert_eq!(
            SaslMechanism::from_str_case_insensitive("plain"),
            Some(SaslMechanism::Plain)
        );
        assert_eq!(
            SaslMechanism::from_str_case_insensitive("SCRAM-SHA-256"),
            Some(SaslMechanism::ScramSha256)
        );
        assert_eq!(
            SaslMechanism::from_str_case_insensitive("scram-sha-512"),
            Some(SaslMechanism::ScramSha512)
        );
        assert_eq!(SaslMechanism::from_str_case_insensitive("GSSAPI"), None);
    }

    #[test]
    fn test_sasl_plain_authentication() {
        let auth = SaslAuthenticator::default();
        auth.add_user("alice", "secret123");
        auth.add_user("admin", "super-admin-pass");

        // Format: [authzid]\0username\0password
        let valid_payload = b"\0alice\0secret123";
        let user = auth.authenticate_plain(valid_payload).unwrap();
        assert_eq!(user, "alice");

        let valid_with_authzid = b"alice-authzid\0alice\0secret123";
        let user2 = auth.authenticate_plain(valid_with_authzid).unwrap();
        assert_eq!(user2, "alice");

        // Wrong password
        let wrong_pass = b"\0alice\0wrongpass";
        assert_eq!(
            auth.authenticate_plain(wrong_pass),
            Err(KafkaErrorCode::SaslAuthenticationFailed)
        );

        // Unknown user
        let unknown_user = b"\0bob\0secret123";
        assert_eq!(
            auth.authenticate_plain(unknown_user),
            Err(KafkaErrorCode::SaslAuthenticationFailed)
        );

        // Malformed payload
        let malformed = b"not-delimited-payload";
        assert_eq!(
            auth.authenticate_plain(malformed),
            Err(KafkaErrorCode::SaslAuthenticationFailed)
        );
    }

    #[test]
    fn test_scram_sha256_roundtrip() {
        let mut session = ScramServerSession::new(SaslMechanism::ScramSha256);

        let client_first = b"n,,n=alice,r=clientNonceAbc123";
        let server_first = session
            .process_client_first(client_first, None, None)
            .unwrap();
        assert!(server_first.starts_with("r=clientNonceAbc123"));
        assert!(server_first.contains(",s="));
        assert!(server_first.contains(",i=4096"));

        // Extract full nonce and salt for client side proof calculation
        let full_nonce = session.full_nonce.clone();
        let salt = session.salt.clone();
        let auth_message_prefix = session.auth_message_prefix.clone();

        let client_final_without_proof = format!("c=biws,r={}", full_nonce);
        let auth_message = format!("{},{}", auth_message_prefix, client_final_without_proof);

        let mut salted_password = [0u8; 32];
        ring::pbkdf2::derive(
            ring::pbkdf2::PBKDF2_HMAC_SHA256,
            NonZeroU32::new(4096).unwrap(),
            &salt,
            b"pencil-password",
            &mut salted_password,
        );

        let s_key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, &salted_password);
        let client_key = ring::hmac::sign(&s_key, b"Client Key");
        let stored_key = ring::digest::digest(&ring::digest::SHA256, client_key.as_ref());
        let stored_key_hmac = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, stored_key.as_ref());
        let client_sig = ring::hmac::sign(&stored_key_hmac, auth_message.as_bytes());

        let client_proof: Vec<u8> = client_key
            .as_ref()
            .iter()
            .zip(client_sig.as_ref().iter())
            .map(|(k, s)| k ^ s)
            .collect();

        let client_final = format!(
            "{},p={}",
            client_final_without_proof,
            BASE64_STANDARD.encode(&client_proof)
        );

        let (user, server_final) = session
            .process_client_final(client_final.as_bytes(), "pencil-password")
            .unwrap();

        assert_eq!(user, "alice");
        assert!(server_final.starts_with("v="));

        // Wrong password should fail
        assert_eq!(
            session.process_client_final(client_final.as_bytes(), "wrong-password"),
            Err(KafkaErrorCode::SaslAuthenticationFailed)
        );
    }

    #[test]
    fn test_scram_sha512_roundtrip() {
        let mut session = ScramServerSession::new(SaslMechanism::ScramSha512);

        let client_first = b"n,,n=bob,r=randomClientNonce1234";
        let server_first = session
            .process_client_first(client_first, None, None)
            .unwrap();
        assert!(server_first.starts_with("r=randomClientNonce1234"));
        assert!(server_first.contains(",s="));
        assert!(server_first.contains(",i=4096"));

        // Extract full nonce and salt for client side proof calculation
        let full_nonce = session.full_nonce.clone();
        let salt = session.salt.clone();
        let auth_message_prefix = session.auth_message_prefix.clone();

        let client_final_without_proof = format!("c=biws,r={}", full_nonce);
        let auth_message = format!("{},{}", auth_message_prefix, client_final_without_proof);

        let mut salted_password = [0u8; 64];
        ring::pbkdf2::derive(
            ring::pbkdf2::PBKDF2_HMAC_SHA512,
            NonZeroU32::new(4096).unwrap(),
            &salt,
            b"my-secure-password",
            &mut salted_password,
        );

        let s_key = ring::hmac::Key::new(ring::hmac::HMAC_SHA512, &salted_password);
        let client_key = ring::hmac::sign(&s_key, b"Client Key");
        let stored_key = ring::digest::digest(&ring::digest::SHA512, client_key.as_ref());
        let stored_key_hmac = ring::hmac::Key::new(ring::hmac::HMAC_SHA512, stored_key.as_ref());
        let client_sig = ring::hmac::sign(&stored_key_hmac, auth_message.as_bytes());

        let client_proof: Vec<u8> = client_key
            .as_ref()
            .iter()
            .zip(client_sig.as_ref().iter())
            .map(|(k, s)| k ^ s)
            .collect();

        let client_final = format!(
            "{},p={}",
            client_final_without_proof,
            BASE64_STANDARD.encode(&client_proof)
        );

        let (user, server_final) = session
            .process_client_final(client_final.as_bytes(), "my-secure-password")
            .unwrap();

        assert_eq!(user, "bob");
        assert!(server_final.starts_with("v="));
    }
}
