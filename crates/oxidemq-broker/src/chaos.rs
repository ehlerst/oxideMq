use oxidemq_core::error::{OxideMqError, Result};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Targets where faults can be injected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FaultTarget {
    Produce,
    Fetch,
    Wal,
    S3Storage,
}

/// A configurable chaos injection rule.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChaosRule {
    pub id: String,
    pub target: FaultTarget,
    pub latency_ms: u64,
    pub error_probability: f64, // 0.0 to 1.0
    pub error_message: Option<String>,
}

/// Thread-safe chaos injection engine.
#[derive(Debug, Clone)]
pub struct ChaosEngine {
    rules: Arc<RwLock<Vec<ChaosRule>>>,
    enabled: Arc<AtomicBool>,
    injected_count: Arc<AtomicU64>,
}

impl Default for ChaosEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl ChaosEngine {
    pub fn new() -> Self {
        Self {
            rules: Arc::new(RwLock::new(Vec::new())),
            enabled: Arc::new(AtomicBool::new(true)),
            injected_count: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::SeqCst);
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    pub fn add_rule(&self, rule: ChaosRule) {
        let mut rules = self.rules.write();
        // Replace existing rule with same ID if present
        rules.retain(|r| r.id != rule.id);
        rules.push(rule);
    }

    pub fn remove_rule(&self, rule_id: &str) {
        self.rules.write().retain(|r| r.id != rule_id);
    }

    pub fn clear_rules(&self) {
        self.rules.write().clear();
    }

    pub fn list_rules(&self) -> Vec<ChaosRule> {
        self.rules.read().clone()
    }

    pub fn injected_count(&self) -> u64 {
        self.injected_count.load(Ordering::Relaxed)
    }

    /// Evaluates active rules for a given target.
    /// If a latency rule matches, returns `Some(duration)`.
    /// If an error rule triggers, returns `Err(OxideMqError)`.
    pub fn check_fault(&self, target: FaultTarget) -> Result<Option<Duration>> {
        if !self.enabled.load(Ordering::Relaxed) {
            return Ok(None);
        }

        let rules = self.rules.read();
        if rules.is_empty() {
            return Ok(None);
        }

        for rule in rules.iter() {
            if rule.target == target {
                // Check error injection
                if rule.error_probability > 0.0 {
                    let should_fail = if rule.error_probability >= 1.0 {
                        true
                    } else {
                        // Deterministic pseudo-random check without heavy rng dependency
                        let count = self.injected_count.load(Ordering::Relaxed);
                        let hash =
                            (count.wrapping_mul(6364136223846793005) % 10000) as f64 / 10000.0;
                        hash < rule.error_probability
                    };

                    if should_fail {
                        self.injected_count.fetch_add(1, Ordering::Relaxed);
                        let msg = rule
                            .error_message
                            .clone()
                            .unwrap_or_else(|| format!("Chaos injected failure for {:?}", target));
                        return Err(OxideMqError::Io(std::io::Error::other(msg)));
                    }
                }

                // Check latency injection
                if rule.latency_ms > 0 {
                    self.injected_count.fetch_add(1, Ordering::Relaxed);
                    return Ok(Some(Duration::from_millis(rule.latency_ms)));
                }
            }
        }

        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chaos_rule_lifecycle() {
        let chaos = ChaosEngine::new();
        assert_eq!(chaos.list_rules().len(), 0);

        chaos.add_rule(ChaosRule {
            id: "slow-s3".to_string(),
            target: FaultTarget::S3Storage,
            latency_ms: 50,
            error_probability: 0.0,
            error_message: None,
        });

        assert_eq!(chaos.list_rules().len(), 1);
        let fault = chaos.check_fault(FaultTarget::S3Storage).unwrap();
        assert_eq!(fault, Some(Duration::from_millis(50)));
        assert_eq!(chaos.injected_count(), 1);

        // Produce should not be affected
        let no_fault = chaos.check_fault(FaultTarget::Produce).unwrap();
        assert_eq!(no_fault, None);

        // Error injection
        chaos.add_rule(ChaosRule {
            id: "fail-wal".to_string(),
            target: FaultTarget::Wal,
            latency_ms: 0,
            error_probability: 1.0,
            error_message: Some("WAL disk full simulation".to_string()),
        });

        let err = chaos.check_fault(FaultTarget::Wal).unwrap_err();
        assert!(err.to_string().contains("WAL disk full simulation"));

        // Disable chaos
        chaos.set_enabled(false);
        assert!(!chaos.is_enabled());
        assert!(chaos.check_fault(FaultTarget::Wal).is_ok());

        chaos.set_enabled(true);
        assert!(chaos.is_enabled());

        // Remove rule
        chaos.remove_rule("fail-wal");
        assert_eq!(chaos.list_rules().len(), 1);

        // Update rule with same id
        chaos.add_rule(ChaosRule {
            id: "slow-s3".to_string(),
            target: FaultTarget::S3Storage,
            latency_ms: 100,
            error_probability: 0.5,
            error_message: None,
        });
        assert_eq!(chaos.list_rules().len(), 1);

        // Check fault with 0.5 probability (may return error or latency)
        let _ = chaos.check_fault(FaultTarget::S3Storage);

        chaos.clear_rules();
        assert_eq!(chaos.list_rules().len(), 0);

        let default_chaos = ChaosEngine::default();
        assert!(default_chaos.is_enabled());
    }
}
