use oxidemq_core::types::TopicPartition;
use oxidemq_protocol::error_code::KafkaErrorCode;
use oxidemq_protocol::messages::encode_control_batch;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tracing::info;

use crate::router::ClusterState;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    Empty,
    Ongoing,
    PrepareCommit,
    PrepareAbort,
    CompleteCommit,
    CompleteAbort,
    Dead,
}

#[derive(Debug, Clone)]
pub struct TransactionMetadata {
    pub transactional_id: String,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub txn_timeout_ms: i32,
    pub state: TransactionState,
    pub partitions: HashSet<TopicPartition>,
    pub last_update: Instant,
}

pub struct TransactionCoordinator {
    transactions: Arc<RwLock<HashMap<String, TransactionMetadata>>>,
    next_producer_id: AtomicI64,
}

impl Default for TransactionCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

impl TransactionCoordinator {
    pub fn new() -> Self {
        Self {
            transactions: Arc::new(RwLock::new(HashMap::new())),
            next_producer_id: AtomicI64::new(1000),
        }
    }

    /// Handles Kafka `InitProducerId` requests.
    /// Allocates an idempotent producer ID or initializes/bumps transactional producer ID.
    pub fn init_producer_id(
        &self,
        transactional_id: Option<&str>,
        transaction_timeout_ms: i32,
    ) -> Result<(i64, i16), KafkaErrorCode> {
        match transactional_id {
            None => {
                // Idempotent producer without transactional ID
                let pid = self.next_producer_id.fetch_add(1, Ordering::SeqCst);
                Ok((pid, 0))
            }
            Some(tx_id) => {
                let mut txns = self.transactions.write();
                if let Some(meta) = txns.get_mut(tx_id) {
                    // Existing transactional ID: bump epoch and reset state
                    meta.producer_epoch += 1;
                    meta.partitions.clear();
                    meta.state = TransactionState::Empty;
                    meta.last_update = Instant::now();
                    info!(
                        "Re-initialized transactional ID '{}': producer_id={}, new_epoch={}",
                        tx_id, meta.producer_id, meta.producer_epoch
                    );
                    Ok((meta.producer_id, meta.producer_epoch))
                } else {
                    // New transactional ID
                    let pid = self.next_producer_id.fetch_add(1, Ordering::SeqCst);
                    let meta = TransactionMetadata {
                        transactional_id: tx_id.to_string(),
                        producer_id: pid,
                        producer_epoch: 0,
                        txn_timeout_ms: if transaction_timeout_ms <= 0 {
                            60000
                        } else {
                            transaction_timeout_ms
                        },
                        state: TransactionState::Empty,
                        partitions: HashSet::new(),
                        last_update: Instant::now(),
                    };
                    txns.insert(tx_id.to_string(), meta);
                    info!(
                        "Initialized new transactional ID '{}': producer_id={}, epoch=0",
                        tx_id, pid
                    );
                    Ok((pid, 0))
                }
            }
        }
    }

    /// Handles Kafka `AddPartitionsToTxn` requests.
    pub fn add_partitions_to_txn(
        &self,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        partitions: Vec<TopicPartition>,
    ) -> Result<(), KafkaErrorCode> {
        let mut txns = self.transactions.write();
        let meta = txns
            .get_mut(transactional_id)
            .ok_or(KafkaErrorCode::InvalidProducerIdMapping)?;

        if meta.producer_id != producer_id {
            return Err(KafkaErrorCode::InvalidProducerIdMapping);
        }

        if producer_epoch < meta.producer_epoch {
            return Err(KafkaErrorCode::ProducerFenced);
        }
        if producer_epoch > meta.producer_epoch {
            return Err(KafkaErrorCode::InvalidProducerEpoch);
        }

        meta.state = TransactionState::Ongoing;
        meta.last_update = Instant::now();
        for tp in partitions {
            meta.partitions.insert(tp);
        }

        Ok(())
    }

    /// Handles Kafka `AddOffsetsToTxn` requests.
    pub fn add_offsets_to_txn(
        &self,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        _group_id: &str,
    ) -> Result<(), KafkaErrorCode> {
        let txns = self.transactions.read();
        let meta = txns
            .get(transactional_id)
            .ok_or(KafkaErrorCode::InvalidProducerIdMapping)?;

        if meta.producer_id != producer_id {
            return Err(KafkaErrorCode::InvalidProducerIdMapping);
        }
        if producer_epoch < meta.producer_epoch {
            return Err(KafkaErrorCode::ProducerFenced);
        }
        if producer_epoch > meta.producer_epoch {
            return Err(KafkaErrorCode::InvalidProducerEpoch);
        }

        Ok(())
    }

    /// Handles Kafka `EndTxn` requests.
    /// Injects COMMIT or ABORT control batches into all registered partitions.
    pub fn end_txn(
        &self,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
        cluster_state: &ClusterState,
    ) -> Result<(), KafkaErrorCode> {
        let mut txns = self.transactions.write();
        let meta = txns
            .get_mut(transactional_id)
            .ok_or(KafkaErrorCode::InvalidProducerIdMapping)?;

        if meta.producer_id != producer_id {
            return Err(KafkaErrorCode::InvalidProducerIdMapping);
        }
        if producer_epoch < meta.producer_epoch {
            return Err(KafkaErrorCode::ProducerFenced);
        }
        if producer_epoch > meta.producer_epoch {
            return Err(KafkaErrorCode::InvalidProducerEpoch);
        }

        let partitions: Vec<TopicPartition> = meta.partitions.drain().collect();
        meta.state = if committed {
            TransactionState::PrepareCommit
        } else {
            TransactionState::PrepareAbort
        };
        meta.last_update = Instant::now();

        // Inject control batch to every partition participating in the transaction
        for tp in partitions {
            let partition = cluster_state.get_or_create_partition(&tp);
            let ctrl_batch = encode_control_batch(0, producer_id, producer_epoch, committed);
            let (base_offset, _) = partition
                .append_records(ctrl_batch)
                .map_err(|_| KafkaErrorCode::UnknownServer)?;

            if committed {
                partition.complete_txn(producer_id);
            } else {
                partition.record_aborted_txn(producer_id, base_offset);
            }
        }

        meta.state = TransactionState::Empty;
        info!(
            "Transaction '{}' (producer_id={}) {} successfully",
            transactional_id,
            producer_id,
            if committed { "COMMITTED" } else { "ABORTED" }
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxidemq_s3stream::block_cache::BlockCache;
    use oxidemq_s3stream::client::MemoryObjectStorage;
    use oxidemq_s3stream::log_cache::LogCache;
    use oxidemq_wal::memory::MemoryWal;

    fn create_test_cluster_state() -> Arc<ClusterState> {
        let wal = Arc::new(MemoryWal::new());
        let storage = Arc::new(MemoryObjectStorage::new());
        let log_cache = Arc::new(LogCache::new(1024 * 1024));
        let block_cache = Arc::new(BlockCache::new(1024 * 1024));
        Arc::new(ClusterState::new(
            0,
            "127.0.0.1",
            9092,
            "test-cluster".to_string(),
            wal,
            storage,
            log_cache,
            block_cache,
        ))
    }

    #[test]
    fn test_transaction_coordinator_lifecycle() {
        let coord = TransactionCoordinator::new();
        let cluster = create_test_cluster_state();

        // 1. Init without transactional_id (idempotent producer)
        let (idemp_pid, epoch) = coord.init_producer_id(None, 0).unwrap();
        assert!(idemp_pid >= 1000);
        assert_eq!(epoch, 0);

        // 2. Init with transactional_id
        let (pid1, ep1) = coord.init_producer_id(Some("tx-test"), 60000).unwrap();
        assert_eq!(ep1, 0);

        // 3. Add partitions to txn
        let tp = TopicPartition::new("test-topic", 0);
        assert!(coord
            .add_partitions_to_txn("tx-test", pid1, ep1, vec![tp.clone()])
            .is_ok());

        // 4. Add offsets to txn
        assert!(coord
            .add_offsets_to_txn("tx-test", pid1, ep1, "test-group")
            .is_ok());

        // 5. Commit transaction
        assert!(coord.end_txn("tx-test", pid1, ep1, true, &cluster).is_ok());

        // 6. Re-init transactional ID (bumps epoch)
        let (pid2, ep2) = coord.init_producer_id(Some("tx-test"), 60000).unwrap();
        assert_eq!(pid2, pid1);
        assert_eq!(ep2, 1);

        // 7. Fenced producer check: old epoch must be rejected
        assert_eq!(
            coord.add_partitions_to_txn("tx-test", pid1, 0, vec![tp.clone()]),
            Err(KafkaErrorCode::ProducerFenced)
        );
        assert_eq!(
            coord.end_txn("tx-test", pid1, 0, false, &cluster),
            Err(KafkaErrorCode::ProducerFenced)
        );

        // 8. Abort transaction with new epoch
        assert!(coord
            .add_partitions_to_txn("tx-test", pid1, ep2, vec![tp])
            .is_ok());
        assert!(coord.end_txn("tx-test", pid1, ep2, false, &cluster).is_ok());
    }
}
