use criterion::{black_box, criterion_group, criterion_main, Criterion};
use oxidemq_broker::chaos::{ChaosEngine, ChaosRule, FaultTarget};
use oxidemq_broker::coordinator::GroupCoordinator;
use oxidemq_broker::router::ClusterState;
use oxidemq_broker::state::ClusterStateSnapshot;
use oxidemq_core::types::TopicPartition;
use oxidemq_s3stream::block_cache::BlockCache;
use oxidemq_s3stream::client::MemoryObjectStorage;
use oxidemq_s3stream::log_cache::LogCache;
use oxidemq_wal::memory::MemoryWal;
use std::sync::Arc;

fn bench_chaos_evaluation(c: &mut Criterion) {
    let mut group = c.benchmark_group("phase5_chaos_evaluation");

    // 1. Fast path: Empty rules
    let chaos_empty = ChaosEngine::new();
    group.bench_function("empty_rules_fast_path", |b| {
        b.iter(|| {
            let res = chaos_empty.check_fault(black_box(FaultTarget::Produce));
            black_box(res).unwrap();
        });
    });

    // 2. Active rule matching check
    let chaos_active = ChaosEngine::new();
    chaos_active.add_rule(ChaosRule {
        id: "bench-latency".to_string(),
        target: FaultTarget::Produce,
        latency_ms: 10,
        error_probability: 0.0,
        error_message: None,
    });

    group.bench_function("active_rule_match_overhead", |b| {
        b.iter(|| {
            let res = chaos_active.check_fault(black_box(FaultTarget::Produce));
            black_box(res).unwrap();
        });
    });

    // 3. Active rule non-matching target
    group.bench_function("active_rule_non_matching_target", |b| {
        b.iter(|| {
            let res = chaos_active.check_fault(black_box(FaultTarget::Fetch));
            black_box(res).unwrap();
        });
    });

    group.finish();
}

fn bench_state_snapshot(c: &mut Criterion) {
    let mut group = c.benchmark_group("phase5_state_snapshot");

    let wal = Arc::new(MemoryWal::new());
    let storage = Arc::new(MemoryObjectStorage::new());
    let log_cache = Arc::new(LogCache::new(64 * 1024 * 1024));
    let block_cache = Arc::new(BlockCache::new(64 * 1024 * 1024));
    let cluster = Arc::new(ClusterState::new(
        1,
        "127.0.0.1",
        9092,
        "bench-cluster",
        wal,
        storage,
        log_cache,
        block_cache,
    ));
    let coordinator = Arc::new(GroupCoordinator::new());

    // Populate 50 partitions
    for i in 0..50 {
        let tp = TopicPartition::new("telemetry", i);
        let _ = cluster.get_or_create_partition(&tp);
        coordinator.commit_offset("group-1", tp, (i * 100) as i64);
    }

    group.bench_function("capture_50_partitions", |b| {
        b.iter(|| {
            let snap = ClusterStateSnapshot::capture(black_box(&cluster), black_box(&coordinator));
            black_box(snap);
        });
    });

    let snapshot = ClusterStateSnapshot::capture(&cluster, &coordinator);
    group.bench_function("apply_restore_50_partitions", |b| {
        b.iter(|| {
            snapshot
                .apply(black_box(&cluster), black_box(&coordinator))
                .unwrap();
        });
    });

    group.finish();
}

criterion_group!(benches, bench_chaos_evaluation, bench_state_snapshot);
criterion_main!(benches);
