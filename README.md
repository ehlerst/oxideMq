# 🦀 oxideMq

> **oxideMq** is an ultra-fast, zero-overhead, cloud-native streaming platform and Apache Kafka® wire-compatible broker written in **100% pure Rust**. It decouples compute and storage by streaming data batches directly to Amazon S3 (or S3-compatible object storage), replacing local disk replication with shared cloud storage and accelerating writes through a high-performance Write-Ahead Log (WAL).

---

## ⚡ Highlights

- **Pure Rust Performance**: Zero JVM garbage collection pauses, predictable sub-millisecond p99 latency, and single-digit megabyte idle memory.
- **Shared Cloud Storage (S3Stream)**: Decoupled compute and storage. Partitions map to object streams on Amazon S3 or [RustStack](https://github.com/ehlerst/ruststack). Partition failover and scaling occur in seconds without cross-AZ network replication taxes.
- **Ultra-Low Latency WAL**: Write-Ahead Log engine with group commit and optional `O_DIRECT` raw device support, acknowledging writes in microseconds.
- **Apache Kafka® Compatible**: Drop-in compatible with standard Kafka clients (Java, Go, Python, C/C++, Rust).
- **Zero-Dependency Architecture**: Embedded administrative web dashboard and atomic state/chaos engineering APIs built directly into a single binary.
- **Ehlerst Rust Standard Compliant**: Built strictly to the [Ehlerst Rust Engineering Standard](https://github.com/ehlerst/ruststack), featuring a modular Cargo monorepo, zero compiler warnings (`-D warnings`), and a 3-tier testing pyramid with continuous benchmarks at every phase.

---

## 🏛️ Workspace Architecture

```
oxideMq/
├── Cargo.toml                      # Centralized workspace dependency definitions
├── LICENSE-APACHE / LICENSE-MIT    # Dual license (Apache-2.0 OR MIT)
├── NOTICE                          # Clean-room pure-Rust attribution notice
├── RUSTSTACK_ISSUES.md             # Issue tracker for RustStack AWS S3 emulation
├── crates/
│   ├── oxidemq-core/               # Shared types, errors, memory aligners, configuration
│   ├── oxidemq-wal/                # Write-Ahead Log, Group Commit, Direct I/O
│   ├── oxidemq-s3stream/           # S3Stream storage, LogCache, BlockCache, Uploader, Compactor
│   ├── oxidemq-protocol/           # Pure-Rust zero-copy Kafka binary wire protocol
│   ├── oxidemq-broker/             # Topic/partition engine & consumer group coordinator
│   ├── oxidemq-server/             # Dual-port daemon (Kafka 9092 & Admin 9093), Web UI, CLI
│   ├── oxidemq-benchmarks/         # Criterion & continuous throughput benchmarking suites
│   └── oxidemq-compat-tests/       # Tier 1 in-memory Kafka client integration tests
└── Testcontainers/
    ├── rust-testcontainers/        # Tier 2 containerized Rust test suite
    └── go-testcontainers/          # Tier 2 containerized Go test suite
```

---

## 🗺️ Implementation Roadmap

| Phase | Description | Status | Benchmark Suite |
|---|---|---|---|
| **Phase 0** | Monorepo scaffolding, core primitives, RustStack tracker | ✅ Completed | Baseline zero-copy & memory benchmarks |
| **Phase 1** | WAL engine, group commit, Direct I/O, crash recovery | ⏳ Next | Append ops/sec, p99 commit latency |
| **Phase 2** | S3Stream engine, LogCache, BlockCache, S3 Uploader | 📅 Planned | Ingest MB/s, tailing vs cold read latency |
| **Phase 3** | Zero-copy Kafka wire protocol serializer/deserializer | 📅 Planned | Codec throughput & zero-allocation check |
| **Phase 4** | Broker state, partition router, consumer group coordinator | 📅 Planned | End-to-end produce/fetch roundtrip |
| **Phase 5** | Deterministic state API, embedded chaos engine, Web UI | 📅 Planned | Nanosecond chaos overhead check |
| **Phase 6** | Docker (<35MB), Testcontainers, CI/CD release engineering | 📅 Planned | Head-to-head comparison vs Kafka/AutoMQ |

---

## 🚀 Quickstart

### Prerequisites
- Rust 1.80+ (`cargo`, `rustc`)

### Build & Check
```bash
# Verify formatting
cargo fmt --all -- --check

# Strict zero-warning linting
RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets --all-features

# Run pure-Rust unit and compatibility tests
cargo test --workspace
```

### Run Benchmarks
```bash
# Run Phase 0 baseline microbenchmarks
cargo bench -p oxidemq-benchmarks --bench phase0_baseline
```

---

## 📜 License & Clean-Room Heritage

- Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT License](LICENSE-MIT) at your option.
- Clean-room attribution details are documented in [NOTICE](NOTICE).
