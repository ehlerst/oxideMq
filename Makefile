# ==============================================================================
# 🦀 oxideMq — Pure Rust Apache Kafka® & S3Stream Streaming Engine
# ==============================================================================

.PHONY: all build release run status test test-compat test-containers \
        lint fmt check-fmt bench coverage docker-build docker-run clean help

# Default target
all: build

## 🔨 Build Targets
build: ## Build debug binary across entire workspace
	cargo build --workspace

release: ## Build optimized release binary
	cargo build --release --workspace

## 🚀 Execution & Operations
run: ## Run oxideMq broker daemon locally with info logging (Kafka :9092, TLS :9093, Schema Registry :8081, Admin :8082)
	RUST_LOG=info cargo run -p oxidemq-server -- start

run-file-wal: ## Run oxideMq broker with local Write-Ahead Log (WAL) on disk
	mkdir -p ./data/wal
	OXIDEMQ_STORAGE_ENGINE=file OXIDEMQ_WAL_DIR=./data/wal RUST_LOG=info cargo run -p oxidemq-server -- start

run-s3: ## Run oxideMq broker with S3Stream storage backend
	OXIDEMQ_STORAGE_ENGINE=s3 OXIDEMQ_S3_BUCKET=oxidemq-data RUST_LOG=info cargo run -p oxidemq-server -- start

status: ## Query local broker cluster status and health via Admin API
	cargo run -p oxidemq-server -- status

dump-state: ## Dump current cluster state snapshot as JSON
	cargo run -p oxidemq-server -- dump-state

## 🧪 Testing & Verification
test: ## Run all unit and integration tests across the workspace
	cargo test --workspace --all-targets

test-compat: ## Run protocol and broker compatibility test suites
	cargo test -p oxidemq-compat-tests

test-containers: ## Run Testcontainers integration tests (requires Docker daemon)
	RUN_TESTCONTAINERS=1 cargo test -p oxidemq-compat-tests --test container_test

test-all: test test-containers ## Run full test suite including container tests

coverage: ## Generate LLVM code coverage summary (requires cargo-llvm-cov)
	cargo llvm-cov --workspace --summary-only

## 🔍 Code Quality & Formatting
fmt: ## Format all Rust code files in workspace
	cargo fmt --all

check-fmt: ## Verify code formatting compliance
	cargo fmt --all -- --check

lint: ## Run Clippy with zero-warning discipline (-D warnings)
	cargo clippy --workspace --all-targets --all-features -- -D warnings

check: check-fmt lint ## Run both formatting check and strict clippy

## ⚡ Performance Benchmarks
bench: ## Run comparative Criterion benchmarks (Phase 6)
	cargo bench -p oxidemq-benchmarks --bench phase6_comparative

bench-all: ## Run all Criterion benchmark suites (Phases 0 through 9)
	cargo bench -p oxidemq-benchmarks

bench-load: ## Run local high-throughput load benchmark (100 partitions, 8 producers)
	cargo run --release -p oxidemq-benchmarks --bin oxidemq-bench -- --partitions 100 --producers 8 --records-per-producer 10000

bench-remote: ## Run remote line-rate 2.5GbE network stress test (usage: make bench-remote TARGET=host:9092)
	cargo run --release -p oxidemq-benchmarks --bin oxidemq-bench -- --broker $${TARGET:-127.0.0.1:9092} --partitions 100 --producers 16 --records-per-producer 25000

## 🐳 Docker Targets
docker-build: ## Build local Docker container image (ehlers320/oxidemq:latest)
	mkdir -p docker-bin/amd64
	cargo build --release -p oxidemq-server
	cp target/release/oxidemq docker-bin/amd64/oxidemq
	chmod +x docker-bin/amd64/oxidemq
	docker build --build-arg TARGETARCH=amd64 -t ehlers320/oxidemq:latest .

docker-run: ## Run Docker container daemon in detached mode
	docker run -d --name oxidemq -p 9092:9092 -p 9093:9093 -p 8081:8081 -p 8082:8082 ehlers320/oxidemq:latest

docker-stop: ## Stop and remove running local Docker container
	docker rm -f oxidemq || true

## 🧹 Maintenance
clean: ## Clean cargo target directory and temporary build artifacts
	cargo clean
	rm -rf docker-bin data

## ❓ Help
help: ## Display list of available targets
	@echo "Available Makefile commands for oxideMq:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'
