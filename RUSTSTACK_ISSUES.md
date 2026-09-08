# 🛰️ RustStack Issue Tracker & Compatibility Matrix

> This document tracks AWS service features, edge cases, and API behaviors required by **oxideMq** that are either missing, partially implemented, or requested as enhancements in **RustStack** (`~/git/ruststack`).

---

## 📋 Integration Status Overview

| Service | Target Capability | Required for oxideMq | RustStack Support Status | Tracking / Notes |
|---|---|---|---|---|
| **S3** | `PutObject` | S3Stream batch uploads | ✅ Supported (`ruststack-s3`) | Verified handler dispatch |
| **S3** | `GetObject` with `Range` header | S3Stream historical / cold reads | ✅ Supported (`ByteRange` parser) | `Range: bytes=start-end` tested |
| **S3** | `DeleteObjects` (Multi-Object) | Compactor garbage collection | ✅ Supported (`DeleteObjects` XML) | Verified in `ruststack-s3` |
| **S3** | `ListObjectsV2` | Stream discovery & recovery | ✅ Supported | Continuation token supported |
| **S3** | `CompleteMultipartUpload` Checksums | Large compacted segment uploads | ✅ Supported (`ruststack-s3`) | Verified CRC32, CRC32C, SHA1, SHA256 & composite checksum calculations |
| **S3** | S3 Express One Zone latency tier | Ultra-low latency WAL tier | ✅ Supported (`ruststack-s3`) | Resolved in Issue #RS-001 |

---

## 🔍 Logged Issues & Feature Requests

### Issue #RS-001: S3 Express One Zone / Directory Bucket Emulation
- **Component**: `ruststack-s3`
- **Type**: Feature Request
- **Description**: S3 Express One Zone delivers single-digit millisecond latency for append logs. Adding an optional low-latency in-memory directory bucket configuration to `ruststack-s3` will allow local stress-testing of S3-based WAL offloading under extreme IOPS without simulated round-trip overhead.
- **Impact on oxideMq**: Enables local high-throughput WAL replication testing directly against emulated S3 Express One Zone directory buckets (`--x-s3` suffix, `Bucket.Type = Directory`, `CreateSession` API authentication, and `EXPRESS_ONEZONE` storage class).
- **Status**: ✅ Resolved / Supported (`ruststack-s3`)
- **Verification & Benchmarks**:
  - Integration test suite: `crates/ruststack-compat-tests/tests/test_s3_express_and_checksums_compat.rs`
  - Criterion benchmark: 4 KB WAL record Put ~36.7 µs (~27,000 ops/sec), Get ~193 ns (>5M ops/sec)

---

## ⚖️ Architectural & Performance Comparison: RustStack S3 vs. MinIO

A comparative evaluation was performed between **RustStack S3** (`~/git/ruststack`) and **MinIO** (tested on dedicated 2.5GbE benchmark node) to validate tiering backends for oxideMq S3Stream:

| Metric / Dimension | RustStack S3 (`~/git/ruststack`) | MinIO (Enterprise / Standalone Node) | oxideMq Evaluation & Impact |
|---|---|---|---|
| **Runtime Footprint (Idle RSS)** | **~2.2 MiB** | **~245 MiB** | **RustStack is >110× lighter**, perfectly matching oxideMq's sub-5 MiB broker ethos |
| **Process Cold-Start** | **~230 ms** | **~1,200 ms** | Instant container/process start enables rapid integration test cycles |
| **Licensing** | **Apache 2.0 / MIT** | **GNU AGPLv3** | MinIO copyleft AGPLv3 imposes compliance risk; RustStack is permissive |
| **S3 Express One Zone** | **✅ Supported** (Directory buckets `--x-s3`, `CreateSession`) | ❌ Unsupported | RustStack allows sub-ms directory bucket append log tiering |
| **Checksum Verification** | ✅ Full CRC32, CRC32C, SHA1, SHA256 | ✅ Full CRC32, CRC32C, SHA1, SHA256 | Complete parity on payload data integrity |
| **Byte-Range Reads** | ✅ Verified (`bytes=start-end`) | ✅ Verified | Parity on historical block and index seeks |
| **Deployment Mode** | Native Rust embedded crate or standalone daemon | External Go binary daemon | RustStack can run zero-cost in-process or via testcontainers |

### Identified Gaps & Findings
1. **Zero Blocking Gaps**: RustStack S3 satisfies all functional requirements for oxideMq S3Stream storage (`PutObject`, `GetObject` with Range headers, `ListObjectsV2`, `DeleteObjects`, and `CompleteMultipartUpload` with checksums).
2. **Recommendation**: Default to **RustStack S3** for local integration testing, CI workflows, and embedded S3 simulation due to low memory consumption (2.2 MiB vs 245 MiB) and permissive Apache-2.0 licensing. MinIO remains fully supported as a generic S3-compatible cloud target via standard S3 API compatibility.

---

## 🛠️ How to Report New Gaps
When a test or integration with `ruststack` fails due to an unsupported AWS operation or header:
1. Document the exact HTTP request, headers, and operation.
2. Note the expected AWS behavior according to official AWS S3 specifications.
3. Record the RustStack file and handler in `crates/ruststack-s3/src/handlers.rs`.
4. Provide a temporary workaround within `oxideMq` (e.g. fallback to standard API).
5. Append a new issue entry to this tracker.

