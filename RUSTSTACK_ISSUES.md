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
| **S3** | `CompleteMultipartUpload` Checksums | Large compacted segment uploads | ⚠️ Under Observation | Validate checksum verification |
| **S3** | S3 Express One Zone latency tier | Ultra-low latency WAL tier | 💡 Feature Request | Issue #RS-001 |

---

## 🔍 Logged Issues & Feature Requests

### Issue #RS-001: S3 Express One Zone / Directory Bucket Emulation
- **Component**: `ruststack-s3`
- **Type**: Feature Request
- **Description**: S3 Express One Zone delivers single-digit millisecond latency for append logs. Adding an optional low-latency in-memory directory bucket configuration to `ruststack-s3` will allow local stress-testing of S3-based WAL offloading under extreme IOPS without simulated round-trip overhead.
- **Impact on oxideMq**: Non-blocking; oxideMq functions with standard S3 bucket semantics and uses local NVMe/Memory WAL for sub-millisecond writes.
- **Status**: Open / Proposed

---

## 🛠️ How to Report New Gaps
When a test or integration with `ruststack` fails due to an unsupported AWS operation or header:
1. Document the exact HTTP request, headers, and operation.
2. Note the expected AWS behavior according to official AWS S3 specifications.
3. Record the RustStack file and handler in `crates/ruststack-s3/src/handlers.rs`.
4. Provide a temporary workaround within `oxideMq` (e.g. fallback to standard API).
5. Append a new issue entry to this tracker.
