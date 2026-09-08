use crate::frame::WalRecord;
use crate::segment::{WalSegment, WAL_FILE_EXTENSION};
use oxidemq_core::error::Result;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use tracing::{info, warn};

/// High-watermark information for an individual stream recovered from the WAL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamWatermark {
    pub stream_id: u64,
    pub start_offset: i64,
    pub next_offset: i64,
    pub record_count: u64,
}

/// Comprehensive report generated after scanning and recovering WAL segments.
#[derive(Debug, Default, Clone)]
pub struct RecoveryReport {
    pub segment_files_scanned: usize,
    pub total_records_recovered: u64,
    pub highest_seq: u64,
    pub stream_watermarks: HashMap<u64, StreamWatermark>,
    pub truncated_bytes: u64,
}

/// WAL Recovery engine that scans, validates, and heals WAL segments after a crash.
pub struct WalRecovery;

impl WalRecovery {
    /// Scans the WAL directory, validates CRC32C of every record, truncates any partially written
    /// trailing bytes at EOF, and reconstructs stream watermarks.
    pub fn recover(wal_dir: &Path) -> Result<RecoveryReport> {
        let mut report = RecoveryReport::default();
        if !wal_dir.exists() {
            return Ok(report);
        }

        let mut segment_paths: Vec<(u64, PathBuf)> = Vec::new();
        for entry in fs::read_dir(wal_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                if let Some(filename) = path.file_name().and_then(|f| f.to_str()) {
                    if filename.ends_with(WAL_FILE_EXTENSION) {
                        if let Some(base_seq) = WalSegment::parse_base_seq_from_filename(filename) {
                            segment_paths.push((base_seq, path));
                        }
                    }
                }
            }
        }

        // Sort segments chronologically by base sequence
        segment_paths.sort_by_key(|k| k.0);
        report.segment_files_scanned = segment_paths.len();

        for (base_seq, path) in segment_paths {
            let mut segment = WalSegment::open_append(path.clone(), base_seq)?;
            let raw_data = segment.read_all()?;
            let mut cursor = 0;
            let total_len = raw_data.len();

            while cursor < total_len {
                let remaining = &raw_data[cursor..];
                match WalRecord::decode(remaining) {
                    Ok(Some((record, bytes_consumed))) => {
                        cursor += bytes_consumed;
                        report.total_records_recovered += 1;
                        if record.seq > report.highest_seq {
                            report.highest_seq = record.seq;
                        }

                        let wm = report.stream_watermarks.entry(record.stream_id).or_insert(
                            StreamWatermark {
                                stream_id: record.stream_id,
                                start_offset: record.offset,
                                next_offset: record.offset + 1,
                                record_count: 0,
                            },
                        );

                        wm.record_count += 1;
                        if record.offset >= wm.next_offset {
                            wm.next_offset = record.offset + 1;
                        }
                    }
                    Ok(None) => {
                        // Incomplete record at the end of the segment (crash mid-write).
                        let incomplete_len = total_len - cursor;
                        warn!(
                            "Truncating incomplete trailing bytes ({}) from segment {:?}",
                            incomplete_len, path
                        );
                        segment.truncate(cursor as u64)?;
                        report.truncated_bytes += incomplete_len as u64;
                        break;
                    }
                    Err(err) => {
                        // CRC corruption detected
                        warn!(
                            "Corrupted record detected at offset {} in segment {:?}: {}. Truncating remainder.",
                            cursor, path, err
                        );
                        let corrupted_len = total_len - cursor;
                        segment.truncate(cursor as u64)?;
                        report.truncated_bytes += corrupted_len as u64;
                        break;
                    }
                }
            }
        }

        info!(
            "WAL recovery completed: {} segments scanned, {} records recovered, {} streams active, highest seq: {}",
            report.segment_files_scanned,
            report.total_records_recovered,
            report.stream_watermarks.len(),
            report.highest_seq
        );

        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::WalRecord;
    use bytes::{Bytes, BytesMut};
    use tempfile::tempdir;

    #[test]
    fn test_recovery_clean_segments() {
        let dir = tempdir().unwrap();
        let mut seg = WalSegment::create(dir.path(), 1).unwrap();

        let mut buf = BytesMut::new();
        let rec1 = WalRecord::new(1, 10, 0, Bytes::from_static(b"rec-0"));
        let rec2 = WalRecord::new(2, 10, 1, Bytes::from_static(b"rec-1"));
        let rec3 = WalRecord::new(3, 20, 0, Bytes::from_static(b"rec-s2-0"));
        rec1.encode(&mut buf);
        rec2.encode(&mut buf);
        rec3.encode(&mut buf);

        seg.append(&buf).unwrap();
        seg.sync().unwrap();
        drop(seg);

        let report = WalRecovery::recover(dir.path()).unwrap();
        assert_eq!(report.segment_files_scanned, 1);
        assert_eq!(report.total_records_recovered, 3);
        assert_eq!(report.highest_seq, 3);
        assert_eq!(report.truncated_bytes, 0);

        let s10 = report.stream_watermarks.get(&10).unwrap();
        assert_eq!(s10.start_offset, 0);
        assert_eq!(s10.next_offset, 2);
        assert_eq!(s10.record_count, 2);

        let s20 = report.stream_watermarks.get(&20).unwrap();
        assert_eq!(s20.start_offset, 0);
        assert_eq!(s20.next_offset, 1);
        assert_eq!(s20.record_count, 1);
    }

    #[test]
    fn test_recovery_with_crash_truncation() {
        let dir = tempdir().unwrap();
        let mut seg = WalSegment::create(dir.path(), 1).unwrap();

        let mut buf = BytesMut::new();
        let rec1 = WalRecord::new(1, 10, 0, Bytes::from_static(b"good-record"));
        rec1.encode(&mut buf);

        // Add 15 bytes of trailing junk simulating power-loss mid-append
        buf.extend_from_slice(b"incomplete-data");

        seg.append(&buf).unwrap();
        seg.sync().unwrap();
        drop(seg);

        let report = WalRecovery::recover(dir.path()).unwrap();
        assert_eq!(report.total_records_recovered, 1);
        assert_eq!(report.highest_seq, 1);
        assert_eq!(report.truncated_bytes, 15);
    }
}
