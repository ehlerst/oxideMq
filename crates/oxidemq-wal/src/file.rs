use crate::frame::WalRecord;
use crate::recovery::WalRecovery;
use crate::segment::{WalSegment, WAL_FILE_EXTENSION};
use crate::WalEngine;
use bytes::{Bytes, BytesMut};
use crossbeam_channel::{bounded, unbounded, Receiver, Sender};
use oxidemq_core::config::WalConfig;
use oxidemq_core::error::{OxideMqError, Result};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tracing::{error, info};

enum WalMessage {
    Append {
        stream_id: u64,
        offset: i64,
        data: Bytes,
        resp: Sender<Result<u64>>,
    },
    Flush {
        resp: Sender<Result<()>>,
    },
    Trim {
        _stream_id: u64,
        _up_to_offset: i64,
        resp: Sender<Result<()>>,
    },
    Shutdown,
}

/// Persistent, file-backed Write-Ahead Log engine featuring high-throughput Group Commit.
pub struct FileWal {
    dir: PathBuf,
    next_seq: Arc<AtomicU64>,
    tx: Sender<WalMessage>,
    worker_handle: Option<thread::JoinHandle<()>>,
}

impl FileWal {
    /// Returns the current monotonic sequence watermark.
    pub fn next_seq(&self) -> u64 {
        self.next_seq.load(Ordering::SeqCst)
    }
    /// Opens or initializes a `FileWal` in the specified directory using the given configuration.
    /// Performs crash-recovery before accepting append requests.
    pub fn open(config: WalConfig) -> Result<Self> {
        let dir = config.dir.clone();
        fs::create_dir_all(&dir)?;

        // Step 1: Run recovery on existing segments
        let report = WalRecovery::recover(&dir)?;
        let start_seq = report.highest_seq + 1;
        let next_seq = Arc::new(AtomicU64::new(start_seq));

        // Step 2: Open active segment
        let active_segment = Self::init_active_segment(&dir, start_seq)?;

        // Step 3: Launch Group Commit worker thread
        let (tx, rx) = unbounded();
        let worker_dir = dir.clone();
        let worker_next_seq = Arc::clone(&next_seq);

        let handle = thread::Builder::new()
            .name("wal-group-commit".to_string())
            .spawn(move || {
                Self::run_group_commit_loop(
                    worker_dir,
                    config,
                    active_segment,
                    worker_next_seq,
                    rx,
                );
            })
            .map_err(|e| OxideMqError::Storage(format!("Failed to spawn WAL worker: {}", e)))?;

        Ok(Self {
            dir,
            next_seq,
            tx,
            worker_handle: Some(handle),
        })
    }

    fn init_active_segment(dir: &Path, start_seq: u64) -> Result<WalSegment> {
        // Find existing segments
        let mut highest_seg: Option<(u64, PathBuf)> = None;
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                if let Some(filename) = path.file_name().and_then(|f| f.to_str()) {
                    if let Some(base_seq) = WalSegment::parse_base_seq_from_filename(filename) {
                        if highest_seg.as_ref().is_none_or(|(s, _)| base_seq > *s) {
                            highest_seg = Some((base_seq, path));
                        }
                    }
                }
            }
        }

        match highest_seg {
            Some((base_seq, path)) => WalSegment::open_append(path, base_seq),
            None => WalSegment::create(dir, start_seq),
        }
    }

    fn run_group_commit_loop(
        dir: PathBuf,
        config: WalConfig,
        mut segment: WalSegment,
        next_seq: Arc<AtomicU64>,
        rx: Receiver<WalMessage>,
    ) {
        let max_segment_size = config.max_segment_size_bytes;
        let sync_to_disk = config.sync_to_disk;
        let max_batch_size = config.max_batch_records;
        let commit_window = Duration::from_micros(config.group_commit_window_micros);

        let mut encode_buffer = BytesMut::with_capacity(128 * 1024);

        while let Ok(first_msg) = rx.recv() {
            match first_msg {
                WalMessage::Shutdown => break,
                WalMessage::Flush { resp } => {
                    let res = segment.sync();
                    let _ = resp.send(res);
                }
                WalMessage::Trim {
                    _stream_id: _,
                    _up_to_offset: _,
                    resp,
                } => {
                    // For now acknowledge trim. Segment deletion is managed by compactor.
                    let _ = resp.send(Ok(()));
                }
                WalMessage::Append {
                    stream_id,
                    offset,
                    data,
                    resp,
                } => {
                    let mut batch = Vec::with_capacity(max_batch_size.min(1024));
                    batch.push((stream_id, offset, data, resp));

                    // Non-blocking drain up to max_batch_size
                    while batch.len() < max_batch_size {
                        match rx.try_recv() {
                            Ok(WalMessage::Append {
                                stream_id,
                                offset,
                                data,
                                resp,
                            }) => {
                                batch.push((stream_id, offset, data, resp));
                            }
                            Ok(WalMessage::Flush { resp }) => {
                                let _ = resp.send(segment.sync());
                            }
                            Ok(WalMessage::Trim { resp, .. }) => {
                                let _ = resp.send(Ok(()));
                            }
                            Ok(WalMessage::Shutdown) => {
                                // Flush before exit
                                let _ = segment.sync();
                                return;
                            }
                            Err(_) => break,
                        }
                    }

                    // Optional short group commit window if batch is small
                    if batch.len() < 16 && !commit_window.is_zero() {
                        thread::sleep(commit_window);
                        while batch.len() < max_batch_size {
                            match rx.try_recv() {
                                Ok(WalMessage::Append {
                                    stream_id,
                                    offset,
                                    data,
                                    resp,
                                }) => {
                                    batch.push((stream_id, offset, data, resp));
                                }
                                Ok(msg) => {
                                    // Handle non-append immediately
                                    match msg {
                                        WalMessage::Flush { resp } => {
                                            let _ = resp.send(segment.sync());
                                        }
                                        WalMessage::Trim { resp, .. } => {
                                            let _ = resp.send(Ok(()));
                                        }
                                        WalMessage::Shutdown => return,
                                        _ => {}
                                    }
                                }
                                Err(_) => break,
                            }
                        }
                    }

                    // Encode all records in the batch contiguously
                    encode_buffer.clear();
                    let mut pending_responses = Vec::with_capacity(batch.len());

                    for (stream_id, offset, payload, resp) in batch {
                        let seq = next_seq.fetch_add(1, Ordering::SeqCst);
                        let record = WalRecord::new(seq, stream_id, offset, payload);
                        record.encode(&mut encode_buffer);
                        pending_responses.push((resp, seq));
                    }

                    // Append contiguous buffer to active segment
                    let append_res = segment.append(&encode_buffer);
                    let sync_res = if append_res.is_ok() && sync_to_disk {
                        segment.sync()
                    } else {
                        Ok(())
                    };

                    let final_err = append_res.err().or_else(|| sync_res.err());

                    match final_err {
                        None => {
                            for (resp, seq) in pending_responses {
                                let _ = resp.send(Ok(seq));
                            }
                        }
                        Some(e) => {
                            error!("WAL append failed during group commit: {}", e);
                            let err_msg = e.to_string();
                            for (resp, _) in pending_responses {
                                let _ = resp.send(Err(OxideMqError::Storage(err_msg.clone())));
                            }
                        }
                    }

                    // Check if segment rotation is required
                    if segment.size >= max_segment_size {
                        let new_base_seq = next_seq.load(Ordering::SeqCst);
                        match WalSegment::create(&dir, new_base_seq) {
                            Ok(new_segment) => {
                                info!(
                                    "Rolled WAL segment to new base sequence {:020}",
                                    new_base_seq
                                );
                                segment = new_segment;
                            }
                            Err(err) => {
                                error!("Failed to roll WAL segment: {}", err);
                            }
                        }
                    }
                }
            }
        }

        let _ = segment.sync();
    }

    /// Read records for a given stream starting from `start_offset` up to `max_bytes` from disk segments.
    pub fn read_stream(
        &self,
        stream_id: u64,
        start_offset: i64,
        max_bytes: usize,
    ) -> Result<Vec<WalRecord>> {
        let mut segment_paths: Vec<(u64, PathBuf)> = Vec::new();
        for entry in fs::read_dir(&self.dir)? {
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
        segment_paths.sort_by_key(|k| k.0);

        let mut matching_records = Vec::new();
        let mut total_bytes = 0;

        for (base_seq, path) in segment_paths {
            let mut segment = WalSegment::open_append(path, base_seq)?;
            let raw_data = segment.read_all()?;
            let mut cursor = 0;

            while cursor < raw_data.len() {
                if let Ok(Some((rec, consumed))) = WalRecord::decode(&raw_data[cursor..]) {
                    cursor += consumed;
                    if rec.stream_id == stream_id && rec.offset >= start_offset {
                        total_bytes += rec.payload.len();
                        matching_records.push(rec);
                        if total_bytes >= max_bytes {
                            return Ok(matching_records);
                        }
                    }
                } else {
                    break;
                }
            }
        }

        Ok(matching_records)
    }

    /// Truncate or purge segments older than `min_seq`.
    pub fn purge_segments_before(&self, min_seq: u64) -> Result<usize> {
        let mut purged = 0;
        for entry in fs::read_dir(&self.dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                if let Some(filename) = path.file_name().and_then(|f| f.to_str()) {
                    if let Some(base_seq) = WalSegment::parse_base_seq_from_filename(filename) {
                        if base_seq < min_seq {
                            fs::remove_file(&path)?;
                            purged += 1;
                        }
                    }
                }
            }
        }
        Ok(purged)
    }
}

impl WalEngine for FileWal {
    fn append(&self, stream_id: u64, offset: i64, data: &[u8]) -> Result<u64> {
        let (resp_tx, resp_rx) = bounded(1);
        self.tx
            .send(WalMessage::Append {
                stream_id,
                offset,
                data: Bytes::copy_from_slice(data),
                resp: resp_tx,
            })
            .map_err(|_| OxideMqError::Storage("WAL worker channel disconnected".to_string()))?;

        resp_rx
            .recv()
            .map_err(|_| OxideMqError::Storage("WAL worker dropped response channel".to_string()))?
    }

    fn flush(&self) -> Result<()> {
        let (resp_tx, resp_rx) = bounded(1);
        self.tx
            .send(WalMessage::Flush { resp: resp_tx })
            .map_err(|_| OxideMqError::Storage("WAL worker channel disconnected".to_string()))?;

        resp_rx
            .recv()
            .map_err(|_| OxideMqError::Storage("WAL worker dropped response channel".to_string()))?
    }

    fn trim(&self, stream_id: u64, up_to_offset: i64) -> Result<()> {
        let (resp_tx, resp_rx) = bounded(1);
        self.tx
            .send(WalMessage::Trim {
                _stream_id: stream_id,
                _up_to_offset: up_to_offset,
                resp: resp_tx,
            })
            .map_err(|_| OxideMqError::Storage("WAL worker channel disconnected".to_string()))?;

        resp_rx
            .recv()
            .map_err(|_| OxideMqError::Storage("WAL worker dropped response channel".to_string()))?
    }
}

impl Drop for FileWal {
    fn drop(&mut self) {
        let _ = self.tx.send(WalMessage::Shutdown);
        if let Some(handle) = self.worker_handle.take() {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_file_wal_append_and_recovery() {
        let dir = tempdir().unwrap();
        let config = WalConfig {
            dir: dir.path().to_path_buf(),
            max_segment_size_bytes: 1024 * 1024,
            group_commit_window_micros: 0,
            max_batch_records: 100,
            sync_to_disk: true,
            direct_io: false,
        };

        // Write records
        {
            let wal = FileWal::open(config.clone()).unwrap();
            let s1 = wal.append(1, 0, b"message-0").unwrap();
            let s2 = wal.append(1, 1, b"message-1").unwrap();
            let s3 = wal.append(2, 0, b"other-stream-0").unwrap();

            assert_eq!(s1, 1);
            assert_eq!(s2, 2);
            assert_eq!(s3, 3);
            wal.flush().unwrap();

            let recs = wal.read_stream(1, 0, 1024).unwrap();
            assert_eq!(recs.len(), 2);
            assert_eq!(recs[0].payload.as_ref(), b"message-0");
            assert_eq!(recs[1].payload.as_ref(), b"message-1");
        }

        // Re-open WAL (crash / restart simulation)
        {
            let wal = FileWal::open(config).unwrap();
            let recs = wal.read_stream(1, 0, 1024).unwrap();
            assert_eq!(recs.len(), 2);

            let s4 = wal.append(1, 2, b"message-2").unwrap();
            assert_eq!(s4, 4);
        }
    }
}
