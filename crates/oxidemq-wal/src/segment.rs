use oxidemq_core::error::Result;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

pub const WAL_FILE_EXTENSION: &str = "wal";

/// Manages an individual contiguous WAL segment file on disk.
#[derive(Debug)]
pub struct WalSegment {
    pub base_seq: u64,
    pub path: PathBuf,
    pub file: File,
    pub size: usize,
}

impl WalSegment {
    /// Creates a new WAL segment in the specified directory.
    pub fn create(dir: &Path, base_seq: u64) -> Result<Self> {
        let filename = format!("{:020}.{}", base_seq, WAL_FILE_EXTENSION);
        let path = dir.join(filename);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;

        Ok(Self {
            base_seq,
            path,
            file,
            size: 0,
        })
    }

    /// Opens an existing WAL segment file for append operations.
    pub fn open_append(path: PathBuf, base_seq: u64) -> Result<Self> {
        let mut file = OpenOptions::new().read(true).write(true).open(&path)?;

        let size = file.seek(SeekFrom::End(0))? as usize;

        Ok(Self {
            base_seq,
            path,
            file,
            size,
        })
    }

    /// Appends pre-encoded record bytes to this segment.
    pub fn append(&mut self, data: &[u8]) -> Result<usize> {
        self.file.write_all(data)?;
        self.size += data.len();
        Ok(self.size)
    }

    /// Commits written data to non-volatile storage via `fdatasync`.
    pub fn sync(&mut self) -> Result<()> {
        self.file.sync_data()?;
        Ok(())
    }

    /// Truncates the segment to `valid_len` bytes (used during recovery if incomplete record at tail).
    pub fn truncate(&mut self, valid_len: u64) -> Result<()> {
        self.file.set_len(valid_len)?;
        self.file.seek(SeekFrom::Start(valid_len))?;
        self.size = valid_len as usize;
        self.file.sync_data()?;
        Ok(())
    }

    /// Reads the entire content of the segment into memory for recovery or inspection.
    pub fn read_all(&mut self) -> Result<Vec<u8>> {
        self.file.seek(SeekFrom::Start(0))?;
        let mut buffer = Vec::with_capacity(self.size);
        self.file.read_to_end(&mut buffer)?;
        Ok(buffer)
    }

    /// Deletes the underlying segment file from disk.
    pub fn remove(self) -> Result<()> {
        drop(self.file);
        if self.path.exists() {
            std::fs::remove_file(&self.path)?;
        }
        Ok(())
    }

    /// Extracts the base sequence number from a `.wal` filename.
    pub fn parse_base_seq_from_filename(filename: &str) -> Option<u64> {
        let stem = filename.strip_suffix(&format!(".{}", WAL_FILE_EXTENSION))?;
        stem.parse::<u64>().ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_segment_create_append_sync() {
        let dir = tempdir().unwrap();
        let mut seg = WalSegment::create(dir.path(), 100).unwrap();
        assert_eq!(seg.base_seq, 100);
        assert_eq!(seg.size, 0);

        let data = b"some-wal-payload-bytes";
        seg.append(data).unwrap();
        seg.sync().unwrap();
        assert_eq!(seg.size, data.len());

        let read_back = seg.read_all().unwrap();
        assert_eq!(read_back, data);
    }

    #[test]
    fn test_segment_truncate() {
        let dir = tempdir().unwrap();
        let mut seg = WalSegment::create(dir.path(), 0).unwrap();
        seg.append(b"abcdefghij").unwrap();
        assert_eq!(seg.size, 10);

        seg.truncate(4).unwrap();
        assert_eq!(seg.size, 4);

        let read_back = seg.read_all().unwrap();
        assert_eq!(read_back, b"abcd");
    }

    #[test]
    fn test_parse_base_seq() {
        assert_eq!(
            WalSegment::parse_base_seq_from_filename("00000000000000000100.wal"),
            Some(100)
        );
        assert_eq!(
            WalSegment::parse_base_seq_from_filename("invalid.txt"),
            None
        );
    }
}
