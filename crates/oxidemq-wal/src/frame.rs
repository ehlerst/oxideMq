use bytes::{Buf, BufMut, Bytes, BytesMut};
use oxidemq_core::bytes_util::compute_crc32c;
use oxidemq_core::error::{OxideMqError, Result};

pub const WAL_MAGIC: u32 = 0x5F57414C; // "_WAL"
pub const HEADER_SIZE: usize = 36; // 4 (magic) + 4 (crc) + 8 (seq) + 8 (stream_id) + 8 (offset) + 4 (len)

/// An individual framed record stored within the Write-Ahead Log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalRecord {
    pub seq: u64,
    pub stream_id: u64,
    pub offset: i64,
    pub payload: Bytes,
}

impl WalRecord {
    pub fn new(seq: u64, stream_id: u64, offset: i64, payload: Bytes) -> Self {
        Self {
            seq,
            stream_id,
            offset,
            payload,
        }
    }

    /// Serializes the WAL record into the provided buffer with magic and CRC32C checksum.
    pub fn encode(&self, dst: &mut BytesMut) {
        let payload_len = self.payload.len() as u32;
        let total_len = HEADER_SIZE + self.payload.len();
        dst.reserve(total_len);

        // Pre-allocate space for header
        dst.put_u32(WAL_MAGIC);

        // Placeholder for CRC32C (will compute over seq..payload)
        let crc_pos = dst.len();
        dst.put_u32(0);

        let body_start = dst.len();
        dst.put_u64(self.seq);
        dst.put_u64(self.stream_id);
        dst.put_i64(self.offset);
        dst.put_u32(payload_len);
        dst.put_slice(&self.payload);

        // Calculate CRC32C from body_start to the end of payload
        let crc = compute_crc32c(&dst[body_start..]);

        // Overwrite CRC32C placeholder
        let crc_bytes = crc.to_be_bytes();
        dst[crc_pos..crc_pos + 4].copy_from_slice(&crc_bytes);
    }

    /// Attempts to decode a single `WalRecord` from the given byte slice.
    /// Returns `Ok(Some((record, bytes_consumed)))` if a complete valid record was parsed.
    /// Returns `Ok(None)` if more data is needed (partial record at EOF).
    /// Returns `Err(OxideMqError)` if data is corrupted (invalid magic or CRC mismatch).
    pub fn decode(src: &[u8]) -> Result<Option<(Self, usize)>> {
        if src.len() < HEADER_SIZE {
            return Ok(None);
        }

        let mut cursor = src;
        let magic = cursor.get_u32();
        if magic != WAL_MAGIC {
            return Err(OxideMqError::Storage(format!(
                "Invalid WAL magic byte: expected 0x{:08X}, found 0x{:08X}",
                WAL_MAGIC, magic
            )));
        }

        let expected_crc = cursor.get_u32();
        let body_start_offset = 8; // magic (4) + crc (4)

        let seq = cursor.get_u64();
        let stream_id = cursor.get_u64();
        let offset = cursor.get_i64();
        let payload_len = cursor.get_u32() as usize;

        let total_record_len = HEADER_SIZE + payload_len;
        if src.len() < total_record_len {
            // Incomplete record, wait for more data
            return Ok(None);
        }

        let body_bytes = &src[body_start_offset..total_record_len];
        let actual_crc = compute_crc32c(body_bytes);
        if actual_crc != expected_crc {
            return Err(OxideMqError::CorruptedRecord {
                expected_crc,
                actual_crc,
            });
        }

        let payload_slice = &src[HEADER_SIZE..total_record_len];
        let payload = Bytes::copy_from_slice(payload_slice);

        let record = Self {
            seq,
            stream_id,
            offset,
            payload,
        };

        Ok(Some((record, total_record_len)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_valid() {
        let payload = Bytes::from_static(b"hello oxideMq WAL");
        let record = WalRecord::new(42, 1001, 50, payload);

        let mut buf = BytesMut::new();
        record.encode(&mut buf);

        let (decoded, consumed) = WalRecord::decode(&buf).unwrap().expect("Record decoded");
        assert_eq!(consumed, buf.len());
        assert_eq!(decoded, record);
    }

    #[test]
    fn test_decode_incomplete() {
        let payload = Bytes::from_static(b"partial record test");
        let record = WalRecord::new(1, 2, 3, payload);

        let mut buf = BytesMut::new();
        record.encode(&mut buf);

        // Truncate by 5 bytes
        let truncated = &buf[..buf.len() - 5];
        let res = WalRecord::decode(truncated).unwrap();
        assert!(res.is_none(), "Incomplete buffer should return Ok(None)");
    }

    #[test]
    fn test_corrupted_crc() {
        let payload = Bytes::from_static(b"secure data");
        let record = WalRecord::new(1, 2, 3, payload);

        let mut buf = BytesMut::new();
        record.encode(&mut buf);

        // Corrupt a byte in the payload
        let last_idx = buf.len() - 1;
        buf[last_idx] ^= 0xFF;

        let res = WalRecord::decode(&buf);
        assert!(matches!(res, Err(OxideMqError::CorruptedRecord { .. })));
    }
}
