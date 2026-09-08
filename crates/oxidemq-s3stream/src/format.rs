use bytes::{Buf, BufMut, Bytes, BytesMut};
use oxidemq_core::bytes_util::compute_crc32c;
use oxidemq_core::error::{OxideMqError, Result};

pub const S3_OBJECT_MAGIC: u32 = 0x53335354; // "S3ST"
pub const S3_OBJECT_VERSION: u16 = 1;
pub const BLOCK_HEADER_SIZE: usize = 8 + 8 + 8 + 4 + 4 + 4; // 36 bytes

/// A contiguous block of stream records packed into an S3 object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3DataBlock {
    pub stream_id: u64,
    pub start_offset: i64,
    pub end_offset: i64,
    pub record_count: u32,
    pub data: Bytes,
}

impl S3DataBlock {
    pub fn new(
        stream_id: u64,
        start_offset: i64,
        end_offset: i64,
        record_count: u32,
        data: Bytes,
    ) -> Self {
        Self {
            stream_id,
            start_offset,
            end_offset,
            record_count,
            data,
        }
    }

    pub fn size_in_bytes(&self) -> usize {
        BLOCK_HEADER_SIZE + self.data.len()
    }
}

/// Metadata index entry pointing to a specific block within an S3 object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3BlockIndex {
    pub stream_id: u64,
    pub start_offset: i64,
    pub end_offset: i64,
    pub record_count: u32,
    pub byte_offset: u64,
    pub byte_length: u64,
}

/// Serializes and deserializes multi-stream S3 objects.
pub struct S3ObjectCodec;

impl S3ObjectCodec {
    /// Encodes a list of data blocks into a single immutable S3 object payload.
    pub fn encode(blocks: &[S3DataBlock]) -> Bytes {
        let mut buf = BytesMut::with_capacity(1024 * 1024);

        // Header: Magic (4B) + Version (2B) + Block Count (2B)
        buf.put_u32(S3_OBJECT_MAGIC);
        buf.put_u16(S3_OBJECT_VERSION);
        buf.put_u16(blocks.len() as u16);

        let mut index_entries = Vec::with_capacity(blocks.len());

        for block in blocks {
            let block_start = buf.len() as u64;
            let crc = compute_crc32c(&block.data);

            buf.put_u64(block.stream_id);
            buf.put_i64(block.start_offset);
            buf.put_i64(block.end_offset);
            buf.put_u32(block.record_count);
            buf.put_u32(block.data.len() as u32);
            buf.put_u32(crc);
            buf.put_slice(&block.data);

            let block_len = (buf.len() as u64) - block_start;

            index_entries.push(S3BlockIndex {
                stream_id: block.stream_id,
                start_offset: block.start_offset,
                end_offset: block.end_offset,
                record_count: block.record_count,
                byte_offset: block_start,
                byte_length: block_len,
            });
        }

        // Footer: Index table offset (8B) + Footer Magic (4B)
        let index_offset = buf.len() as u64;
        buf.put_u16(index_entries.len() as u16);
        for entry in index_entries {
            buf.put_u64(entry.stream_id);
            buf.put_i64(entry.start_offset);
            buf.put_i64(entry.end_offset);
            buf.put_u32(entry.record_count);
            buf.put_u64(entry.byte_offset);
            buf.put_u64(entry.byte_length);
        }

        buf.put_u64(index_offset);
        buf.put_u32(S3_OBJECT_MAGIC);

        buf.freeze()
    }

    /// Decodes all data blocks from a full S3 object payload.
    pub fn decode_all(data: &[u8]) -> Result<Vec<S3DataBlock>> {
        if data.len() < 8 {
            return Err(OxideMqError::Storage("S3 object too small".to_string()));
        }

        let mut cursor = data;
        let magic = cursor.get_u32();
        if magic != S3_OBJECT_MAGIC {
            return Err(OxideMqError::Storage(format!(
                "Invalid S3 object magic: 0x{:08X}",
                magic
            )));
        }

        let version = cursor.get_u16();
        if version != S3_OBJECT_VERSION {
            return Err(OxideMqError::Storage(format!(
                "Unsupported S3 object version: {}",
                version
            )));
        }

        let block_count = cursor.get_u16() as usize;
        let mut blocks = Vec::with_capacity(block_count);

        for _ in 0..block_count {
            if cursor.len() < BLOCK_HEADER_SIZE {
                return Err(OxideMqError::Storage(
                    "Truncated block header in S3 object".to_string(),
                ));
            }

            let stream_id = cursor.get_u64();
            let start_offset = cursor.get_i64();
            let end_offset = cursor.get_i64();
            let record_count = cursor.get_u32();
            let data_len = cursor.get_u32() as usize;
            let expected_crc = cursor.get_u32();

            if cursor.len() < data_len {
                return Err(OxideMqError::Storage(
                    "Truncated block data in S3 object".to_string(),
                ));
            }

            let block_bytes = &cursor[..data_len];
            let actual_crc = compute_crc32c(block_bytes);
            if actual_crc != expected_crc {
                return Err(OxideMqError::CorruptedRecord {
                    expected_crc,
                    actual_crc,
                });
            }

            let block_data = Bytes::copy_from_slice(block_bytes);
            cursor.advance(data_len);

            blocks.push(S3DataBlock::new(
                stream_id,
                start_offset,
                end_offset,
                record_count,
                block_data,
            ));
        }

        Ok(blocks)
    }

    /// Reads footer index from the tail of the S3 object for O(1) block lookup.
    pub fn read_footer_index(data: &[u8]) -> Result<Vec<S3BlockIndex>> {
        if data.len() < 12 {
            return Err(OxideMqError::Storage(
                "Object too small for footer index".to_string(),
            ));
        }

        let footer_magic_offset = data.len() - 4;
        let index_offset_pos = data.len() - 12;

        let mut magic_cursor = &data[footer_magic_offset..];
        if magic_cursor.get_u32() != S3_OBJECT_MAGIC {
            return Err(OxideMqError::Storage(
                "Invalid footer magic in S3 object".to_string(),
            ));
        }

        let mut offset_cursor = &data[index_offset_pos..footer_magic_offset];
        let index_offset = offset_cursor.get_u64() as usize;

        if index_offset >= data.len() {
            return Err(OxideMqError::Storage(
                "Invalid footer index offset".to_string(),
            ));
        }

        let mut index_cursor = &data[index_offset..index_offset_pos];
        let count = index_cursor.get_u16() as usize;
        let mut entries = Vec::with_capacity(count);

        for _ in 0..count {
            entries.push(S3BlockIndex {
                stream_id: index_cursor.get_u64(),
                start_offset: index_cursor.get_i64(),
                end_offset: index_cursor.get_i64(),
                record_count: index_cursor.get_u32(),
                byte_offset: index_cursor.get_u64(),
                byte_length: index_cursor.get_u64(),
            });
        }

        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_s3_object_encode_decode() {
        let b1 = S3DataBlock::new(1, 0, 9, 10, Bytes::from_static(b"block-1-data"));
        let b2 = S3DataBlock::new(2, 0, 4, 5, Bytes::from_static(b"block-2-stream-2-data"));

        let encoded = S3ObjectCodec::encode(&[b1.clone(), b2.clone()]);
        assert!(!encoded.is_empty());

        let decoded = S3ObjectCodec::decode_all(&encoded).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0], b1);
        assert_eq!(decoded[1], b2);

        let index = S3ObjectCodec::read_footer_index(&encoded).unwrap();
        assert_eq!(index.len(), 2);
        assert_eq!(index[0].stream_id, 1);
        assert_eq!(index[0].start_offset, 0);
        assert_eq!(index[0].end_offset, 9);
        assert_eq!(index[1].stream_id, 2);
    }
}
