use crate::error::{OxideMqError, Result};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::io::{Read, Write};

/// Unique identifier for a streaming storage stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct StreamId(pub u64);

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "stream-{}", self.0)
    }
}

/// A topic and partition index pair.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: i32,
}

impl TopicPartition {
    pub fn new(topic: impl Into<String>, partition: i32) -> Self {
        Self {
            topic: topic.into(),
            partition,
        }
    }
}

impl fmt::Display for TopicPartition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.topic, self.partition)
    }
}

/// Compression codec supported by the messaging system.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[repr(i8)]
pub enum CompressionCodec {
    #[default]
    None = 0,
    Gzip = 1,
    Snappy = 2,
    Lz4 = 3,
    Zstd = 4,
}

impl CompressionCodec {
    pub fn from_attributes(attributes: i16) -> Self {
        match attributes & 0x07 {
            1 => Self::Gzip,
            2 => Self::Snappy,
            3 => Self::Lz4,
            4 => Self::Zstd,
            _ => Self::None,
        }
    }

    pub fn to_attributes(self) -> i16 {
        self as i16
    }

    /// Compresses raw record batch payload using this codec.
    pub fn compress(self, data: &[u8]) -> Result<Bytes> {
        match self {
            Self::None => Ok(Bytes::copy_from_slice(data)),
            Self::Gzip => {
                let mut encoder =
                    flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
                encoder
                    .write_all(data)
                    .map_err(|e| OxideMqError::Compression(format!("Gzip compress error: {e}")))?;
                let compressed = encoder
                    .finish()
                    .map_err(|e| OxideMqError::Compression(format!("Gzip finish error: {e}")))?;
                Ok(Bytes::from(compressed))
            }
            Self::Snappy => {
                let mut encoder = snap::raw::Encoder::new();
                let compressed = encoder.compress_vec(data).map_err(|e| {
                    OxideMqError::Compression(format!("Snappy compress error: {e}"))
                })?;
                Ok(Bytes::from(compressed))
            }
            Self::Lz4 => {
                let mut out = Vec::new();
                let mut encoder = lz4_flex::frame::FrameEncoder::new(&mut out);
                encoder
                    .write_all(data)
                    .map_err(|e| OxideMqError::Compression(format!("LZ4 compress error: {e}")))?;
                encoder
                    .finish()
                    .map_err(|e| OxideMqError::Compression(format!("LZ4 finish error: {e}")))?;
                Ok(Bytes::from(out))
            }
            Self::Zstd => {
                let compressed = zstd::stream::encode_all(data, 0)
                    .map_err(|e| OxideMqError::Compression(format!("Zstd compress error: {e}")))?;
                Ok(Bytes::from(compressed))
            }
        }
    }

    /// Decompresses raw compressed record batch payload using this codec.
    pub fn decompress(self, data: &[u8]) -> Result<Bytes> {
        match self {
            Self::None => Ok(Bytes::copy_from_slice(data)),
            Self::Gzip => {
                let mut decoder = flate2::read::GzDecoder::new(data);
                let mut decompressed = Vec::new();
                decoder.read_to_end(&mut decompressed).map_err(|e| {
                    OxideMqError::Compression(format!("Gzip decompress error: {e}"))
                })?;
                Ok(Bytes::from(decompressed))
            }
            Self::Snappy => {
                if data.starts_with(b"\xff\x06\x00\x00sNaPpY") {
                    let mut decoder = snap::read::FrameDecoder::new(data);
                    let mut decompressed = Vec::new();
                    decoder.read_to_end(&mut decompressed).map_err(|e| {
                        OxideMqError::Compression(format!("Snappy framed decompress error: {e}"))
                    })?;
                    Ok(Bytes::from(decompressed))
                } else {
                    let mut decoder = snap::raw::Decoder::new();
                    let decompressed = decoder.decompress_vec(data).map_err(|e| {
                        OxideMqError::Compression(format!("Snappy raw decompress error: {e}"))
                    })?;
                    Ok(Bytes::from(decompressed))
                }
            }
            Self::Lz4 => {
                if data.starts_with(&[0x04, 0x22, 0x4D, 0x18]) {
                    let mut decoder = lz4_flex::frame::FrameDecoder::new(data);
                    let mut decompressed = Vec::new();
                    decoder.read_to_end(&mut decompressed).map_err(|e| {
                        OxideMqError::Compression(format!("LZ4 frame decompress error: {e}"))
                    })?;
                    Ok(Bytes::from(decompressed))
                } else {
                    let decompressed =
                        lz4_flex::block::decompress_size_prepended(data).map_err(|e| {
                            OxideMqError::Compression(format!("LZ4 block decompress error: {e}"))
                        })?;
                    Ok(Bytes::from(decompressed))
                }
            }
            Self::Zstd => {
                let decompressed = zstd::stream::decode_all(data).map_err(|e| {
                    OxideMqError::Compression(format!("Zstd decompress error: {e}"))
                })?;
                Ok(Bytes::from(decompressed))
            }
        }
    }
}

/// Header key-value pair associated with an individual record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordHeader {
    pub key: String,
    pub value: Bytes,
}

impl RecordHeader {
    pub fn new(key: impl Into<String>, value: impl Into<Bytes>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

/// An individual Kafka / streaming record within a batch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
    pub offset: i64,
    pub timestamp: i64,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Vec<RecordHeader>,
}

impl Record {
    pub fn new(offset: i64, timestamp: i64, key: Option<Bytes>, value: Option<Bytes>) -> Self {
        Self {
            offset,
            timestamp,
            key,
            value,
            headers: Vec::new(),
        }
    }

    pub fn size_in_bytes(&self) -> usize {
        let key_len = self.key.as_ref().map_or(0, |k| k.len());
        let val_len = self.value.as_ref().map_or(0, |v| v.len());
        let headers_len: usize = self
            .headers
            .iter()
            .map(|h| h.key.len() + h.value.len() + 8)
            .sum();
        16 + key_len + val_len + headers_len
    }
}

/// A batch of records adhering to the Kafka RecordBatch v2 format.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordBatch {
    pub base_offset: i64,
    pub partition_leader_epoch: i32,
    pub magic: i8,
    pub crc: u32,
    pub attributes: i16,
    pub last_offset_delta: i32,
    pub base_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub records: Vec<Record>,
    pub raw_bytes: Bytes,
}

impl RecordBatch {
    pub fn new(base_offset: i64, records: Vec<Record>, raw_bytes: Bytes) -> Self {
        let last_offset_delta = if records.is_empty() {
            0
        } else {
            (records.len() - 1) as i32
        };

        let now = chrono::Utc::now().timestamp_millis();

        Self {
            base_offset,
            partition_leader_epoch: 0,
            magic: 2,
            crc: 0,
            attributes: 0,
            last_offset_delta,
            base_timestamp: now,
            max_timestamp: now,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records,
            raw_bytes,
        }
    }

    pub fn count(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub fn last_offset(&self) -> i64 {
        self.base_offset + self.last_offset_delta as i64
    }

    pub fn size_in_bytes(&self) -> usize {
        self.raw_bytes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stream_id() {
        let sid = StreamId(42);
        assert_eq!(sid.to_string(), "stream-42");
        assert_eq!(sid.0, 42);
    }

    #[test]
    fn test_topic_partition() {
        let tp = TopicPartition::new("test-topic", 3);
        assert_eq!(tp.topic, "test-topic");
        assert_eq!(tp.partition, 3);
        assert_eq!(tp.to_string(), "test-topic-3");

        let serialized = serde_json::to_string(&tp).unwrap();
        let deserialized: TopicPartition = serde_json::from_str(&serialized).unwrap();
        assert_eq!(tp, deserialized);
    }

    #[test]
    fn test_compression_codec() {
        assert_eq!(CompressionCodec::default(), CompressionCodec::None);
        assert_eq!(CompressionCodec::from_attributes(0), CompressionCodec::None);
        assert_eq!(CompressionCodec::from_attributes(1), CompressionCodec::Gzip);
        assert_eq!(
            CompressionCodec::from_attributes(2),
            CompressionCodec::Snappy
        );
        assert_eq!(CompressionCodec::from_attributes(3), CompressionCodec::Lz4);
        assert_eq!(CompressionCodec::from_attributes(4), CompressionCodec::Zstd);
        assert_eq!(CompressionCodec::from_attributes(7), CompressionCodec::None);

        assert_eq!(CompressionCodec::None.to_attributes(), 0);
        assert_eq!(CompressionCodec::Gzip.to_attributes(), 1);
        assert_eq!(CompressionCodec::Snappy.to_attributes(), 2);
        assert_eq!(CompressionCodec::Lz4.to_attributes(), 3);
        assert_eq!(CompressionCodec::Zstd.to_attributes(), 4);
    }

    #[test]
    fn test_record_and_headers() {
        let mut rec = Record::new(
            10,
            1234567890,
            Some(Bytes::from_static(b"key")),
            Some(Bytes::from_static(b"value")),
        );
        let header = RecordHeader::new("trace-id", Bytes::from_static(b"abc"));
        assert_eq!(header.key, "trace-id");
        assert_eq!(header.value, Bytes::from_static(b"abc"));
        rec.headers.push(header);

        let size = rec.size_in_bytes();
        assert!(size > 0);

        let empty_rec = Record::new(0, 0, None, None);
        assert_eq!(empty_rec.size_in_bytes(), 16);
    }

    #[test]
    fn test_record_batch() {
        let rec1 = Record::new(0, 1000, None, Some(Bytes::from_static(b"v1")));
        let rec2 = Record::new(1, 1001, None, Some(Bytes::from_static(b"v2")));
        let raw = Bytes::from_static(b"raw-batch-bytes");
        let batch = RecordBatch::new(100, vec![rec1, rec2], raw.clone());

        assert_eq!(batch.count(), 2);
        assert!(!batch.is_empty());
        assert_eq!(batch.last_offset(), 101);
        assert_eq!(batch.size_in_bytes(), raw.len());

        let empty_batch = RecordBatch::new(200, vec![], Bytes::new());
        assert_eq!(empty_batch.count(), 0);
        assert!(empty_batch.is_empty());
        assert_eq!(empty_batch.last_offset(), 200);
        assert_eq!(empty_batch.size_in_bytes(), 0);
    }

    #[test]
    fn test_compression_roundtrips() {
        let sample_data = b"The quick brown fox jumps over the lazy dog. Repeat this string to ensure good compression ratio! The quick brown fox jumps over the lazy dog.";

        let codecs = [
            CompressionCodec::None,
            CompressionCodec::Gzip,
            CompressionCodec::Snappy,
            CompressionCodec::Lz4,
            CompressionCodec::Zstd,
        ];

        for codec in codecs {
            let compressed = codec
                .compress(sample_data)
                .expect("Compression should succeed");
            if codec != CompressionCodec::None {
                assert!(
                    compressed.len() < sample_data.len(),
                    "Codec {:?} did not compress",
                    codec
                );
            } else {
                assert_eq!(compressed.as_ref(), sample_data);
            }

            let decompressed = codec
                .decompress(&compressed)
                .expect("Decompression should succeed");
            assert_eq!(
                decompressed.as_ref(),
                sample_data,
                "Decompressed mismatch for codec {:?}",
                codec
            );
        }
    }

    #[test]
    fn test_compression_snappy_framed() {
        use std::io::Write;
        let sample = b"Snappy framed test data with enough repeated elements 1234567890 1234567890";
        let mut framed_out = Vec::new();
        let mut encoder = snap::write::FrameEncoder::new(&mut framed_out);
        encoder.write_all(sample).unwrap();
        drop(encoder);

        assert!(framed_out.starts_with(b"\xff\x06\x00\x00sNaPpY"));
        let decompressed = CompressionCodec::Snappy.decompress(&framed_out).unwrap();
        assert_eq!(decompressed.as_ref(), sample);
    }

    #[test]
    fn test_compression_lz4_block() {
        let sample = b"LZ4 prepend size test payload with repeated data xyz123 xyz123 xyz123";
        let compressed = lz4_flex::block::compress_prepend_size(sample);
        let decompressed = CompressionCodec::Lz4.decompress(&compressed).unwrap();
        assert_eq!(decompressed.as_ref(), sample);
    }

    #[test]
    fn test_compression_invalid_data() {
        let invalid = b"completely-corrupted-and-invalid-compressed-bytes";
        assert!(CompressionCodec::Gzip.decompress(invalid).is_err());
        assert!(CompressionCodec::Snappy.decompress(invalid).is_err());
        assert!(CompressionCodec::Lz4.decompress(invalid).is_err());
        assert!(CompressionCodec::Zstd.decompress(invalid).is_err());
    }
}
