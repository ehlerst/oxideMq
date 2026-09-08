use bytes::{Buf, BufMut, Bytes, BytesMut};
use oxidemq_core::error::{OxideMqError, Result};
use std::io;
use tokio_util::codec::{Decoder, Encoder};

pub const MAX_KAFKA_FRAME_SIZE: usize = 100 * 1024 * 1024; // 100 MB max frame

/// TCP frame codec that reads and writes 4-byte length-prefixed Kafka frames.
#[derive(Debug, Default)]
pub struct KafkaFrameCodec;

impl KafkaFrameCodec {
    pub fn new() -> Self {
        Self
    }
}

impl Decoder for KafkaFrameCodec {
    type Item = Bytes;
    type Error = OxideMqError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        if src.len() < 4 {
            // Need at least 4 bytes for length header
            return Ok(None);
        }

        let mut length_bytes = [0u8; 4];
        length_bytes.copy_from_slice(&src[..4]);
        let frame_len = i32::from_be_bytes(length_bytes);

        if frame_len < 0 {
            return Err(OxideMqError::Protocol(format!(
                "Negative frame length: {}",
                frame_len
            )));
        }

        let frame_len = frame_len as usize;
        if frame_len > MAX_KAFKA_FRAME_SIZE {
            return Err(OxideMqError::Protocol(format!(
                "Frame length exceeds maximum ({} > {})",
                frame_len, MAX_KAFKA_FRAME_SIZE
            )));
        }

        let total_len = 4 + frame_len;
        if src.len() < total_len {
            // Frame incomplete, reserve and wait for more data
            src.reserve(total_len - src.len());
            return Ok(None);
        }

        // Advance past 4-byte length prefix and extract payload frame
        src.advance(4);
        let frame = src.split_to(frame_len).freeze();
        Ok(Some(frame))
    }
}

impl Encoder<Bytes> for KafkaFrameCodec {
    type Error = io::Error;

    fn encode(&mut self, item: Bytes, dst: &mut BytesMut) -> std::result::Result<(), Self::Error> {
        let len = item.len() as i32;
        dst.reserve(4 + item.len());
        dst.put_i32(len);
        dst.put_slice(&item);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_codec_framing() {
        let mut codec = KafkaFrameCodec::new();
        let mut buf = BytesMut::new();

        let payload = Bytes::from_static(b"kafka-request-payload");
        codec.encode(payload.clone(), &mut buf).unwrap();

        assert_eq!(buf.len(), 4 + payload.len());

        let decoded = codec.decode(&mut buf).unwrap().expect("Decoded frame");
        assert_eq!(decoded, payload);
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn test_codec_error_conditions() {
        let mut codec = KafkaFrameCodec::new();

        // < 4 bytes returns Ok(None)
        let mut short_buf = BytesMut::from(&b"12"[..]);
        assert_eq!(codec.decode(&mut short_buf).unwrap(), None);

        // Negative frame length
        let mut neg_buf = BytesMut::new();
        neg_buf.put_i32(-5);
        assert!(codec.decode(&mut neg_buf).is_err());

        // Oversized frame
        let mut huge_buf = BytesMut::new();
        huge_buf.put_i32(MAX_KAFKA_FRAME_SIZE as i32 + 100);
        assert!(codec.decode(&mut huge_buf).is_err());

        // Incomplete payload
        let mut partial_buf = BytesMut::new();
        partial_buf.put_i32(10);
        partial_buf.put_slice(b"1234");
        assert_eq!(codec.decode(&mut partial_buf).unwrap(), None);
    }
}
