use bytes::{Buf, BufMut, Bytes, BytesMut};
use oxidemq_core::error::{OxideMqError, Result};

/// Wire-level decoding helpers for Kafka types.
pub struct KafkaDecoder;

impl KafkaDecoder {
    pub fn read_string(src: &mut Bytes) -> Result<Option<String>> {
        if src.len() < 2 {
            return Err(OxideMqError::Protocol("Truncated string length".into()));
        }
        let len = src.get_i16();
        if len < 0 {
            return Ok(None);
        }
        let len = len as usize;
        if src.len() < len {
            return Err(OxideMqError::Protocol("Truncated string body".into()));
        }
        let bytes = src.copy_to_bytes(len);
        let s = String::from_utf8(bytes.to_vec())
            .map_err(|e| OxideMqError::Protocol(format!("Invalid UTF-8: {}", e)))?;
        Ok(Some(s))
    }

    pub fn read_compact_string(src: &mut Bytes) -> Result<Option<String>> {
        let len = Self::read_unsigned_varint(src)?;
        if len == 0 {
            return Ok(None);
        }
        let len = (len - 1) as usize;
        if src.len() < len {
            return Err(OxideMqError::Protocol(
                "Truncated compact string body".into(),
            ));
        }
        let bytes = src.copy_to_bytes(len);
        let s = String::from_utf8(bytes.to_vec())
            .map_err(|e| OxideMqError::Protocol(format!("Invalid UTF-8: {}", e)))?;
        Ok(Some(s))
    }

    pub fn read_bytes(src: &mut Bytes) -> Result<Option<Bytes>> {
        if src.len() < 4 {
            return Err(OxideMqError::Protocol("Truncated bytes length".into()));
        }
        let len = src.get_i32();
        if len < 0 {
            return Ok(None);
        }
        let len = len as usize;
        if src.len() < len {
            return Err(OxideMqError::Protocol("Truncated bytes body".into()));
        }
        Ok(Some(src.copy_to_bytes(len)))
    }

    pub fn read_compact_bytes(src: &mut Bytes) -> Result<Option<Bytes>> {
        let len = Self::read_unsigned_varint(src)?;
        if len == 0 {
            return Ok(None);
        }
        let len = (len - 1) as usize;
        if src.len() < len {
            return Err(OxideMqError::Protocol(
                "Truncated compact bytes body".into(),
            ));
        }
        Ok(Some(src.copy_to_bytes(len)))
    }

    pub fn read_unsigned_varint(src: &mut Bytes) -> Result<u64> {
        let mut value: u64 = 0;
        let mut shift = 0;
        while src.has_remaining() {
            let b = src.get_u8();
            value |= ((b & 0x7F) as u64) << shift;
            if (b & 0x80) == 0 {
                return Ok(value);
            }
            shift += 7;
            if shift > 63 {
                return Err(OxideMqError::Protocol("Varint overflow".into()));
            }
        }
        Err(OxideMqError::Protocol("Truncated varint".into()))
    }

    pub fn read_varint(src: &mut Bytes) -> Result<i64> {
        let uval = Self::read_unsigned_varint(src)?;
        // Zigzag decode
        Ok(((uval >> 1) as i64) ^ (-((uval & 1) as i64)))
    }
}

/// Wire-level encoding helpers for Kafka types.
pub struct KafkaEncoder;

impl KafkaEncoder {
    pub fn write_string(dst: &mut BytesMut, val: Option<&str>) {
        match val {
            Some(s) => {
                dst.put_i16(s.len() as i16);
                dst.put_slice(s.as_bytes());
            }
            None => {
                dst.put_i16(-1);
            }
        }
    }

    pub fn write_compact_string(dst: &mut BytesMut, val: Option<&str>) {
        match val {
            Some(s) => {
                Self::write_unsigned_varint(dst, (s.len() + 1) as u64);
                dst.put_slice(s.as_bytes());
            }
            None => {
                Self::write_unsigned_varint(dst, 0);
            }
        }
    }

    pub fn write_bytes(dst: &mut BytesMut, val: Option<&[u8]>) {
        match val {
            Some(b) => {
                dst.put_i32(b.len() as i32);
                dst.put_slice(b);
            }
            None => {
                dst.put_i32(-1);
            }
        }
    }

    pub fn write_compact_bytes(dst: &mut BytesMut, val: Option<&[u8]>) {
        match val {
            Some(b) => {
                Self::write_unsigned_varint(dst, (b.len() + 1) as u64);
                dst.put_slice(b);
            }
            None => {
                Self::write_unsigned_varint(dst, 0);
            }
        }
    }

    pub fn write_unsigned_varint(dst: &mut BytesMut, mut value: u64) {
        while value >= 0x80 {
            dst.put_u8(((value & 0x7F) as u8) | 0x80);
            value >>= 7;
        }
        dst.put_u8(value as u8);
    }

    pub fn write_varint(dst: &mut BytesMut, value: i64) {
        // Zigzag encode
        let zigzag = ((value << 1) ^ (value >> 63)) as u64;
        Self::write_unsigned_varint(dst, zigzag);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_codec() {
        let mut buf = BytesMut::new();
        KafkaEncoder::write_string(&mut buf, Some("my-kafka-topic"));
        KafkaEncoder::write_string(&mut buf, None);

        let mut read_buf = buf.freeze();
        assert_eq!(
            KafkaDecoder::read_string(&mut read_buf).unwrap(),
            Some("my-kafka-topic".to_string())
        );
        assert_eq!(KafkaDecoder::read_string(&mut read_buf).unwrap(), None);
    }

    #[test]
    fn test_varint_codec() {
        let mut buf = BytesMut::new();
        KafkaEncoder::write_varint(&mut buf, 0);
        KafkaEncoder::write_varint(&mut buf, -1);
        KafkaEncoder::write_varint(&mut buf, 1234567);

        let mut read_buf = buf.freeze();
        assert_eq!(KafkaDecoder::read_varint(&mut read_buf).unwrap(), 0);
        assert_eq!(KafkaDecoder::read_varint(&mut read_buf).unwrap(), -1);
        assert_eq!(KafkaDecoder::read_varint(&mut read_buf).unwrap(), 1234567);
    }
}
