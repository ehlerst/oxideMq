use crate::parser::{KafkaDecoder, KafkaEncoder};
use crate::ApiKey;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use oxidemq_core::error::{OxideMqError, Result};

/// Standard Kafka Request Header (v1 & v2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestHeader {
    pub api_key: ApiKey,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<String>,
}

impl RequestHeader {
    pub fn new(
        api_key: ApiKey,
        api_version: i16,
        correlation_id: i32,
        client_id: Option<impl Into<String>>,
    ) -> Self {
        Self {
            api_key,
            api_version,
            correlation_id,
            client_id: client_id.map(Into::into),
        }
    }

    pub fn decode(src: &mut Bytes) -> Result<Self> {
        if src.len() < 8 {
            return Err(OxideMqError::Protocol("Truncated request header".into()));
        }

        let key_raw = src.get_i16();
        let api_key = ApiKey::from_i16(key_raw)
            .ok_or_else(|| OxideMqError::Protocol(format!("Unsupported API key: {}", key_raw)))?;

        let api_version = src.get_i16();
        let correlation_id = src.get_i32();
        let client_id = KafkaDecoder::read_string(src)?;

        if api_key == ApiKey::ApiVersions && api_version >= 3 && src.has_remaining() {
            let _tag_count = KafkaDecoder::read_unsigned_varint(src)?;
        }

        Ok(Self {
            api_key,
            api_version,
            correlation_id,
            client_id,
        })
    }

    pub fn encode(&self, dst: &mut BytesMut) {
        dst.put_i16(self.api_key as i16);
        dst.put_i16(self.api_version);
        dst.put_i32(self.correlation_id);
        KafkaEncoder::write_string(dst, self.client_id.as_deref());
    }
}

/// Standard Kafka Response Header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResponseHeader {
    pub correlation_id: i32,
}

impl ResponseHeader {
    pub fn new(correlation_id: i32) -> Self {
        Self { correlation_id }
    }

    pub fn decode(src: &mut Bytes) -> Result<Self> {
        if src.len() < 4 {
            return Err(OxideMqError::Protocol("Truncated response header".into()));
        }
        let correlation_id = src.get_i32();
        Ok(Self { correlation_id })
    }

    pub fn encode(&self, dst: &mut BytesMut) {
        dst.put_i32(self.correlation_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_header_codec() {
        let header = RequestHeader::new(ApiKey::Produce, 7, 42, Some("test-client"));
        let mut buf = BytesMut::new();
        header.encode(&mut buf);

        let mut read_buf = buf.freeze();
        let decoded = RequestHeader::decode(&mut read_buf).unwrap();
        assert_eq!(decoded, header);
    }

    #[test]
    fn test_response_header_codec() {
        let header = ResponseHeader::new(999);
        let mut buf = BytesMut::new();
        header.encode(&mut buf);

        let mut read_buf = buf.freeze();
        let decoded = ResponseHeader::decode(&mut read_buf).unwrap();
        assert_eq!(decoded, header);
    }

    #[test]
    fn test_header_errors_and_edge_cases() {
        let mut short_req = Bytes::from_static(&[0x00, 0x01, 0x00]);
        assert!(RequestHeader::decode(&mut short_req).is_err());

        let mut bad_key =
            Bytes::from_static(&[0xFF, 0xFF, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0xFF, 0xFF]);
        assert!(RequestHeader::decode(&mut bad_key).is_err());

        // ApiVersions v3 with tagged buffer
        let mut api_versions_v3 = BytesMut::new();
        api_versions_v3.put_i16(18); // ApiKey::ApiVersions
        api_versions_v3.put_i16(3); // version 3
        api_versions_v3.put_i32(100);
        KafkaEncoder::write_string(&mut api_versions_v3, Some("client"));
        KafkaEncoder::write_unsigned_varint(&mut api_versions_v3, 0); // 0 tagged fields
        let mut buf = api_versions_v3.freeze();
        let hdr = RequestHeader::decode(&mut buf).unwrap();
        assert_eq!(hdr.api_key, ApiKey::ApiVersions);
        assert_eq!(hdr.api_version, 3);

        let mut short_resp = Bytes::from_static(&[0x00, 0x01]);
        assert!(ResponseHeader::decode(&mut short_resp).is_err());
    }
}
