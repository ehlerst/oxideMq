//! # oxideMq Compatibility Tests (Tier 1)
//!
//! Pure in-memory integration & Kafka protocol compatibility tests executing in milliseconds.

#[cfg(test)]
mod tests {
    use oxidemq_core::prelude::*;

    #[test]
    fn test_core_primitives_in_memory() {
        let tp = TopicPartition::new("telemetry-events", 0);
        assert_eq!(tp.topic, "telemetry-events");
        assert_eq!(tp.partition, 0);

        let record = Record::new(
            0,
            123456789,
            Some(bytes::Bytes::from_static(b"key-1")),
            Some(bytes::Bytes::from_static(b"value-1")),
        );
        let batch = RecordBatch::new(
            0,
            vec![record],
            bytes::Bytes::from_static(b"raw-batch-payload"),
        );
        assert_eq!(batch.count(), 1);
        assert_eq!(batch.last_offset(), 0);
    }
}
