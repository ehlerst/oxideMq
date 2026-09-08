package io.oxidemq.client;

import java.util.Objects;

/**
 * Deterministic synthetic object with verifiable integrity, sequence, and payload.
 */
public class FakeDomainObject {
    private String id;
    private int sequence;
    private long timestamp;
    private String payload;
    private String checksum;

    public FakeDomainObject() {}

    public FakeDomainObject(String id, int sequence, long timestamp, String payload, String checksum) {
        this.id = id;
        this.sequence = sequence;
        this.timestamp = timestamp;
        this.payload = payload;
        this.checksum = checksum;
    }

    public static FakeDomainObject generate(int seq) {
        String id = String.format("synthetic-obj-%06d", seq);
        long ts = 1715000000000L + seq * 1000L;
        String payload = String.format("synthetic-payload-content-seq-%06d-key-%x", seq, (seq * 31337) ^ 0xCAFEBABE);
        String checksum = Integer.toHexString((id + ":" + seq + ":" + payload).hashCode());
        return new FakeDomainObject(id, seq, ts, payload, checksum);
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public int getSequence() { return sequence; }
    public void setSequence(int sequence) { this.sequence = sequence; }

    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }

    public String getChecksum() { return checksum; }
    public void setChecksum(String checksum) { this.checksum = checksum; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FakeDomainObject that = (FakeDomainObject) o;
        return sequence == that.sequence &&
                timestamp == that.timestamp &&
                Objects.equals(id, that.id) &&
                Objects.equals(payload, that.payload) &&
                Objects.equals(checksum, that.checksum);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, sequence, timestamp, payload, checksum);
    }

    @Override
    public String toString() {
        return "FakeDomainObject{" +
                "id='" + id + '\'' +
                ", sequence=" + sequence +
                ", timestamp=" + timestamp +
                ", checksum='" + checksum + '\'' +
                '}';
    }
}
