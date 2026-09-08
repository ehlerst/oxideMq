package io.oxidemq.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

/**
 * Standard Apache Kafka Consumer that consumes and verifies synthetic fake objects against the known dataset.
 */
public class ConsumerApp {
    private static final Logger log = LoggerFactory.getLogger(ConsumerApp.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: ConsumerApp <bootstrap-servers> <topic> <expected-count>");
            System.exit(1);
        }

        String bootstrapServers = args[0];
        String topic = args[1];
        int expectedCount = Integer.parseInt(args[2]);

        log.info("Starting ConsumerApp connecting to {} consuming {} fake objects from topic {}",
                bootstrapServers, expectedCount, topic);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "synthetic-verification-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        Set<Integer> receivedSequences = new HashSet<>();
        long deadline = System.currentTimeMillis() + 60000; // 60s timeout

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            TopicPartition tp = new TopicPartition(topic, 0);
            consumer.assign(Collections.singletonList(tp));
            consumer.seekToBeginning(Collections.singletonList(tp));

            while (receivedSequences.size() < expectedCount && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    FakeDomainObject actual = mapper.readValue(record.value(), FakeDomainObject.class);
                    int seq = actual.getSequence();

                    if (seq < 0 || seq >= expectedCount) {
                        throw new IllegalStateException("Sequence out of expected bounds: " + seq);
                    }

                    FakeDomainObject expected = FakeDomainObject.generate(seq);
                    if (!expected.equals(actual)) {
                        throw new AssertionError(String.format("Record validation failed at seq=%d! Expected: %s, Actual: %s",
                                seq, expected, actual));
                    }

                    receivedSequences.add(seq);
                    if (receivedSequences.size() % 50 == 0 || receivedSequences.size() == expectedCount) {
                        log.info("Validated {}/{} records...", receivedSequences.size(), expectedCount);
                    }
                }
            }

            if (receivedSequences.size() != expectedCount) {
                throw new IllegalStateException(String.format("Verification timed out! Expected %d records, but only received %d",
                        expectedCount, receivedSequences.size()));
            }

            System.out.println("CONSUME_VALIDATED: " + expectedCount + " records successfully verified from " + topic);
            log.info("All {} fake objects successfully validated against known dataset!", expectedCount);
            System.exit(0);
        } catch (Exception e) {
            log.error("Fatal error in ConsumerApp", e);
            System.exit(3);
        }
    }
}
