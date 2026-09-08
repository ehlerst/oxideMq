package io.oxidemq.testcontainers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class OxideMqContainerTest {

    @Container
    static final OxideMqContainer producerContainer = new OxideMqContainer();

    @Container
    static final OxideMqContainer consumerContainer = new OxideMqContainer();

    @Test
    @DisplayName("Verify HTTP Health endpoint via Testcontainer")
    void testHttpHealthAndStatus() throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(producerContainer.getAdminUrl() + "/_oxidemq/health"))
                .timeout(Duration.ofSeconds(5))
                .GET()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        assertEquals("OK", response.body().trim());
    }

    @Test
    @DisplayName("Verify Kafka Producer via Official Java Kafka Client")
    void testKafkaProduce() throws Exception {
        String bootstrap = producerContainer.getBootstrapServers();
        assertNotNull(bootstrap);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "10000");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>("testcontainers-java-topic", "key-1", "hello-from-java-testcontainers");

            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata metadata = future.get();

            assertNotNull(metadata);
            assertEquals("testcontainers-java-topic", metadata.topic());
            assertTrue(metadata.offset() >= 0);
        }
    }

    @Test
    @DisplayName("Verify Multiple Records Produce via Official Java Kafka Client")
    void testKafkaProduceMultipleRecords() throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, producerContainer.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < 10; i++) {
                RecordMetadata rm = producer.send(new ProducerRecord<>("batch-java-topic", "key-" + i, "val-" + i)).get();
                assertNotNull(rm);
                assertEquals(i, rm.offset());
            }
        }
    }

    @Test
    @DisplayName("Verify End-to-End Produce and Consumer Fetch via Official Java Kafka Client")
    void testKafkaProduceAndConsume() throws Exception {
        String bootstrap = consumerContainer.getBootstrapServers();
        String topic = "consumer-test-topic";

        // 1. Produce 5 records
        Properties prodProps = new Properties();
        prodProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        prodProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prodProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prodProps.put(ProducerConfig.ACKS_CONFIG, "1");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(prodProps)) {
            for (int i = 0; i < 5; i++) {
                producer.send(new ProducerRecord<>(topic, "k-" + i, "v-" + i)).get();
            }
        }

        // 2. Consume with official KafkaConsumer
        Properties consProps = new Properties();
        consProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        consProps.put(ConsumerConfig.GROUP_ID_CONFIG, "testcontainers-consumer-group");
        consProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consProps)) {
            TopicPartition tp = new TopicPartition(topic, 0);
            consumer.assign(Collections.singletonList(tp));
            consumer.seekToBeginning(Collections.singletonList(tp));

            java.util.List<ConsumerRecord<String, String>> received = new java.util.ArrayList<>();
            long deadline = System.currentTimeMillis() + 10000;
            while (received.size() < 5 && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> rec : records) {
                    received.add(rec);
                }
            }
            assertEquals(5, received.size(), "Should have received all 5 records");

            int idx = 0;
            for (ConsumerRecord<String, String> rec : received) {
                assertEquals("k-" + idx, rec.key());
                assertEquals("v-" + idx, rec.value());
                assertEquals(idx, rec.offset());
                idx++;
            }
        }
    }
}
