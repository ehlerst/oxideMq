package io.oxidemq.testcontainers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
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
import java.util.Properties;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class OxideMqContainerTest {

    @Container
    static final OxideMqContainer oxidemq = new OxideMqContainer();

    @Test
    @DisplayName("Verify HTTP Health endpoint via Testcontainer")
    void testHttpHealthAndStatus() throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(oxidemq.getAdminUrl() + "/_oxidemq/health"))
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
        String bootstrap = oxidemq.getBootstrapServers();
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
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, oxidemq.getBootstrapServers());
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
}
