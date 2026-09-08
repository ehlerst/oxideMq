package io.oxidemq.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.Future;

/**
 * Standard Apache Kafka Producer that produces a known set of synthetic fake objects.
 */
public class ProducerApp {
    private static final Logger log = LoggerFactory.getLogger(ProducerApp.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: ProducerApp <bootstrap-servers> <topic> <count>");
            System.exit(1);
        }

        String bootstrapServers = args[0];
        String topic = args[1];
        int count = Integer.parseInt(args[2]);

        log.info("Starting ProducerApp connecting to {} producing {} fake objects to topic {}",
                bootstrapServers, count, topic);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, "3");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "15000");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < count; i++) {
                FakeDomainObject obj = FakeDomainObject.generate(i);
                String json = mapper.writeValueAsString(obj);
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, obj.getId(), json);

                Future<RecordMetadata> future = producer.send(record);
                RecordMetadata rm = future.get();
                if (log.isDebugEnabled() || (i + 1) % 50 == 0 || i == count - 1) {
                    log.info("Produced obj sequence={} offset={}", obj.getSequence(), rm.offset());
                }
            }
            producer.flush();
            System.out.println("PRODUCE_COMPLETED: " + count + " records successfully sent to " + topic);
            log.info("Successfully produced all {} fake objects.", count);
            System.exit(0);
        } catch (Exception e) {
            log.error("Fatal error in ProducerApp", e);
            System.exit(2);
        }
    }
}
