package io.oxidemq.testcontainers;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

/**
 * Testcontainer for a standard Kafka Producer application producing known synthetic fake objects.
 */
public class ProducerContainer extends GenericContainer<ProducerContainer> {

    public static final DockerImageName DEFAULT_IMAGE = DockerImageName.parse("oxidemq-kafka-client:latest");

    private String bootstrapServers = "localhost:9092";
    private String topic = "synthetic-verification-topic";
    private int recordCount = 100;

    public ProducerContainer() {
        this(DEFAULT_IMAGE);
    }

    public ProducerContainer(DockerImageName imageName) {
        super(imageName);
        withNetworkMode("host");
    }

    public ProducerContainer withBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        return this;
    }

    public ProducerContainer withTopic(String topic) {
        this.topic = topic;
        return this;
    }

    public ProducerContainer withRecordCount(int recordCount) {
        this.recordCount = recordCount;
        return this;
    }

    @Override
    protected void configure() {
        super.configure();
        withCommand("produce", bootstrapServers, topic, String.valueOf(recordCount));
        waitingFor(Wait.forLogMessage(".*PRODUCE_COMPLETED.*\\n", 1).withStartupTimeout(Duration.ofSeconds(60)));
    }
}
