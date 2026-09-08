package io.oxidemq.testcontainers;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

/**
 * Testcontainer for a standard Kafka Consumer application consuming and verifying synthetic fake objects.
 */
public class ConsumerContainer extends GenericContainer<ConsumerContainer> {

    public static final DockerImageName DEFAULT_IMAGE = DockerImageName.parse("oxidemq-kafka-client:latest");

    private String bootstrapServers = "localhost:9092";
    private String topic = "synthetic-verification-topic";
    private int expectedCount = 100;

    public ConsumerContainer() {
        this(DEFAULT_IMAGE);
    }

    public ConsumerContainer(DockerImageName imageName) {
        super(imageName);
        withNetworkMode("host");
    }

    public ConsumerContainer withBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        return this;
    }

    public ConsumerContainer withTopic(String topic) {
        this.topic = topic;
        return this;
    }

    public ConsumerContainer withExpectedCount(int expectedCount) {
        this.expectedCount = expectedCount;
        return this;
    }

    @Override
    protected void configure() {
        super.configure();
        withCommand("consume", bootstrapServers, topic, String.valueOf(expectedCount));
        waitingFor(Wait.forLogMessage(".*CONSUME_VALIDATED.*\\n", 1).withStartupTimeout(Duration.ofSeconds(60)));
    }
}
