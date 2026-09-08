package io.oxidemq.testcontainers;

import com.github.dockerjava.api.command.InspectContainerResponse;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * Testcontainers implementation for oxideMq: Diskless, Pure Rust Apache Kafka®
 * compatible event streaming broker.
 */
public class OxideMqContainer extends GenericContainer<OxideMqContainer> {

    public static final DockerImageName DEFAULT_IMAGE_NAME =
            DockerImageName.parse("ehlers320/oxidemq:latest");

    public static final int KAFKA_PORT = 9092;
    public static final int ADMIN_PORT = 9093;

    public OxideMqContainer() {
        this(DEFAULT_IMAGE_NAME);
    }

    public OxideMqContainer(String dockerImageName) {
        this(DockerImageName.parse(dockerImageName));
    }

    public OxideMqContainer(final DockerImageName dockerImageName) {
        super(dockerImageName);
        dockerImageName.assertCompatibleWith(
                DEFAULT_IMAGE_NAME,
                DockerImageName.parse("ehlers320/oxidemq"),
                DockerImageName.parse("oxidemq")
        );
        withExposedPorts(KAFKA_PORT, ADMIN_PORT);
        waitingFor(Wait.forHttp("/_oxidemq/health")
                .forPort(ADMIN_PORT)
                .withStartupTimeout(Duration.ofSeconds(60)));
    }

    @Override
    protected void configure() {
        super.configure();
        withEnv("OXIDEMQ_ADVERTISED_HOST", getHost());
    }

    @Override
    protected void containerIsStarted(InspectContainerResponse containerInfo) {
        super.containerIsStarted(containerInfo);
        try {
            HttpClient client = HttpClient.newHttpClient();
            String jsonPayload = String.format("{\"host\":\"%s\",\"port\":%d}", getHost(), getMappedPort(KAFKA_PORT));
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(getAdminUrl() + "/_oxidemq/advertised"))
                    .header("Content-Type", "application/json")
                    .timeout(Duration.ofSeconds(5))
                    .POST(HttpRequest.BodyPublishers.ofString(jsonPayload))
                    .build();
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                logger().warn("Failed to set advertised address on broker: " + response.body());
            }
        } catch (Exception e) {
            logger().warn("Exception while configuring advertised address on broker", e);
        }
    }

    /**
     * @return the bootstrap servers connection string, e.g. "localhost:32789"
     */
    public String getBootstrapServers() {
        return String.format("%s:%d", getHost(), getMappedPort(KAFKA_PORT));
    }

    /**
     * @return the HTTP URL of the embedded Dark-Mode Web Console and Admin API
     */
    public String getAdminUrl() {
        return String.format("http://%s:%d", getHost(), getMappedPort(ADMIN_PORT));
    }
}
