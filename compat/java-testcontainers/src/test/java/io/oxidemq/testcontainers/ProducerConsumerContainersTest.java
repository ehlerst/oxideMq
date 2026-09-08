package io.oxidemq.testcontainers;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class ProducerConsumerContainersTest {
    private static final Logger log = LoggerFactory.getLogger(ProducerConsumerContainersTest.class);

    private static Process brokerProcess;
    private static final int KAFKA_PORT = 9092;
    private static final int ADMIN_PORT = 9093;

    @BeforeAll
    static void setUp() throws Exception {
        if (!isPortOpen("127.0.0.1", KAFKA_PORT)) {
            File binary = findOxidemqBinary();
            log.info("Starting local oxideMq broker daemon: {}", binary.getAbsolutePath());
            ProcessBuilder pb = new ProcessBuilder(
                    binary.getAbsolutePath(), "start",
                    "--kafka-port", String.valueOf(KAFKA_PORT),
                    "--admin-port", String.valueOf(ADMIN_PORT)
            );
            pb.redirectErrorStream(true);
            brokerProcess = pb.start();
            waitForPort("127.0.0.1", KAFKA_PORT, Duration.ofSeconds(10));
            log.info("oxideMq broker successfully started on port {}", KAFKA_PORT);
        } else {
            log.info("Using already running oxideMq broker on port {}", KAFKA_PORT);
        }
    }

    @AfterAll
    static void tearDown() {
        if (brokerProcess != null && brokerProcess.isAlive()) {
            log.info("Terminating background oxideMq broker daemon...");
            brokerProcess.destroy();
        }
    }

    @Test
    @DisplayName("Verify Producer Container produces fake objects and Consumer Container validates them against oxideMq")
    void testProducerAndConsumerContainersEndToEnd() {
        String topic = "end-to-end-synthetic-topic";
        int count = 100;

        // 1. Launch Producer Test Container
        log.info("Launching ProducerContainer to produce {} fake objects...", count);
        try (ProducerContainer producer = new ProducerContainer()
                .withBootstrapServers("localhost:" + KAFKA_PORT)
                .withTopic(topic)
                .withRecordCount(count)) {
            producer.start();
            String logs = producer.getLogs();
            assertTrue(logs.contains("PRODUCE_COMPLETED: " + count + " records successfully sent"),
                    "Producer container did not complete successfully: " + logs);
        }

        // 2. Launch Consumer Test Container
        log.info("Launching ConsumerContainer to consume and validate {} fake objects...", count);
        try (ConsumerContainer consumer = new ConsumerContainer()
                .withBootstrapServers("localhost:" + KAFKA_PORT)
                .withTopic(topic)
                .withExpectedCount(count)) {
            consumer.start();
            String logs = consumer.getLogs();
            assertTrue(logs.contains("CONSUME_VALIDATED: " + count + " records successfully verified"),
                    "Consumer container did not validate records successfully: " + logs);
        }
    }

    private static boolean isPortOpen(String host, int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 200);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private static void waitForPort(String host, int port, Duration timeout) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (isPortOpen(host, port)) {
                return;
            }
            Thread.sleep(100);
        }
        throw new RuntimeException("Timed out waiting for port " + port);
    }

    private static File findOxidemqBinary() {
        String[] candidates = {
                "../../target/release/oxidemq",
                "../target/release/oxidemq",
                "target/release/oxidemq",
                "/usr/local/bin/oxidemq"
        };
        for (String c : candidates) {
            File f = new File(c);
            if (f.exists() && f.canExecute()) {
                return f;
            }
        }
        throw new IllegalStateException("Could not find executable oxidemq binary in any candidate location");
    }
}
