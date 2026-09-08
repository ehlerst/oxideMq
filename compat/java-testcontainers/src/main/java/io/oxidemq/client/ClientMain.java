package io.oxidemq.client;

import java.util.Arrays;

/**
 * Universal CLI entrypoint for Producer and Consumer client testcontainers.
 */
public class ClientMain {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: ClientMain <produce|consume> [args...]");
            System.exit(1);
        }

        String command = args[0];
        String[] subArgs = Arrays.copyOfRange(args, 1, args.length);

        if ("produce".equalsIgnoreCase(command)) {
            ProducerApp.main(subArgs);
        } else if ("consume".equalsIgnoreCase(command)) {
            ConsumerApp.main(subArgs);
        } else {
            System.err.println("Unknown command: " + command + ". Expected 'produce' or 'consume'.");
            System.exit(1);
        }
    }
}
