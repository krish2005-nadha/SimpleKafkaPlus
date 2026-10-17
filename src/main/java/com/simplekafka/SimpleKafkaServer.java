package com.simplekafka;

import com.simplekafka.broker.Broker;
import com.simplekafka.server.HttpServerApp;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Main application entry point for SimpleKafka+.
 * Boots the message broker, recovers local log files, and starts the HTTP server.
 */
public class SimpleKafkaServer {
    private static final int DEFAULT_PORT = 8000;
    private static final String DEFAULT_DATA_DIR = "data";

    public static void main(String[] args) {
        int port = DEFAULT_PORT;
        if (args.length > 0) {
            try {
                port = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid port number specified. Using default: " + DEFAULT_PORT);
            }
        } else if (System.getenv("PORT") != null) {
            try {
                port = Integer.parseInt(System.getenv("PORT"));
            } catch (NumberFormatException ignored) {
            }
        }

        String dataDirPath = System.getenv().getOrDefault("DATA_DIR", DEFAULT_DATA_DIR);
        Path dataDir = Path.of(dataDirPath);

        System.out.println("==================================================");
        System.out.println("            SimpleKafka+ Broker Starting          ");
        System.out.println("==================================================");
        System.out.println("Storage Directory : " + dataDir.toAbsolutePath());
        System.out.println("HTTP Port          : " + port);

        try {
            Broker broker = new Broker(dataDir);
            HttpServerApp app = new HttpServerApp(broker, port);

            // Graceful shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("\nShutting down SimpleKafka+ broker...");
                app.stop(1);
                try {
                    broker.close();
                } catch (IOException e) {
                    System.err.println("Error closing broker storage: " + e.getMessage());
                }
                System.out.println("SimpleKafka+ broker stopped cleanly.");
            }));

            app.start();

            System.out.println("Broker started successfully!");
            System.out.println("Endpoints available at:");
            System.out.println("  - Health check : GET  http://127.0.0.1:" + port + "/health");
            System.out.println("  - List topics  : GET  http://127.0.0.1:" + port + "/topics");
            System.out.println("  - Create topic : POST http://127.0.0.1:" + port + "/topics");
            System.out.println("  - Publish msg  : POST http://127.0.0.1:" + port + "/topics/{topic}/messages");
            System.out.println("  - Consume msg  : GET  http://127.0.0.1:" + port + "/topics/{topic}/messages?offset=0&limit=10");
            System.out.println("==================================================");

        } catch (IOException e) {
            System.err.println("Failed to start SimpleKafka+ server: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
