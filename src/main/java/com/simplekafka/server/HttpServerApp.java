package com.simplekafka.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.simplekafka.broker.Broker;
import com.simplekafka.broker.TopicAlreadyExistsException;
import com.simplekafka.broker.TopicNotFoundException;
import com.simplekafka.model.Message;
import com.simplekafka.model.TopicInfo;
import com.simplekafka.server.dto.ConsumeResponse;
import com.simplekafka.server.dto.CreateTopicRequest;
import com.simplekafka.server.dto.PublishRequest;
import com.simplekafka.storage.Topic;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * Embedded HTTP API server for SimpleKafka+ using Java's built-in HttpServer.
 * Provides RESTful endpoints for health checks, topic management, message publishing,
 * and offset-based consumption.
 */
public class HttpServerApp {
    private final HttpServer server;
    private final Broker broker;
    private final ObjectMapper objectMapper;
    private final byte[] indexHtmlBytes;

    public HttpServerApp(Broker broker, int port) throws IOException {
        this.broker = broker;
        this.objectMapper = new ObjectMapper();

        byte[] htmlBytes;
        try (InputStream is = getClass().getResourceAsStream("/static/index.html")) {
            if (is != null) {
                htmlBytes = is.readAllBytes();
            } else {
                htmlBytes = "<h1>SimpleKafka+ Broker</h1>".getBytes(StandardCharsets.UTF_8);
            }
        }
        this.indexHtmlBytes = htmlBytes;

        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.server.setExecutor(Executors.newFixedThreadPool(16));

        setupRoutes();
    }

    private void setupRoutes() {
        server.createContext("/", new RootHandler());
        server.createContext("/health", new HealthHandler());
        server.createContext("/topics", new TopicsHandler());
    }

    public void start() {
        server.start();
    }

    public void stop(int delaySeconds) {
        server.stop(delaySeconds);
    }

    public int getPort() {
        return server.getAddress().getPort();
    }

    private class RootHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String path = exchange.getRequestURI().getPath();
            if ("/".equals(path) || "/dashboard".equals(path) || "/index.html".equals(path)) {
                if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                    sendError(exchange, 405, "Method Not Allowed");
                    return;
                }
                exchange.getResponseHeaders().set("Content-Type", "text/html; charset=UTF-8");
                exchange.sendResponseHeaders(200, indexHtmlBytes.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(indexHtmlBytes);
                }
                return;
            }

            if ("/favicon.ico".equals(path)) {
                exchange.sendResponseHeaders(204, -1);
                exchange.close();
                return;
            }

            sendError(exchange, 404, "Endpoint not found: " + path);
        }
    }

    private class HealthHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendError(exchange, 405, "Method Not Allowed");
                return;
            }

            Map<String, Object> health = Map.of(
                    "status", "UP",
                    "topicsCount", broker.listTopics().size(),
                    "timestamp", System.currentTimeMillis()
            );
            sendJson(exchange, 200, health);
        }
    }

    private class TopicsHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try {
                String path = exchange.getRequestURI().getPath();
                // Normalize path (strip trailing slash if present)
                if (path.length() > 1 && path.endsWith("/")) {
                    path = path.substring(0, path.length() - 1);
                }

                String[] segments = path.split("/");
                // segments[0] is "", segments[1] is "topics"

                if (segments.length == 2) {
                    // Path: /topics
                    handleTopicsRoot(exchange);
                } else if (segments.length == 3) {
                    // Path: /topics/{topicName}
                    handleTopicByName(exchange, decode(segments[2]));
                } else if (segments.length == 4 && "messages".equals(segments[3])) {
                    // Path: /topics/{topicName}/messages
                    handleTopicMessages(exchange, decode(segments[2]));
                } else {
                    sendError(exchange, 404, "Endpoint not found: " + path);
                }
            } catch (TopicAlreadyExistsException e) {
                sendError(exchange, 409, e.getMessage());
            } catch (TopicNotFoundException e) {
                sendError(exchange, 404, e.getMessage());
            } catch (IllegalArgumentException | JsonProcessingException e) {
                sendError(exchange, 400, "Bad Request: " + e.getMessage());
            } catch (Exception e) {
                sendError(exchange, 500, "Internal Server Error: " + e.getMessage());
            }
        }

        private void handleTopicsRoot(HttpExchange exchange) throws IOException {
            String method = exchange.getRequestMethod();
            if ("GET".equalsIgnoreCase(method)) {
                List<TopicInfo> topics = broker.listTopics();
                sendJson(exchange, 200, topics);
            } else if ("POST".equalsIgnoreCase(method)) {
                String body = readBody(exchange);
                CreateTopicRequest request = objectMapper.readValue(body, CreateTopicRequest.class);
                if (request == null || request.getName() == null || request.getName().trim().isEmpty()) {
                    sendError(exchange, 400, "Topic name is required");
                    return;
                }
                Topic topic = broker.createTopic(request.getName().trim());
                TopicInfo info = new TopicInfo(topic.getName(), topic.getMessageCount(), topic.getNextOffset());
                sendJson(exchange, 201, info);
            } else {
                sendError(exchange, 405, "Method Not Allowed");
            }
        }

        private void handleTopicByName(HttpExchange exchange, String topicName) throws IOException {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendError(exchange, 405, "Method Not Allowed");
                return;
            }
            Topic topic = broker.getTopic(topicName);
            TopicInfo info = new TopicInfo(topic.getName(), topic.getMessageCount(), topic.getNextOffset());
            sendJson(exchange, 200, info);
        }

        private void handleTopicMessages(HttpExchange exchange, String topicName) throws IOException {
            String method = exchange.getRequestMethod();
            if ("POST".equalsIgnoreCase(method)) {
                String body = readBody(exchange);
                PublishRequest request = objectMapper.readValue(body, PublishRequest.class);
                if (request == null || request.getValue() == null) {
                    sendError(exchange, 400, "Message 'value' is required");
                    return;
                }
                Message message = broker.publish(topicName, request.getKey(), request.getValue());
                sendJson(exchange, 201, message);
            } else if ("GET".equalsIgnoreCase(method)) {
                Map<String, String> query = parseQuery(exchange.getRequestURI());
                long offset = 0;
                int limit = 100;

                if (query.containsKey("offset")) {
                    try {
                        offset = Long.parseLong(query.get("offset"));
                    } catch (NumberFormatException e) {
                        sendError(exchange, 400, "Query parameter 'offset' must be an integer");
                        return;
                    }
                }
                if (query.containsKey("limit")) {
                    try {
                        limit = Integer.parseInt(query.get("limit"));
                    } catch (NumberFormatException e) {
                        sendError(exchange, 400, "Query parameter 'limit' must be an integer");
                        return;
                    }
                }

                if (offset < 0) {
                    sendError(exchange, 400, "'offset' must be non-negative");
                    return;
                }
                if (limit <= 0) {
                    sendError(exchange, 400, "'limit' must be greater than zero");
                    return;
                }

                Topic topic = broker.getTopic(topicName);
                List<Message> messages = topic.consume(offset, limit);
                long nextOffset = messages.isEmpty() ? offset : messages.get(messages.size() - 1).getOffset() + 1;

                ConsumeResponse response = new ConsumeResponse(
                        topicName,
                        offset,
                        messages.size(),
                        nextOffset,
                        messages
                );
                sendJson(exchange, 200, response);
            } else {
                sendError(exchange, 405, "Method Not Allowed");
            }
        }
    }

    private void sendJson(HttpExchange exchange, int statusCode, Object body) throws IOException {
        byte[] bytes = objectMapper.writeValueAsBytes(body);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private void sendError(HttpExchange exchange, int statusCode, String message) throws IOException {
        sendJson(exchange, statusCode, Map.of("error", message));
    }

    private String readBody(HttpExchange exchange) throws IOException {
        try (InputStream is = exchange.getRequestBody()) {
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private String decode(String segment) {
        return URLDecoder.decode(segment, StandardCharsets.UTF_8);
    }

    private Map<String, String> parseQuery(URI uri) {
        Map<String, String> params = new HashMap<>();
        String rawQuery = uri.getRawQuery();
        if (rawQuery == null || rawQuery.isEmpty()) {
            return params;
        }

        String[] pairs = rawQuery.split("&");
        for (String pair : pairs) {
            int idx = pair.indexOf("=");
            if (idx > 0) {
                String key = URLDecoder.decode(pair.substring(0, idx), StandardCharsets.UTF_8);
                String value = URLDecoder.decode(pair.substring(idx + 1), StandardCharsets.UTF_8);
                params.put(key, value);
            } else if (!pair.isEmpty()) {
                params.put(URLDecoder.decode(pair, StandardCharsets.UTF_8), "");
            }
        }
        return params;
    }
}
