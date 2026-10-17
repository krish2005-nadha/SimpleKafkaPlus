package com.simplekafka.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.simplekafka.broker.Broker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class HttpServerAppTest {

    @TempDir
    Path tempDir;

    private Broker broker;
    private HttpServerApp app;
    private HttpClient client;
    private ObjectMapper mapper;
    private String baseUrl;

    @BeforeEach
    void setUp() throws IOException {
        broker = new Broker(tempDir);
        // Start on ephemeral port (port 0 selects available free port)
        app = new HttpServerApp(broker, 0);
        app.start();

        client = HttpClient.newHttpClient();
        mapper = new ObjectMapper();
        baseUrl = "http://127.0.0.1:" + app.getPort();
    }

    @AfterEach
    void tearDown() throws IOException {
        if (app != null) {
            app.stop(0);
        }
        if (broker != null) {
            broker.close();
        }
    }

    @Test
    @DisplayName("GET /health returns 200 and status UP")
    void testHealthCheck() throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/health"))
                .GET()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());

        JsonNode json = mapper.readTree(response.body());
        assertEquals("UP", json.get("status").asText());
        assertEquals(0, json.get("topicsCount").asInt());
        assertTrue(json.has("timestamp"));
    }

    @Test
    @DisplayName("GET / and /dashboard serves web console dashboard")
    void testWebConsoleDashboard() throws Exception {
        HttpRequest rootReq = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/"))
                .GET()
                .build();
        HttpResponse<String> rootRes = client.send(rootReq, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, rootRes.statusCode());
        assertTrue(rootRes.headers().firstValue("Content-Type").orElse("").contains("text/html"));
        assertTrue(rootRes.body().contains("SimpleKafka+"));

        HttpRequest dashReq = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/dashboard"))
                .GET()
                .build();
        HttpResponse<String> dashRes = client.send(dashReq, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, dashRes.statusCode());
        assertTrue(dashRes.body().contains("SimpleKafka+"));
    }

    @Test
    @DisplayName("Topic management: create, duplicate conflict, and list")
    void testTopicLifecycle() throws Exception {
        // Create topic
        HttpRequest createReq = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/topics"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{\"name\":\"orders\"}"))
                .build();

        HttpResponse<String> createRes = client.send(createReq, HttpResponse.BodyHandlers.ofString());
        assertEquals(201, createRes.statusCode());
        JsonNode createJson = mapper.readTree(createRes.body());
        assertEquals("orders", createJson.get("name").asText());
        assertEquals(0, createJson.get("messageCount").asLong());

        // Duplicate create -> 409 Conflict
        HttpResponse<String> dupRes = client.send(createReq, HttpResponse.BodyHandlers.ofString());
        assertEquals(409, dupRes.statusCode());

        // Invalid topic name -> 400 Bad Request
        HttpRequest invalidReq = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/topics"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{\"name\":\"\"}"))
                .build();
        HttpResponse<String> invalidRes = client.send(invalidReq, HttpResponse.BodyHandlers.ofString());
        assertEquals(400, invalidRes.statusCode());

        // List topics
        HttpRequest listReq = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/topics"))
                .GET()
                .build();

        HttpResponse<String> listRes = client.send(listReq, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, listRes.statusCode());
        JsonNode listJson = mapper.readTree(listRes.body());
        assertTrue(listJson.isArray());
        assertEquals(1, listJson.size());
        assertEquals("orders", listJson.get(0).get("name").asText());

        // Get single topic
        HttpRequest getTopicReq = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/topics/orders"))
                .GET()
                .build();
        HttpResponse<String> getTopicRes = client.send(getTopicReq, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, getTopicRes.statusCode());

        // Get non-existent topic -> 404
        HttpRequest notFoundReq = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/topics/missing"))
                .GET()
                .build();
        assertEquals(404, client.send(notFoundReq, HttpResponse.BodyHandlers.ofString()).statusCode());
    }

    @Test
    @DisplayName("Publish and consume messages with offsets and pagination")
    void testPublishAndConsume() throws Exception {
        // Create topic
        broker.createTopic("notifications");

        // Publish message 1
        HttpRequest pub1 = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/topics/notifications/messages"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{\"key\":\"u1\",\"value\":\"welcome-msg\"}"))
                .build();

        HttpResponse<String> pubRes1 = client.send(pub1, HttpResponse.BodyHandlers.ofString());
        assertEquals(201, pubRes1.statusCode());
        JsonNode msg1 = mapper.readTree(pubRes1.body());
        assertEquals(0, msg1.get("offset").asLong());
        assertEquals("u1", msg1.get("key").asText());
        assertEquals("welcome-msg", msg1.get("value").asText());

        // Publish message 2
        HttpRequest pub2 = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/topics/notifications/messages"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{\"key\":\"u2\",\"value\":\"password-reset\"}"))
                .build();

        HttpResponse<String> pubRes2 = client.send(pub2, HttpResponse.BodyHandlers.ofString());
        assertEquals(201, pubRes2.statusCode());
        JsonNode msg2 = mapper.readTree(pubRes2.body());
        assertEquals(1, msg2.get("offset").asLong());

        // Consume messages starting from offset 0, limit 1
        HttpRequest consumeReq1 = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/topics/notifications/messages?offset=0&limit=1"))
                .GET()
                .build();

        HttpResponse<String> consumeRes1 = client.send(consumeReq1, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, consumeRes1.statusCode());
        JsonNode consumeJson1 = mapper.readTree(consumeRes1.body());
        assertEquals("notifications", consumeJson1.get("topic").asText());
        assertEquals(1, consumeJson1.get("count").asInt());
        assertEquals(1, consumeJson1.get("nextOffset").asLong());
        assertEquals(0, consumeJson1.get("messages").get(0).get("offset").asLong());

        // Consume starting from offset 1, limit 5
        HttpRequest consumeReq2 = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/topics/notifications/messages?offset=1&limit=5"))
                .GET()
                .build();

        HttpResponse<String> consumeRes2 = client.send(consumeReq2, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, consumeRes2.statusCode());
        JsonNode consumeJson2 = mapper.readTree(consumeRes2.body());
        assertEquals(1, consumeJson2.get("count").asInt());
        assertEquals(2, consumeJson2.get("nextOffset").asLong());
        assertEquals(1, consumeJson2.get("messages").get(0).get("offset").asLong());
    }

    @Test
    @DisplayName("Validation and error handling on message endpoints")
    void testMessageEndpointErrors() throws Exception {
        broker.createTopic("chat");

        // Missing value in payload -> 400
        HttpRequest invalidBodyReq = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/topics/chat/messages"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{\"key\":\"u1\"}"))
                .build();
        assertEquals(400, client.send(invalidBodyReq, HttpResponse.BodyHandlers.ofString()).statusCode());

        // Publish to unknown topic -> 404
        HttpRequest notFoundPub = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/topics/missing/messages"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{\"value\":\"hello\"}"))
                .build();
        assertEquals(404, client.send(notFoundPub, HttpResponse.BodyHandlers.ofString()).statusCode());

        // Negative offset -> 400
        HttpRequest negOffsetReq = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/topics/chat/messages?offset=-5"))
                .GET()
                .build();
        assertEquals(400, client.send(negOffsetReq, HttpResponse.BodyHandlers.ofString()).statusCode());

        // Non-positive limit -> 400
        HttpRequest badLimitReq = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/topics/chat/messages?limit=0"))
                .GET()
                .build();
        assertEquals(400, client.send(badLimitReq, HttpResponse.BodyHandlers.ofString()).statusCode());

        // Unsupported method -> 405
        HttpRequest unsupportedMethod = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/health"))
                .method("DELETE", HttpRequest.BodyPublishers.noBody())
                .build();
        assertEquals(405, client.send(unsupportedMethod, HttpResponse.BodyHandlers.ofString()).statusCode());
    }
}
