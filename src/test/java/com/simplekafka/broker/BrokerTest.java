package com.simplekafka.broker;

import com.simplekafka.model.Message;
import com.simplekafka.model.TopicInfo;
import com.simplekafka.storage.Topic;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class BrokerTest {

    @TempDir
    Path tempDir;

    private Broker broker;

    @BeforeEach
    void setUp() throws IOException {
        broker = new Broker(tempDir);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (broker != null) {
            broker.close();
        }
    }

    @Test
    @DisplayName("Create topic and list topics successfully")
    void testCreateAndListTopics() throws IOException {
        assertTrue(broker.listTopics().isEmpty());

        Topic topic = broker.createTopic("events");
        assertNotNull(topic);
        assertEquals("events", topic.getName());
        assertEquals(0, topic.getMessageCount());
        assertEquals(0, topic.getNextOffset());

        List<TopicInfo> topics = broker.listTopics();
        assertEquals(1, topics.size());
        assertEquals("events", topics.get(0).name());
        assertEquals(0, topics.get(0).messageCount());
        assertEquals(0, topics.get(0).nextOffset());
    }

    @Test
    @DisplayName("Duplicate topic creation throws TopicAlreadyExistsException")
    void testDuplicateTopicCreation() throws IOException {
        broker.createTopic("orders");
        assertThrows(TopicAlreadyExistsException.class, () -> broker.createTopic("orders"));
    }

    @Test
    @DisplayName("Invalid topic name throws IllegalArgumentException")
    void testInvalidTopicNames() {
        assertThrows(IllegalArgumentException.class, () -> broker.createTopic(""));
        assertThrows(IllegalArgumentException.class, () -> broker.createTopic("invalid/topic"));
        assertThrows(IllegalArgumentException.class, () -> broker.createTopic("invalid topic"));
        assertThrows(IllegalArgumentException.class, () -> broker.createTopic(null));
    }

    @Test
    @DisplayName("Publishing messages assigns sequential offsets and unique IDs")
    void testPublishSequentialOffsets() throws IOException {
        broker.createTopic("sensor-data");

        Message m1 = broker.publish("sensor-data", "temp", "21.5");
        Message m2 = broker.publish("sensor-data", "humidity", "45.0");
        Message m3 = broker.publish("sensor-data", "pressure", "1013.2");

        assertNotNull(m1.getId());
        assertNotNull(m2.getId());
        assertNotNull(m3.getId());
        assertNotEquals(m1.getId(), m2.getId());

        assertEquals(0, m1.getOffset());
        assertEquals(1, m2.getOffset());
        assertEquals(2, m3.getOffset());

        assertEquals("21.5", m1.getValue());
        assertEquals("humidity", m2.getKey());
        assertTrue(m1.getTimestamp() > 0);
    }

    @Test
    @DisplayName("Consume messages with offset and limit")
    void testConsumeWithOffsetAndLimit() throws IOException {
        broker.createTopic("payments");

        for (int i = 0; i < 10; i++) {
            broker.publish("payments", "order-" + i, "amount-" + (100 + i));
        }

        // Consume first 3 messages from offset 0
        List<Message> firstBatch = broker.consume("payments", 0, 3);
        assertEquals(3, firstBatch.size());
        assertEquals(0, firstBatch.get(0).getOffset());
        assertEquals(1, firstBatch.get(1).getOffset());
        assertEquals(2, firstBatch.get(2).getOffset());

        // Consume next 4 messages from offset 3
        List<Message> secondBatch = broker.consume("payments", 3, 4);
        assertEquals(4, secondBatch.size());
        assertEquals(3, secondBatch.get(0).getOffset());
        assertEquals(6, secondBatch.get(3).getOffset());

        // Consume past the end returns empty list
        List<Message> emptyBatch = broker.consume("payments", 10, 5);
        assertTrue(emptyBatch.isEmpty());
    }

    @Test
    @DisplayName("Consume invalid arguments validate offset and limit")
    void testConsumeInvalidArguments() throws IOException {
        broker.createTopic("test");

        assertThrows(IllegalArgumentException.class, () -> broker.consume("test", -1, 10));
        assertThrows(IllegalArgumentException.class, () -> broker.consume("test", 0, 0));
        assertThrows(IllegalArgumentException.class, () -> broker.consume("test", 0, -5));
    }

    @Test
    @DisplayName("Publishing to nonexistent topic throws TopicNotFoundException")
    void testPublishToNonexistentTopic() {
        assertThrows(TopicNotFoundException.class, () -> broker.publish("ghost", "k", "v"));
    }

    @Test
    @DisplayName("Persistence and recovery across broker restarts")
    void testPersistenceAndRecovery() throws IOException {
        // Step 1: Create topic and publish messages on the first broker instance
        broker.createTopic("analytics");
        broker.publish("analytics", "click", "home_page");
        broker.publish("analytics", "click", "product_details");
        broker.publish("analytics", "purchase", "checkout_completed");

        assertEquals(3, broker.getTopic("analytics").getMessageCount());
        assertEquals(3, broker.getTopic("analytics").getNextOffset());

        // Step 2: Simulate application shutdown
        broker.close();

        // Step 3: Start a new broker pointing to the same data directory
        Broker recoveredBroker = new Broker(tempDir);
        try {
            assertTrue(recoveredBroker.hasTopic("analytics"), "Topic should be discovered from disk");

            Topic recoveredTopic = recoveredBroker.getTopic("analytics");
            assertEquals(3, recoveredTopic.getMessageCount());
            assertEquals(3, recoveredTopic.getNextOffset());

            // Consume all 3 recovered messages
            List<Message> recoveredMessages = recoveredBroker.consume("analytics", 0, 10);
            assertEquals(3, recoveredMessages.size());

            assertEquals("click", recoveredMessages.get(0).getKey());
            assertEquals("home_page", recoveredMessages.get(0).getValue());
            assertEquals(0, recoveredMessages.get(0).getOffset());

            assertEquals("click", recoveredMessages.get(1).getKey());
            assertEquals("product_details", recoveredMessages.get(1).getValue());
            assertEquals(1, recoveredMessages.get(1).getOffset());

            assertEquals("purchase", recoveredMessages.get(2).getKey());
            assertEquals("checkout_completed", recoveredMessages.get(2).getValue());
            assertEquals(2, recoveredMessages.get(2).getOffset());

            // Step 4: Publish new message to the recovered topic and verify offset continuity
            Message newMsg = recoveredBroker.publish("analytics", "review", "rated_5_stars");
            assertEquals(3, newMsg.getOffset(), "Offset must continue sequentially from recovered state");
            assertEquals(4, recoveredTopic.getMessageCount());
            assertEquals(4, recoveredTopic.getNextOffset());

            // Step 5: Read all 4 messages
            List<Message> allMessages = recoveredBroker.consume("analytics", 0, 10);
            assertEquals(4, allMessages.size());
            assertEquals(3, allMessages.get(3).getOffset());
            assertEquals("rated_5_stars", allMessages.get(3).getValue());
        } finally {
            recoveredBroker.close();
        }
    }
}
