package com.simplekafka.broker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.simplekafka.model.Message;
import com.simplekafka.model.TopicInfo;
import com.simplekafka.storage.Topic;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Core Message Broker for SimpleKafka+.
 * Manages topic lifecycle, local file-based persistence discovery,
 * thread-safe publishing, and sequential offset-based consumption.
 */
public class Broker implements Closeable {
    private static final String LOG_EXTENSION = ".log";

    private final Path dataDir;
    private final ObjectMapper objectMapper;
    private final ConcurrentMap<String, Topic> topics;

    public Broker(Path dataDir) throws IOException {
        this.dataDir = dataDir;
        this.objectMapper = new ObjectMapper();
        this.topics = new ConcurrentHashMap<>();

        initializeAndRecover();
    }

    private void initializeAndRecover() throws IOException {
        if (!Files.exists(dataDir)) {
            Files.createDirectories(dataDir);
        }

        // Recover existing topic logs from the data directory
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dataDir, "*" + LOG_EXTENSION)) {
            for (Path entry : stream) {
                if (Files.isRegularFile(entry)) {
                    String fileName = entry.getFileName().toString();
                    String topicName = fileName.substring(0, fileName.length() - LOG_EXTENSION.length());
                    Topic topic = new Topic(topicName, entry, objectMapper);
                    topics.put(topicName, topic);
                }
            }
        }
    }

    /**
     * Creates a new topic. Fails if topic with the given name already exists.
     */
    public synchronized Topic createTopic(String topicName) throws IOException {
        Topic.validateTopicName(topicName);
        if (topics.containsKey(topicName)) {
            throw new TopicAlreadyExistsException(topicName);
        }

        Path logPath = dataDir.resolve(topicName + LOG_EXTENSION);
        Topic topic = new Topic(topicName, logPath, objectMapper);
        topics.put(topicName, topic);
        return topic;
    }

    /**
     * Retrieves an existing topic by name.
     */
    public Topic getTopic(String topicName) {
        Topic topic = topics.get(topicName);
        if (topic == null) {
            throw new TopicNotFoundException(topicName);
        }
        return topic;
    }

    public boolean hasTopic(String topicName) {
        return topics.containsKey(topicName);
    }

    /**
     * Publishes a message to a topic.
     */
    public Message publish(String topicName, String key, String value) throws IOException {
        Topic topic = getTopic(topicName);
        return topic.publish(key, value);
    }

    /**
     * Consumes messages from a topic starting from the given offset.
     */
    public List<Message> consume(String topicName, long fromOffset, int limit) {
        Topic topic = getTopic(topicName);
        return topic.consume(fromOffset, limit);
    }

    /**
     * Returns metadata for all active topics.
     */
    public List<TopicInfo> listTopics() {
        List<TopicInfo> list = new ArrayList<>();
        for (Topic topic : topics.values()) {
            list.add(new TopicInfo(topic.getName(), topic.getMessageCount(), topic.getNextOffset()));
        }
        return list;
    }

    public Path getDataDir() {
        return dataDir;
    }

    private boolean closed = false;

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        for (Topic topic : topics.values()) {
            topic.close();
        }
    }
}
