package com.simplekafka.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.simplekafka.model.Message;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

/**
 * Topic manages an append-only commit log for a given topic name.
 * It provides thread-safe publishing with sequential offsets, persistence to disk
 * via JSONL format, and index-based offset consumption.
 */
public class Topic implements Closeable {
    private static final Pattern TOPIC_NAME_PATTERN = Pattern.compile("^[a-zA-Z0-9._-]{1,128}$");

    private final String name;
    private final Path logPath;
    private final ObjectMapper objectMapper;
    private final List<Message> inMemoryLog;
    private final AtomicLong nextOffset;
    private BufferedWriter fileWriter;

    public Topic(String name, Path logPath, ObjectMapper objectMapper) throws IOException {
        validateTopicName(name);
        this.name = name;
        this.logPath = logPath;
        this.objectMapper = objectMapper;
        this.inMemoryLog = new ArrayList<>();
        this.nextOffset = new AtomicLong(0);

        recoverAndInitialize();
    }

    public static void validateTopicName(String name) {
        if (name == null || !TOPIC_NAME_PATTERN.matcher(name).matches()) {
            throw new IllegalArgumentException(
                    "Invalid topic name '" + name + "'. Must be 1-128 alphanumeric characters, dots, dashes, or underscores."
            );
        }
    }

    /**
     * Recovers existing messages from the local JSONL log file and initializes the append-only writer.
     */
    private synchronized void recoverAndInitialize() throws IOException {
        if (Files.exists(logPath)) {
            try (BufferedReader reader = Files.newBufferedReader(logPath, StandardCharsets.UTF_8)) {
                String line;
                long lastOffset = -1;
                while ((line = reader.readLine()) != null) {
                    line = line.trim();
                    if (!line.isEmpty()) {
                        Message message = objectMapper.readValue(line, Message.class);
                        inMemoryLog.add(message);
                        lastOffset = message.getOffset();
                    }
                }
                nextOffset.set(lastOffset + 1);
            }
        } else {
            if (logPath.getParent() != null) {
                Files.createDirectories(logPath.getParent());
            }
            Files.createFile(logPath);
        }

        // Open writer in append mode with explicit flush on write
        this.fileWriter = Files.newBufferedWriter(
                logPath,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND
        );
    }

    /**
     * Publishes a message to the topic commit log.
     * Assigns a unique UUID, timestamp, and sequential offset.
     * Persists immediately to disk and appends to in-memory index.
     */
    public synchronized Message publish(String key, String value) throws IOException {
        if (value == null) {
            throw new IllegalArgumentException("Message value cannot be null");
        }

        long offset = nextOffset.getAndIncrement();
        String id = UUID.randomUUID().toString();
        long timestamp = System.currentTimeMillis();

        Message message = new Message(id, offset, timestamp, key, value);

        // Serialize and persist to disk (append-only JSONL format)
        String jsonLine = objectMapper.writeValueAsString(message);
        fileWriter.write(jsonLine);
        fileWriter.newLine();
        fileWriter.flush();

        inMemoryLog.add(message);
        return message;
    }

    /**
     * Reads messages starting from a given offset up to limit count.
     */
    public synchronized List<Message> consume(long fromOffset, int limit) {
        if (fromOffset < 0) {
            throw new IllegalArgumentException("Offset must be non-negative");
        }
        if (limit <= 0) {
            throw new IllegalArgumentException("Limit must be greater than zero");
        }

        if (inMemoryLog.isEmpty() || fromOffset >= inMemoryLog.size()) {
            return Collections.emptyList();
        }

        int start = (int) fromOffset;
        int end = Math.min(start + limit, inMemoryLog.size());

        return new ArrayList<>(inMemoryLog.subList(start, end));
    }

    public String getName() {
        return name;
    }

    public synchronized long getMessageCount() {
        return inMemoryLog.size();
    }

    public long getNextOffset() {
        return nextOffset.get();
    }

    public Path getLogPath() {
        return logPath;
    }

    private boolean closed = false;

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        if (fileWriter != null) {
            try {
                fileWriter.flush();
            } finally {
                fileWriter.close();
            }
        }
    }
}
