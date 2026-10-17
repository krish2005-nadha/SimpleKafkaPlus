package com.simplekafka.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * Represents a single message published to a topic.
 * Each message has a unique identifier, an immutable sequential offset,
 * a timestamp, an optional key (for partitioning/grouping), and a payload value.
 */
public class Message {
    private final String id;
    private final long offset;
    private final long timestamp;
    private final String key;
    private final String value;

    @JsonCreator
    public Message(
            @JsonProperty("id") String id,
            @JsonProperty("offset") long offset,
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("key") String key,
            @JsonProperty("value") String value) {
        this.id = id;
        this.offset = offset;
        this.timestamp = timestamp;
        this.key = key;
        this.value = value;
    }

    public String getId() {
        return id;
    }

    public long getOffset() {
        return offset;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Message message)) return false;
        return offset == message.offset &&
                timestamp == message.timestamp &&
                Objects.equals(id, message.id) &&
                Objects.equals(key, message.key) &&
                Objects.equals(value, message.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, offset, timestamp, key, value);
    }

    @Override
    public String toString() {
        return "Message{" +
                "id='" + id + '\'' +
                ", offset=" + offset +
                ", timestamp=" + timestamp +
                ", key='" + key + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}
