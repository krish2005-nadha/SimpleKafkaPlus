package com.simplekafka.broker;

public class TopicAlreadyExistsException extends RuntimeException {
    public TopicAlreadyExistsException(String topicName) {
        super("Topic '" + topicName + "' already exists");
    }
}
