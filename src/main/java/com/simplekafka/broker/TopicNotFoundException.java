package com.simplekafka.broker;

public class TopicNotFoundException extends RuntimeException {
    public TopicNotFoundException(String topicName) {
        super("Topic '" + topicName + "' was not found");
    }
}
