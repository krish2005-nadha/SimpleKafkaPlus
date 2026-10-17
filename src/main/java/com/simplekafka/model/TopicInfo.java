package com.simplekafka.model;

public record TopicInfo(String name, long messageCount, long nextOffset) {
}
