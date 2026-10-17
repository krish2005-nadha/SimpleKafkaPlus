package com.simplekafka.server.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class PublishRequest {
    private final String key;
    private final String value;

    @JsonCreator
    public PublishRequest(
            @JsonProperty("key") String key,
            @JsonProperty("value") String value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }
}
