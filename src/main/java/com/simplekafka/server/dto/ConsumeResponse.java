package com.simplekafka.server.dto;

import com.simplekafka.model.Message;

import java.util.List;

public record ConsumeResponse(
        String topic,
        long fromOffset,
        int count,
        long nextOffset,
        List<Message> messages
) {
}
