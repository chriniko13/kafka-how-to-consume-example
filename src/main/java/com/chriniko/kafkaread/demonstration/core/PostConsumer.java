package com.chriniko.kafkaread.demonstration.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.function.Consumer;

public class PostConsumer {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static Consumer<ConsumerRecord<String, String>> consumePostRecord() {
        return record -> {
            String postAsJson = record.value();
            if (postAsJson != null) {

                Post post = getPost(postAsJson);
                String id = post.getId();

                System.out.println("[" + Thread.currentThread().getName()
                        + "] >>> Post id: "
                        + id + " --- partition: "
                        + record.partition() + " --- offset: "
                        + record.offset());
            }
        };
    }

    private static Post getPost(String postAsJson) {
        try {
            return objectMapper.readValue(postAsJson, Post.class);
        } catch (IOException e) {
            throw new ProcessingException(e);
        }
    }

}
