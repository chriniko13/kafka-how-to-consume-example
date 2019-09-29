package com.chriniko.kafkaread.demonstration.core;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Post {

    public static final String KAFKA_TOPIC = "posts";

    private String id;

    private String author;
    private String description;
    private String text;

    private String createdAt;
    private String updatedAt;

}
