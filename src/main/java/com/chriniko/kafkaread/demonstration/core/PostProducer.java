package com.chriniko.kafkaread.demonstration.core;

import com.chriniko.kafkaread.demonstration.core.Post;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.security.SecureRandom;
import java.time.Instant;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class PostProducer {

    private final Producer<String, String> producer;

    public PostProducer() {
        producer = createProducer();
    }

    public void run() {

        try {
            SecureRandom random = new SecureRandom();
            ObjectMapper mapper = new ObjectMapper();

            while (true) {

                String id = UUID.randomUUID().toString();
                Instant now = Instant.now();
                int idx = random.nextInt();

                Post post = new Post(id, "author" + idx, "decription" + idx, "text" + idx, now.toString(), now.toString());
                String postAsString = mapper.writeValueAsString(post);

                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("posts", id, postAsString);
                producer.send(producerRecord, (metadata, exception) -> {
                    if (exception != null) {
                        System.out.println("error occurred during send producer record, error: " + exception);
                    } else {
                        System.out.println("successfully produced producer record, metadata: " + metadata);
                    }
                });

                TimeUnit.SECONDS.sleep(3);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }



    public Producer<String, String> createProducer() {

        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "posts-producer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<>(props);
    }
}
