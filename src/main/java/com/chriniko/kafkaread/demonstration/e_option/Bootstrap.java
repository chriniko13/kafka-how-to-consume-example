package com.chriniko.kafkaread.demonstration.e_option;

import com.chriniko.kafkaread.demonstration.core.PostProducer;

public class Bootstrap {

    public static void run() {
        // Note: start producing records to kafka
        new Thread(() -> new PostProducer().run()).start();

        new ConsumerWithKafkaStreams();
    }

}
