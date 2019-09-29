package com.chriniko.kafkaread.demonstration.c_option;

import com.chriniko.kafkaread.demonstration.core.PostConsumer;
import com.chriniko.kafkaread.demonstration.core.PartitionsInfoProvider;
import com.chriniko.kafkaread.demonstration.core.PostProducer;

import java.util.UUID;

public class Bootstrap {


    public static void run() {
        // Note: start producing records to kafka
        new Thread(() -> new PostProducer().run()).start();

        final String bootstrap = "localhost:9092";

        final String groupId = "c-option" + UUID.randomUUID().toString();
        final String kafkaTopic = "posts";

        final int howManyConsumers = PartitionsInfoProvider.noOfPartitions(bootstrap, kafkaTopic);

        System.out.println("Will create " + howManyConsumers + " consumers...");

        new PartitionsAssignedConsumerCoordinator(
                bootstrap, groupId, kafkaTopic, howManyConsumers,
                PostConsumer.consumePostRecord()
        );
    }
}
