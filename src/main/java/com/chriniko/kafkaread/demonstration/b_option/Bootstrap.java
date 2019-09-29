package com.chriniko.kafkaread.demonstration.b_option;

/*
    From Kafka Consumer Java Doc:

    2. Decouple Consumption and Processing

    Another alternative is to have one or more consumer threads that do all data consumption
    and hands off ConsumerRecords instances to a blocking queue consumed by a pool of processor threads that actually
    handle the record processing. This option likewise has pros and cons:

    PRO: This option allows independently scaling the number of consumers and processors.
    This makes it possible to have a single consumer that feeds many processor threads,
    avoiding any limitation on partitions.

    CON: Guaranteeing order across the processors requires particular care as the threads will execute independently
    an earlier chunk of data may actually be processed after a later chunk of data just due to the luck of thread execution timing.
    For processing that has no ordering requirements this is not a problem.

    CON: Manually committing the position becomes harder as it requires that all threads co-ordinate to ensure that
    processing is complete for that partition.

 */

import com.chriniko.kafkaread.demonstration.core.PostConsumer;
import com.chriniko.kafkaread.demonstration.core.PostProducer;

import java.util.UUID;

public class Bootstrap {


    public static void run() {

        // Note: start producing records to kafka
        new Thread(() -> new PostProducer().run()).start();

        final String bootstrap = "localhost:9092";
        final String groupId = "b-option" + UUID.randomUUID().toString();
        final String kafkaTopic = "posts";

        OneConsumerWithSubscriptionWithWorkerThreads reader
                = new OneConsumerWithSubscriptionWithWorkerThreads(bootstrap, groupId, kafkaTopic);

        reader.search(PostConsumer.consumePostRecord());

        for (; ;) ;

    }
}
