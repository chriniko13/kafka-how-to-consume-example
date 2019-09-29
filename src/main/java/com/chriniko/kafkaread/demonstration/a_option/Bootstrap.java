package com.chriniko.kafkaread.demonstration.a_option;

import com.chriniko.kafkaread.demonstration.core.PostConsumer;
import com.chriniko.kafkaread.demonstration.core.PartitionsInfoProvider;
import com.chriniko.kafkaread.demonstration.core.Pools;
import com.chriniko.kafkaread.demonstration.core.PostProducer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/*
    From Kafka Consumer Java Doc:

    1. One Consumer Per Thread

    A simple option is to give each thread its own consumer instance. Here are the pros and cons of this approach:

    PRO: It is the easiest to implement

    PRO: It is often the fastest as no inter-thread co-ordination is needed

    PRO: It makes in-order processing on a per-partition basis very easy to implement
    (each thread just processes messages in the order it receives them).

    CON: More consumers means more TCP connections to the cluster (one per thread).
    In general Kafka handles connections very efficiently so this is generally a small cost.

    CON: Multiple consumers means more requests being sent to the server and slightly less batching
    of data which can cause some drop in I/O throughput.

    CON: The number of total threads across all processes will be limited by the total number of partitions.

 */

public class Bootstrap {


    public static void run() {
        // Note: start producing records to kafka
        new Thread(() -> new PostProducer().run()).start();


        // --- Consumer Initialization Section ---
        final String bootstrap = "localhost:9092";
        final String groupId = "a-option" + UUID.randomUUID().toString();
        final String kafkaTopic = "posts";

        //final int noOfWorkers = Runtime.getRuntime().availableProcessors();
        //final int noOfWorkers = 4;
        final int noOfWorkers = PartitionsInfoProvider.noOfPartitions(bootstrap, kafkaTopic);

        // Note: create workers pool.
        final ExecutorService workers = Executors.newFixedThreadPool(noOfWorkers, new ThreadFactory() {
            private AtomicInteger idx = new AtomicInteger();
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName("bom-reader-" + idx.incrementAndGet());
                return t;
            }
        });

        // Note: create one consumer per thread.
        final List<ConsumerWithSubscription> bomReaders = IntStream.rangeClosed(1, noOfWorkers)
                .boxed()
                .map(idx -> new ConsumerWithSubscription(bootstrap, groupId, kafkaTopic))
                .collect(Collectors.toList());

        // Note: set clear/exit operations
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            bomReaders.forEach(ConsumerWithSubscription::exit);
            Pools.shutdownAndAwaitTermination(workers);
        }));

        // Note: set work to do.
        Consumer<ConsumerRecord<String, String>> consumerRecordConsumer = PostConsumer.consumePostRecord();

        for (ConsumerWithSubscription bomReader : bomReaders) {
            workers.submit(() -> bomReader.consume(consumerRecordConsumer));
        }


        for (; ; ) ;
    }
}
