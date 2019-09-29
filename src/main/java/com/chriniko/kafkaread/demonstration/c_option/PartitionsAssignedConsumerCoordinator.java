package com.chriniko.kafkaread.demonstration.c_option;

import com.chriniko.kafkaread.demonstration.core.ListPartitioner;
import com.chriniko.kafkaread.demonstration.core.Pools;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class PartitionsAssignedConsumerCoordinator {

    private final ExecutorService workers;

    private KafkaConsumer<String, String> consumer;

    public PartitionsAssignedConsumerCoordinator(String bootstrap,
                                                 String groupId,
                                                 String kafkaTopic,
                                                 int howManyConsumers,
                                                 Consumer<ConsumerRecord<String, String>> consumerRecordConsumer) {

        // Note: just a lean set of configurations, we only need this to find out the partitions infos of the specified kafka topic.
        Properties properties = new Properties();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        consumer = new KafkaConsumer<>(properties);

        // find the partitions
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(kafkaTopic);

        // split partitions among consumer workers
        List<List<PartitionInfo>> partitionInfosPartitioned = ListPartitioner.partition(partitionInfos, howManyConsumers, true);

        int index = 0;
        for (List<PartitionInfo> infos : partitionInfosPartitioned) {
            System.out.println("~~~~ SPLITTED PARTITIONS LIST " + (++index) + " ~~~~");
            System.out.println(infos);
        }

        // create the consumer workers
        List<PartitionsAssignedConsumerWorker> bomAssignedPartitionsConsumerWorkers = partitionInfosPartitioned
                .stream()
                .map(_partitionInfos -> new PartitionsAssignedConsumerWorker(_partitionInfos, bootstrap, groupId, consumerRecordConsumer))
                .collect(Collectors.toList());

        workers = Executors.newFixedThreadPool(howManyConsumers, new ThreadFactory() {
            private AtomicInteger idx = new AtomicInteger();
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName("bom-assigned-partitions-consumer-worker-" + idx.incrementAndGet());
                return t;
            }
        });

        // start workers
        bomAssignedPartitionsConsumerWorkers.forEach(worker -> workers.submit(worker));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[" + Thread.currentThread().getName() + "] >>> Starting exit...");
            consumer.close();
            Pools.shutdownAndAwaitTermination(workers);
        }));

    }

}
