package com.chriniko.kafkaread.demonstration.c_option;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class PartitionsAssignedConsumerWorker implements Runnable {



    private final Consumer<ConsumerRecord<String, String>> consumerRecordConsumer;

    private final KafkaConsumer<String, String> consumer;

    public PartitionsAssignedConsumerWorker(List<PartitionInfo> assignedPartitionInfos,
                                            String bootstrap,
                                            String groupId,
                                            Consumer<ConsumerRecord<String, String>> consumerRecordConsumer) {

        this.consumerRecordConsumer = consumerRecordConsumer;

        Properties properties = new Properties();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 4000);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);

        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 7000);

        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        consumer = new KafkaConsumer<>(properties);

        List<TopicPartition> topicPartitions = assignedPartitionInfos
                .stream()
                .map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
                .collect(Collectors.toList());

        consumer.assign(topicPartitions);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[" + Thread.currentThread().getName() + "] >>> Starting exit...");
            consumer.wakeup();
        }));
    }

    @Override
    public void run() {

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                if (records.isEmpty()) {
                    continue;
                }
                records.forEach(consumerRecordConsumer);
                consumer.commitSync();
            }
        } catch (WakeupException e) {
            // Note: ignore, we are closing.
        } catch (Exception e) {
            System.err.println("[" + Thread.currentThread().getName() + "] >>> ERROR OCCURRED: " + e);
        } finally {
            consumer.close();
        }

    }
}
