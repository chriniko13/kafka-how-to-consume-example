package com.chriniko.kafkaread.demonstration.b_option;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

public class ReaderSlaveWorker implements Runnable {

    private final Consumer<ConsumerRecord<String, String>> consumerRecordConsumer;

    private final ConsumerRecords<String, String> records;

    private final ConcurrentHashMap<TopicPartition, BlockingQueue<ConsumerRecord<String, String>>> commitQueueMap;

    public ReaderSlaveWorker(Consumer<ConsumerRecord<String, String>> consumerRecordConsumer,
                             ConsumerRecords<String, String> records,
                             ConcurrentHashMap<TopicPartition, BlockingQueue<ConsumerRecord<String, String>>> commitQueueMap) {
        this.consumerRecordConsumer = consumerRecordConsumer;
        this.records = records;
        this.commitQueueMap = commitQueueMap;
    }

    @Override
    public void run() {
        // Note: consume records.
        records.forEach(consumerRecordConsumer);

        // Note: update correctly the commit queue map in order.
        Set<TopicPartition> topicPartitions = records.partitions();

        for (TopicPartition topicPartition : topicPartitions) {

            BlockingQueue<ConsumerRecord<String, String>> queue = commitQueueMap.get(topicPartition);

            List<ConsumerRecord<String, String>> partitionRecords = this.records.records(topicPartition);

            if (queue != null) {
                queue.addAll(partitionRecords);
            } else {
                LinkedBlockingQueue<ConsumerRecord<String, String>> justCreatedQueue = new LinkedBlockingQueue<>(partitionRecords);
                commitQueueMap.put(topicPartition, justCreatedQueue);
            }
        }

    }
}
