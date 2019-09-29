package com.chriniko.kafkaread.demonstration.a_option;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

public class ConsumerWithSubscription {

    private final KafkaConsumer<String, String> consumer;

    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets;

    private final CountDownLatch countDownLatch;

    public ConsumerWithSubscription(String bootstrap, String groupId, String kafkaTopic) {
        countDownLatch = new CountDownLatch(1);

        currentOffsets = new LinkedHashMap<>();

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

        properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, StickyAssignor.class.getName());

        consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(
                Collections.singletonList(kafkaTopic),
                new ConsumerRebalanceListener() {
                    @Override
                    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                        System.out.println("[" + Thread.currentThread().getName() + "] >>> Lost partitions in rebalance. Committing current offsets: " + currentOffsets);
                        consumer.commitSync(currentOffsets);
                        currentOffsets.clear();
                    }

                    @Override
                    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                        // Note: nothing for our use case at the moment.
                    }
                });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[" + Thread.currentThread().getName() + "] >>> Starting exit...");
            consumer.wakeup();

            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));
    }


    public void consume(Consumer<ConsumerRecord<String, String>> consumerRecordConsumer) {
        try {
            while (true) {

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                if (records.isEmpty()) {
                    continue;
                }

                for (ConsumerRecord<String, String> record : records) {

                    // Note: still at least once delivery semantic possibility here
                    // >>>START: THIS SHOULD BE AN ATOMIC OPERATION IN ORDER TO HAVE EXACTLY ONCE DELIVERY SEMANTIC
                    consumerRecordConsumer.accept(record);

                    currentOffsets.put(
                            new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1)
                    );

                    // Note: Option B
                    /*
                    try {
                        consumer.commitSync(currentOffsets);
                        currentOffsets.clear();
                    } catch (CommitFailedException e) {
                        System.out.println("[" + Thread.currentThread().getName() + "] >>> Failed to commit record");
                    }
                    */
                    // >>>END: THIS SHOULD BE AN ATOMIC OPERATION IN ORDER TO HAVE EXACTLY ONCE DELIVERY SEMANTIC
                }

                // Note: Option A
                consumer.commitAsync(currentOffsets, (offsets, error) -> {
                    if (error == null) {
                        currentOffsets.clear();
                    } else {
                        System.out.println("[" + Thread.currentThread().getName() + "] >>> Failed to commit record");
                    }
                });

            }
        } catch (WakeupException e) {
            // Note: ignore, we are closing.
        } catch (Exception e) {
            System.err.println("[" + Thread.currentThread().getName() + "] >>> ERROR OCCURRED: " + e);
        } finally {
            try {
                System.out.println("[" + Thread.currentThread().getName() + "] >>> COMMITTING BEFORE EXITING...");
                consumer.commitSync(currentOffsets);
            } finally {
                consumer.close();
            }
        }
    }

    public void exit() {
        countDownLatch.countDown();
    }

}
