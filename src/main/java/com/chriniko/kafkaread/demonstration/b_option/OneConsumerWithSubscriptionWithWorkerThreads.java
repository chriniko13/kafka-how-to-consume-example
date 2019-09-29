package com.chriniko.kafkaread.demonstration.b_option;

import com.chriniko.kafkaread.demonstration.core.Pools;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class OneConsumerWithSubscriptionWithWorkerThreads {

    private final KafkaConsumer<String, String> consumer;

    private final ExecutorService workers;

    private final ConcurrentHashMap<TopicPartition, BlockingQueue<ConsumerRecord<String, String>>> commitQueueMap;

    public OneConsumerWithSubscriptionWithWorkerThreads(String bootstrap, String groupId, String kafkaTopic) {

        commitQueueMap = new ConcurrentHashMap<>();

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

        consumer.subscribe(Collections.singletonList(kafkaTopic));

        final int noOfWorkers = consumer.partitionsFor(kafkaTopic).size();

        workers = Executors.newFixedThreadPool(noOfWorkers, new ThreadFactory() {
            private AtomicInteger idx = new AtomicInteger();

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName("bom-reader-slave-worker-" + idx.incrementAndGet());
                return t;
            }
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[" + Thread.currentThread().getName() + "] >>> Starting exit...");

            consumer.wakeup();
            Pools.shutdownAndAwaitTermination(workers);
        }));
    }

    public void search(Consumer<ConsumerRecord<String, String>> consumerRecordConsumer) {
        try {
            while (true) {

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                if (records.isEmpty()) {
                    continue;
                }

                workers.submit(new ReaderSlaveWorker(consumerRecordConsumer, records, commitQueueMap));

                processCommits();
            }
        } catch (WakeupException e) {
            // Note: ignore, we are closing.
        } catch (Exception e) {
            System.err.println("[" + Thread.currentThread().getName() + "] >>> ERROR OCCURRED: " + e);
        } finally {
            consumer.close();
        }
    }

    private void processCommits() {

        commitQueueMap.entrySet().forEach(entry -> {

            TopicPartition topicPartition = entry.getKey();
            BlockingQueue<ConsumerRecord<String, String>> queue = entry.getValue();

            // Note: find highest offset for the current topic partition processed.
            ConsumerRecord<String, String> consumerRecord = queue.poll();
            ConsumerRecord<String, String> highestOffset = consumerRecord;

            while (consumerRecord != null) {
                if (consumerRecord.offset() > highestOffset.offset()) {
                    highestOffset = consumerRecord;
                }
                consumerRecord = queue.poll();
            }

            if (highestOffset != null) {
                System.out.println("[" + Thread.currentThread().getName() + "] >>> Sending commit topic-partition: " + topicPartition + ", offset: " + highestOffset.offset());
                try {
                    consumer.commitSync(
                            Collections.singletonMap(
                                    topicPartition,
                                    new OffsetAndMetadata(highestOffset.offset() + 1)
                            )
                    );
                } catch (CommitFailedException e) {
                    System.out.println("[" + Thread.currentThread().getName() + "] >>> Failed to commit record");
                }
            }

        });
    }

}
