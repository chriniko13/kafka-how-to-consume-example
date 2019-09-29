package com.chriniko.kafkaread.demonstration.d_option;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.function.Consumer;

public class ConsumerWithSubscriptionExactlyOnce {

    private final Consumer<ConsumerRecord<String, String>> consumerRecordConsumer;
    private final KafkaConsumer<String, String> consumer;
    private final Connection connection;

    public ConsumerWithSubscriptionExactlyOnce(String bootstrap,
                                               String groupId,
                                               Consumer<ConsumerRecord<String, String>> consumerRecordConsumer) throws SQLException {

        this.consumerRecordConsumer = consumerRecordConsumer;

        connection = DBUtil.getDataSource().getConnection();
        connection.setAutoCommit(false);

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
        consumer.subscribe(Collections.singletonList("posts"), new SaveOffsetsOnRebalance(connection, consumer));
        consumer.poll(0);

        for (TopicPartition topicPartition : consumer.assignment()) {
            consumer.seek(topicPartition, getOffsetFromDB(topicPartition));
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Starting exit...");
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            consumer.wakeup();
        }));
    }

    public void consume() throws SQLException {
        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    // TX START
                    try {
                        processRecord(consumerRecord);
                        storeRecordInDB(connection, consumerRecord);
                        connection.commit();
                    } catch (Exception error) {
                        error.printStackTrace();
                        connection.rollback();
                    }
                    // TX END
                }
            }
        } catch (WakeupException e) {
            // ignore for shutdown
        } finally {
            consumer.close();
            System.out.println("Closed consumer and we are done");
        }
    }

    private void storeRecordInDB(Connection connection,
                                 ConsumerRecord<String, String> consumerRecord) throws SQLException {

        try (Statement statement = connection.createStatement()) {

            String key = consumerRecord.key();
            String value = consumerRecord.value();
            String topic = consumerRecord.topic();
            int partition = consumerRecord.partition();
            long offset = consumerRecord.offset();

            statement.execute("INSERT INTO test.posts(id, postAsJson, topicName, topicPartition, partitionOffset) " +
                    "VALUES ('" + key + "', '" + value + "', '" + topic + "'," + partition + ", " + offset + ")");
        }
    }

    private void processRecord(ConsumerRecord<String, String> consumerRecord) {
        consumerRecordConsumer.accept(consumerRecord);
    }

    private long getOffsetFromDB(TopicPartition topicPartition) throws SQLException {
        // for given topic name and partition, find max offset.
        String topic = topicPartition.topic();
        int partition = topicPartition.partition();

        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery("SELECT MAX(partitionOffset) FROM test.posts " +
                    "WHERE topicName='" + topic + "' AND topicPartition = " + partition)) {
                if (rs.first()) {
                    return rs.getLong(1);
                } else return 0;
            }
        }
    }

    // ---

    private class SaveOffsetsOnRebalance implements ConsumerRebalanceListener {

        private final Connection connection;
        private final KafkaConsumer<String, String> consumer;

        public SaveOffsetsOnRebalance(Connection connection,
                                      KafkaConsumer<String, String> consumer) {
            this.connection = connection;
            this.consumer = consumer;
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            // commit db transaction
            try {
                connection.commit();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            for (TopicPartition partition : partitions) {
                try {
                    consumer.seek(partition, getOffsetFromDB(partition));
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
