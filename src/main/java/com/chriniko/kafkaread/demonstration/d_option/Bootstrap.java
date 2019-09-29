package com.chriniko.kafkaread.demonstration.d_option;

import com.chriniko.kafkaread.demonstration.core.PostConsumer;
import com.chriniko.kafkaread.demonstration.core.PostProducer;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

public class Bootstrap {


    public static void run() {

        // Note: create db table
        try(Connection connection = DBUtil.getDataSource().getConnection();
            Statement statement = connection.createStatement()) {

            try {
                connection.setAutoCommit(false);

                statement.execute("DROP TABLE IF EXISTS test.posts");
                statement.execute("CREATE TABLE posts(" +
                        "  id varchar(70)," +
                        "  postAsJson JSON," +
                        "  topicName VARCHAR(70)," +
                        "  topicPartition bigint," +
                        "  partitionOffset bigint," +
                        "  PRIMARY KEY (id)" +
                        ")");

                connection.commit();
            } catch (SQLException e) {
                connection.rollback();
                throw e;
            }

        } catch (SQLException e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
        }

        // Note: start producing records to kafka
        new Thread(() -> new PostProducer().run()).start();


        try {
            new ConsumerWithSubscriptionExactlyOnce(
                    "localhost:9092",
                    "d-option"+ UUID.randomUUID().toString(),
                    PostConsumer.consumePostRecord()
            ).consume();
        } catch (SQLException e) {
            e.printStackTrace();
        }


    }
}
