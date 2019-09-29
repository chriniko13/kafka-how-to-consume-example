package com.chriniko.kafkaread.demonstration.f_option;

import akka.Done;
import akka.NotUsed;
import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.japi.Pair;
import akka.kafka.*;
import akka.kafka.javadsl.Committer;
import akka.kafka.javadsl.Consumer;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.chriniko.kafkaread.demonstration.core.PostConsumer;
import com.chriniko.kafkaread.demonstration.core.Pools;
import com.typesafe.config.Config;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ForkJoinPool;

public class ConsumerWithAlpakka {

    private final ForkJoinPool workers = new ForkJoinPool();

    private final ConsumptionOption consumptionOption = ConsumptionOption.C;

    enum ConsumptionOption {
        A, B, C
    }

    public ConsumerWithAlpakka() {

        Runtime.getRuntime().addShutdownHook(new Thread(() -> Pools.shutdownAndAwaitTermination(workers)));

        final String groupId = "f-option" + UUID.randomUUID().toString();

        final ActorSystem system = ActorSystem.create();
        final ActorMaterializer materializer = ActorMaterializer.create(system);

        final Config akkaKafkaConsumerConfig = system.settings().config().getConfig("akka.kafka.consumer");

        final ConsumerSettings<String, String> consumerSettings =
                ConsumerSettings.create(akkaKafkaConsumerConfig, new StringDeserializer(), new StringDeserializer())
                        .withBootstrapServers("localhost:9092")
                        .withGroupId(groupId)
                        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Config akkaKafkaCommitterConfig = system.settings().config().getConfig("akka.kafka.committer");
        CommitterSettings committerSettings = CommitterSettings.apply(akkaKafkaCommitterConfig);

        ActorRef rebalanceListener = system.actorOf(Props.create(RebalanceListener.class));

        if (consumptionOption == ConsumptionOption.A) {

            Consumer
                    .committableSource(
                            consumerSettings,
                            Subscriptions.topics("posts").withRebalanceListener(rebalanceListener)
                    )
                    .mapAsync(1,
                            msg -> process(msg.record())
                                    .thenApply(done -> msg.committableOffset())
                    )
                    .toMat(Committer.sink(committerSettings), Keep.both())
                    .mapMaterializedValue(Consumer::createDrainingControl)
                    .run(materializer);

        } else if (consumptionOption == ConsumptionOption.B) {

            int maxPartitions = 12;

            Consumer.committablePartitionedSource(consumerSettings, Subscriptions.topics("posts").withRebalanceListener(rebalanceListener))
                    .flatMapMerge(maxPartitions, Pair::second)
                    .via(Flow.fromFunction(param -> {
                        PostConsumer.consumePostRecord().accept(param.record());
                        return param;
                    }))
                    .map(msg -> msg.committableOffset())
                    .toMat(Committer.sink(committerSettings), Keep.both())
                    .mapMaterializedValue(Consumer::createDrainingControl)
                    .run(materializer);

        } else if (consumptionOption == ConsumptionOption.C) {

            int maxPartitions = 12;

            Consumer.committablePartitionedSource(consumerSettings, Subscriptions.topics("posts").withRebalanceListener(rebalanceListener))
                    .mapAsyncUnordered(
                            maxPartitions,
                            pair -> {
                                Source<ConsumerMessage.CommittableMessage<String, String>, NotUsed> source = pair.second();

                                return source
                                        .via(Flow.fromFunction(param -> {
                                            PostConsumer.consumePostRecord().accept(param.record());
                                            return param;
                                        }))
                                        .map(ConsumerMessage.CommittableMessage::committableOffset)
                                        .runWith(Committer.sink(committerSettings), materializer);

                            })
                    .toMat(Sink.ignore(), Keep.both())
                    .mapMaterializedValue(Consumer::createDrainingControl)
                    .run(materializer);

        }

    }

    private CompletionStage<Done> process(ConsumerRecord<String, String> record) {
        return CompletableFuture.supplyAsync(
                () -> {
                    PostConsumer.consumePostRecord().accept(record);
                    return Done.done();
                },
                workers
        );
    }

    static class RebalanceListener extends AbstractLoggingActor {

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(
                            TopicPartitionsAssigned.class,
                            assigned -> {
                                log().info("Assigned: {}", assigned);
                            })
                    .match(
                            TopicPartitionsRevoked.class,
                            revoked -> {
                                log().info("Revoked: {}", revoked);
                            })
                    .build();
        }
    }

}
