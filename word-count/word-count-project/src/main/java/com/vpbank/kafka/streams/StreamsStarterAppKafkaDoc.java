package com.vpbank.kafka.streams;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

public class StreamsStarterAppKafkaDoc {
    /**
     * The main method of the StreamsStarterApp class.
     * This method sets up the Kafka Streams configuration and performs word count on a stream of text data.
     * It streams data from a Kafka topic, performs word count by splitting each value into words, and writes the results to another Kafka topic.
     * The method starts a KafkaStreams application and gracefully shuts it down when a shutdown signal is received.
     *
     * @param args The command line arguments passed to the application (not used in this code).
     */
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-word-count-app"); // equal to consumer group id
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "10.9.18.10:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();

        // Stream from Kafka
        KStream<String, String> source = builder.stream("streams-plaintext-input");

        source.flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+"))) // map value to lowercase and split by non-word chars
                .groupBy((key, value) -> value) // group by the values
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"))
                .toStream()
                .to("streams-wordcount-output", Produced.with(Serdes.String(), Serdes.Long()));

        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);

        final CountDownLatch latch = new CountDownLatch(1);

        // Attach shutdown hook to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}