package com.vpbank.kafka.streams;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.json.JSONObject;
import org.slf4j.event.KeyValuePair;

public class BankBalanceCalc {
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
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-bank-balance-app"); // equal to consumer group id
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "10.9.18.10:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

        props.put(StreamsConfig.STATE_DIR_CONFIG, "/home/ec2-user/tmp/stream-bank-balance-app" + Long.valueOf(ProcessHandle.current().pid()).toString());

        // write comment
        // disible the cache to show every step involved in the transformation. non prod
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        StreamsBuilder builder = new StreamsBuilder();

        // Stream from Kafka
        KStream<String, String> textLines = builder.stream("bank-balance-input");
        
        // aggregate value by key
        KTable<String, Long> groupedTable = textLines.groupByKey().aggregate(
            () -> 0L,
            (key, value, aggregate) -> aggregate + new JSONObject(value).getLong("amount"),
            Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("bank-balance-agg-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Long())
        );

        groupedTable.toStream().to("bank-balance-output", Produced.with(Serdes.String(), Serdes.Long()));

        

        // Build topology
        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();

        // Attach shutdown hook to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
            }

        });

    }
}