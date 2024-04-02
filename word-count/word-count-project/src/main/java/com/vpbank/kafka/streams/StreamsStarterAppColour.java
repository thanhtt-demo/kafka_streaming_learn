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
import org.slf4j.event.KeyValuePair;

public class StreamsStarterAppColour {
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
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-colour-app"); // equal to consumer group id
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "10.9.18.10:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/home/ec2-user/tmp/stream-colour-app" + Long.valueOf(ProcessHandle.current().pid()).toString());

        // write comment
        // disible the cache to show every step involved in the transformation. non prod
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        StreamsBuilder builder = new StreamsBuilder();

        // Stream from Kafka
        KStream<String, String> textLines = builder.stream("streams-colour-input");
        
        // Filter if value don't have 
        KStream<String, String> realKeyValues = textLines.filter((key, value) -> value.matches("^[a-z]*,(red|white)$"))
                .flatMap(
                        (key, value) -> {
                            List<KeyValue<String, String>> result = new LinkedList<>();
                            result.add(KeyValue.pair(value.split(",")[0], value.split(",")[1]));
                            return result;
                        });

        // Convert stream to table
        KTable<String, String> convertedTable = realKeyValues.toTable(Materialized.as("stream-converted-to-table"));
        
        // count by value
        KTable<String, Long> groupedTable = convertedTable.groupBy((key,value) -> KeyValue.pair(value, key)).count();

        groupedTable.toStream().to("streams-colour-output", Produced.with(Serdes.String(), Serdes.Long()));

        

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