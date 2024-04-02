package com.vpbank.kafka.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Instant;
import java.util.Properties;

public class BankBalanceCalcUdemy {
    /**
     * The main method of the StreamsStarterApp class.
     * This method sets up the Kafka Streams configuration and performs word count
     * on a stream of text data.
     * It streams data from a Kafka topic, performs word count by splitting each
     * value into words, and writes the results to another Kafka topic.
     * The method starts a KafkaStreams application and gracefully shuts it down
     * when a shutdown signal is received.
     *
     * @param args The command line arguments passed to the application (not used in
     *             this code).
     */

    private static JsonNode newBalance(JsonNode transaction, JsonNode balance) {
        // create a new balance json object
        ObjectNode newBalance = JsonNodeFactory.instance.objectNode();
        newBalance.put("count", balance.get("count").asInt() + 1);
        newBalance.put("balance", balance.get("balance").asInt() + transaction.get("amount").asInt());

        // DateTimeFormatter formatter = DateTimeFormatter.ISO_DATE_TIME;
        Long balanceEpoch = Instant.parse(balance.get("time").asText()).toEpochMilli();
        Long transactionEpoch = Instant.parse(transaction.get("time").asText()).toEpochMilli();
        Instant newBalanceInstant = Instant.ofEpochMilli(Math.max(balanceEpoch, transactionEpoch));
        newBalance.put("time", newBalanceInstant.toString());
        return newBalance;
    }
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "udemy-stream-bank-balance-app"); // equal to consumer group id
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "10.9.18.10:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

        // props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, jsonSerde.getClass());
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/home/ec2-user/tmp/udemy-tream-bank-balance-app" + Long.valueOf(ProcessHandle.current().pid()).toString());

        // write comment
        // disible the cache to show every step involved in the transformation. non prod
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        // json Serde
        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
        final Consumed<String, JsonNode> consumed = Consumed.with(Serdes.String(), jsonSerde);

        StreamsBuilder builder = new StreamsBuilder();

        // Stream from Kafka
        KStream<String, JsonNode> textLines = builder.stream("bank-balance-input", consumed);
        

        // create the initial json object for balances
        ObjectNode initialBalance = JsonNodeFactory.instance.objectNode();
        initialBalance.put("count", 0);
        initialBalance.put("balance", 0);
        initialBalance.put("time", Instant.ofEpochMilli(0L).toString());

        // aggregate value by key
        KTable<String, JsonNode> groupedTable = textLines.groupByKey().aggregate(
            () -> initialBalance,
            (key, value, aggregate) -> newBalance(value, aggregate),
            Materialized.<String, JsonNode, KeyValueStore<Bytes, byte[]>>as("bank-balance-agg")
                .withKeySerde(Serdes.String())
                .withValueSerde(jsonSerde)
        );

        groupedTable.toStream().to("bank-balance-output-udemy", Produced.with(Serdes.String(), jsonSerde));

        

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