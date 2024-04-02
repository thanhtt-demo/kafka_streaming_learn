package com.vpbank.kafka.streams;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.json.JSONObject;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class BankBalanceProducer {
    private static final Logger log = LoggerFactory.getLogger(BankBalanceProducer.class.getSimpleName());
    public static void main(String[] args) {
        log.info("I am a Kafka Producer!");

        // create Producer Properties
        Properties properties = new Properties();

        // connect to kafka cluster
        properties.setProperty("bootstrap.servers", "10.9.18.10:9092");

        // set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // set batch size to 100 messages
        // properties.setProperty("batch.size", "16384"); // 16 KB batch size
        properties.setProperty("linger.ms", "1000"); // 1 second delay before sending the batch
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        
        // set partitioner class by key
        // properties.setProperty("partitioner.class", "org.apache.kafka.clients.producer.internals.DefaultPartitioner");
        // properties.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, "org.apache.kafka.clients.producer.internals.DefaultPartitioner");
        
        // ensure we don't push duplicates messages
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");


        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        AtomicInteger count = new AtomicInteger(0);

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        Runnable task = () -> {

            for (int i = 0; i <= 10; i++) {

                String topic = "bank-balance-input";
                // create string random from array list ["thanh", "hang", "chau", "thuat",
                // "thuan", "hieu"]
                List<String> names = Arrays.asList("thanh", "hang", "chau", "thuat", "thuan", "hieu");
                String customerName = names.get((int) (Math.random() * 6));
                String key = customerName;
                Integer amount = ThreadLocalRandom.current().nextInt(0, 100);

                JSONObject json = new JSONObject();
                json.put("name", customerName);
                json.put("amount", amount);

                // LocalDateTime now = LocalDateTime.now();
                json.put("time", Instant.now().toString());

                String value = json.toString();

                // create a Producer Record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

                // send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        // executes every time a record successfully sent or an exception is thrown
                        if (e == null) {
                            // the record was successfully sent
                            log.info("Key: " + key + " | Partition: " + metadata.partition() + " | Offset: "
                                    + metadata.offset() + " | Timestamp: " + metadata.timestamp());
                        } else {
                            log.error("Error while producing", e);
                        }
                    }
                });
            }

            // tell the producer to send all data and block until done -- synchronous
            producer.flush();

            if (count.incrementAndGet() > 10) {
                // close the producer
                producer.close();
                // shut down the executor service after some time
                executor.shutdown();
                try {
                    // wait for the executor service to terminate
                    if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                        // if the executor service doesn't terminate within the given time, force it to shut down
                        executor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    // if the current thread is interrupted while waiting, force the executor service to shut down
                    executor.shutdownNow();
                }
            }

        };

        executor.scheduleAtFixedRate(task, 0, 3, TimeUnit.SECONDS);
    }
}
