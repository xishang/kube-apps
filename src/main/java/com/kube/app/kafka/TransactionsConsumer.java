package com.kube.app.kafka;

import com.kube.app.entity.Transaction;
import com.kube.app.common.Constants;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class TransactionsConsumer {


    public static void main(String[] args) {
        // Configure the Kafka consumer
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_BROKER);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, TransactionDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, Constants.KAFKA_CONSUMER_GROUP_LOCAL);

        // Create the Kafka consumer
        Consumer<String, Transaction> consumer = new KafkaConsumer<>(props);

        // Subscribe to the topic
        consumer.subscribe(Collections.singletonList(Constants.TOPIC_TRANSACTIONS));

        // Start consuming messages
        while (true) {
            ConsumerRecords<String, Transaction> records = consumer.poll(Duration.ofMillis(100));
            // Process the records
            for (ConsumerRecord<String, Transaction> record : records) {
                String key = record.key();
                Transaction value = record.value();
                System.out.println("Received message: key=" + key + ", value=" + value);
            }
        }
    }

}
