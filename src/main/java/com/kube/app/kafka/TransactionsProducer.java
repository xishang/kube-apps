package com.kube.app.kafka;

import com.kube.app.entity.Transaction;
import com.kube.app.common.Constants;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class TransactionsProducer {

    public static void main(String[] args) {
        // Create a new topic
//        createTopic();

        // Send a message to the Kafka topic
        sendMessage("Hello, Kafka!");
    }

    private static void createTopic() {
        try (AdminClient adminClient = AdminClient.create(getAdminConfig())) {
            NewTopic newTopic = new NewTopic(Constants.TOPIC_TRANSACTIONS, 1, (short) 1);
            adminClient.createTopics(Collections.singleton(newTopic)).all().get();
            System.out.println("Topic created: " + Constants.TOPIC_TRANSACTIONS);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    private static List<Transaction> generateMessages() {
        List<Transaction> messages = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Transaction transaction = new Transaction();
            transaction.setAccountId("account-" + i);
            transaction.setTransactionTime(System.currentTimeMillis());
            transaction.setAmount(new Random().nextDouble());
            messages.add(transaction);
        }
        return messages;
    }

    private static void sendMessage(String message) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_BROKER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, TransactionSerializer.class.getName());

        // Create the Kafka producer
        Producer<String, Transaction> producer = new KafkaProducer<>(props);

        for (Transaction transaction : generateMessages()) {
            // Create and send a message
            ProducerRecord<String, Transaction> record = new ProducerRecord<>(Constants.TOPIC_TRANSACTIONS, transaction);
            producer.send(record);
            System.out.println("Message send: " + transaction);
        }

        // Close the producer
        producer.close();
    }

    private static Properties getAdminConfig() {
        Properties props = new Properties();
        props.put("bootstrap.servers", Constants.KAFKA_BROKER);
        return props;
    }

}
