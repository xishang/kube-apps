package com.kube.app.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kube.app.entity.Transaction;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

public class TransactionDeserializer implements Deserializer<Transaction> {

    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Transaction deserialize(String topic, byte[] data) {
        try {
            Transaction transaction = objectMapper.readValue(data, Transaction.class);
            return transaction;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

}
