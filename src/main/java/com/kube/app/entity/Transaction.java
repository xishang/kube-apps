package com.kube.app.entity;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class Transaction {

    private String accountId;
    private Long transactionTime;
    private Double amount;

    public Transaction() {
    }

    public Transaction(String accountId, Long transactionTime, Double amount) {
        this.accountId = accountId;
        this.transactionTime = transactionTime;
        this.amount = amount;
    }

    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    public Long getTransactionTime() {
        return transactionTime;
    }

    public void setTransactionTime(Long transactionTime) {
        this.transactionTime = transactionTime;
    }

    public Double getAmount() {
        return amount;
    }

    public void setAmount(Double amount) {
        this.amount = amount;
    }

    public static Transaction fromJson(String json) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(json, Transaction.class);
    }

    @Override
    public String toString() {
        return "Transaction{" + "accountId='" + accountId + '\'' + ", transactionTime='" + transactionTime + '\'' + ", amount=" + amount + '}';
    }

}
