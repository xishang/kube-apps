package com.kube.app.entity;

public class TransactionAccountAggregation {

    public String accountId;
    public double totalAmount;
    public int totalNumber;

    public TransactionAccountAggregation() {
    }

    public TransactionAccountAggregation(String accountId, double totalAmount, int totalNumber) {
        this.accountId = accountId;
        this.totalAmount = totalAmount;
        this.totalNumber = totalNumber;
    }

    public String toString() {
        return "TransactionAccountAggregation{" + "accountId='" + accountId + '\'' + ", totalAmount='" + totalAmount + '\'' + ", totalNumber=" + totalNumber + '}';
    }

}
