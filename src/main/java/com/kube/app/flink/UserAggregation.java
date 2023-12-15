package com.kube.app.flink;

public class UserAggregation {

    public String accountId;
    public int count;
    public double totalAmount;

    public UserAggregation(String accountId, int count, double totalAmount) {
        this.accountId = accountId;
        this.count = count;
        this.totalAmount = totalAmount;
    }

}
