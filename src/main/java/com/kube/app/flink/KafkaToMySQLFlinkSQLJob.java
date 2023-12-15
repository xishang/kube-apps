package com.kube.app.flink;

import com.kube.app.common.TimestampTransfer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Scanner;

import static org.apache.flink.table.api.Expressions.*;

public class KafkaToMySQLFlinkSQLJob {

    public static void main(String[] args) throws Exception {
//        String sourceSQL, sinkSQL;
//        try {
//            sourceSQL = new String(Files.readAllBytes(Paths.get("/Users/shang_xi/kube-apps/src/main/resources/transaction.sql")));
//            sinkSQL = new String(Files.readAllBytes(Paths.get("/Users/shang_xi/kube-apps/src/main/resources/account_aggregation.sql")));
//        } catch (IOException e) {
//            throw new RuntimeException("Failed to read query.sql file", e);
//        }
        String sourceSQL = "CREATE TABLE transactions (\n" +
                "    account_id VARCHAR,\n" +
                "    transaction_time TIMESTAMP(3),\n" +
                "    amount DOUBLE\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'transactions',\n" +
                "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'format' = 'json'\n" +
                ")";
        String sinkSQL = "CREATE TABLE account_aggregation (\n" +
                "    account_id VARCHAR,\n" +
                "    window_time TIMESTAMP(3),\n" +
                "    window_start TIMESTAMP(3),\n" +
                "    window_end TIMESTAMP(3),\n" +
                "    total_number BIGINT,\n" +
                "    total_amount DOUBLE,\n" +
                "    average_amount DOUBLE,\n" +
                "    PRIMARY KEY (account_id, window_time) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:mysql://mysql:3306/statistics',\n" +
                "    'table-name' = 'account_aggregation',\n" +
                "    'driver' = 'com.mysql.jdbc.Driver',\n" +
                "    'username' = 'root',\n" +
                "    'password' = '123456'\n" +
                ")";


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql(sourceSQL);
        tEnv.executeSql(sinkSQL);
//                window(Tumble.over(lit(1).minute()).on(call(TimestampTransfer.class, $("transaction_time"))).as("window_time"))
        Table transactions = tEnv.from("transactions");

        Table aggregation = transactions
                .window(Tumble.over(lit(1).minute()).on($("transaction_time")).as("window_time"))
                .groupBy($("account_id"), $("window_time"))
                .select(
                        $("account_id"),
                        $("window_time").start().as("window_start"),
                        $("amount").sum().as("average_amount"));

//        Table aggregation = transactions
//                .window(Tumble.over(lit(1).minute()).on($("transaction_time")).as("window_time"))
//                .window(Tumble.over(lit(1).hour()).on(call(TimestampTransfer.class, $("transaction_time"))).as("window_time"))
//                .groupBy($("account_id"), $("window_time"))
//                .select(
//                        $("account_id"),
//                        $("window_time"),
//                        $("window_time").start().as("window_start"),
//                        $("window_time").end().as("window_end"),
//                        $("amount").count().as("total_count"),
//                        $("amount").sum().as("total_amount"),
//                        $("amount").avg().as("average_amount")
//                );
        aggregation.executeInsert("account_aggregation");

        env.execute("Flink Table API: Kafka -> MySQL");
    }

}
