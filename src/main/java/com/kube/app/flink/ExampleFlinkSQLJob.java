package com.kube.app.flink;

import com.kube.app.common.TimestampTransfer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.*;

public class ExampleFlinkSQLJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

//        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
//        TableEnvironment tEnv = TableEnvironment.create(settings);

        tEnv.executeSql("CREATE TABLE transactions (\n" +
                "    account_id  VARCHAR,\n" +
                "    amount      DOUBLE,\n" +
                "    transaction_time TIMESTAMP(3),\n" +
                "    WATERMARK FOR transaction_time AS transaction_time - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic'     = 'transactions',\n" +
                "    'properties.group.id'  = 'flink-consumer-group'," +
                "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'format'    = 'json'\n" +
                ")");

        tEnv.executeSql("CREATE TABLE account_aggregation (\n" +
                "    account_id VARCHAR,\n" +
                "    window_time     TIMESTAMP(3),\n" +
                "    total_number     BIGINT\n," +
                "    total_amount     DOUBLE\n," +
                "    average_amount     DOUBLE\n," +
                "    PRIMARY KEY (account_id, window_time) NOT ENFORCED" +
                ") WITH (\n" +
                "   'connector'  = 'jdbc',\n" +
                "   'url'        = 'jdbc:mysql://localhost:3306/statistics',\n" +
                "   'table-name' = 'account_aggregation',\n" +
                "   'driver'     = 'com.mysql.cj.jdbc.Driver',\n" +
                "   'username'   = 'root',\n" +
                "   'password'   = '123456'\n" +
                ")");

        Table transactions = tEnv.from("transactions");
        report(transactions).executeInsert("account_aggregation").print();


        env.execute("Flink Table API: Kafka -> MySQL");
    }

    public static Table report(Table transactions) {
        return transactions
                .window(Tumble.over(lit(1).minute()).on($("transaction_time")).as("window_time"))
                .groupBy($("account_id"), $("window_time"))
                .select(
                        $("account_id"),
                        $("window_time").start().as("window_time"),
                        $("amount").count().as("total_number"),
                        $("amount").sum().as("total_amount"),
                        $("amount").avg().as("average_amount")
                );
    }

}
