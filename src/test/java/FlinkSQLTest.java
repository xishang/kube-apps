import com.kube.app.common.TimestampTransfer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.api.Expressions.*;
import static org.apache.flink.table.api.Expressions.$;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class FlinkSQLTest {

    @Test
    public void testSQLExecution() throws Exception {
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


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql(sourceSQL);
        tEnv.executeSql(sinkSQL);

        Table transactions = tEnv.from("transactions");
        Table aggregation = transactions.
                window(Tumble.over(lit(1).minute()).on(call(TimestampTransfer.class, $("transaction_time"))).as("window_time"))
                .groupBy($("account_id"), $("window_time"))
                .select(
                        $("account_id"),
                        $("window_time"),
                        $("window_time").min().as("window_start"),
                        $("window_time").max().as("window_end"),
                        $("amount").count().as("total_count"),
                        $("amount").sum().as("total_amount"),
                        $("amount").sum().dividedBy($("amount").count()).as("average_amount")
                );
        aggregation.executeInsert("account_aggregation");

        env.execute("Flink Table API: Kafka -> MySQL");

    }
}
