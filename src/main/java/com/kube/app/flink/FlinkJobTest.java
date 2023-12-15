//package com.kube.app.flink;
//
//import com.kube.app.entity.Transaction;
//import com.kube.app.entity.TransactionAccountAggregation;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.api.common.typeinfo.Types;
//import org.apache.flink.streaming.api.TimeCharacteristic;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
//import org.apache.flink.streaming.api.windowing.time.Time;
//import org.apache.flink.test.util.AbstractTestBase;
//import org.apache.flink.test.util.TestBaseUtils;
//import org.junit.Test;
//
//import java.time.Duration;
//
//public class FlinkJobTest extends AbstractTestBase {
//
//    @Test
//    public void testFlinkJob() throws Exception {
//        // Set up the test environment
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        // Create the input data stream
//        DataStream<Integer> input = env.fromElements(1, 2, 3, 4, 5);
//
//        // Apply transformations and operations on the input stream
//        DataStream<Integer> output = input
//                .map(value -> value * 2)
//                .filter(value -> value > 5);
//
//        // Execute the Flink job
//        env.execute();
//
//    }
//
//    @Test
//    public void test2() throws Exception {
//        // Set up the test environment
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//
//        // Create a stream of records with event times
//        DataStream<Transaction> eventsStream = env
//                .fromElements(
//                        new Transaction("1", 109l, 100.0),
//                        new Transaction("1", 107l, 100.0),
//                        new Transaction("1", 118l, 100.0),
//                        new Transaction("1", 121l, 100.0),
//                        new Transaction("1", 1001L, 100.0),
//                        new Transaction("1", 1011L, 100.0),
//                        new Transaction("1", 1008L, 100.0)
//                )
//                // Assign event time timestamps to the stream elements
//                .assignTimestampsAndWatermarks(
//                        WatermarkStrategy
//                                .<Transaction>forBoundedOutOfOrderness(Duration.ofMillis(5))
//                                .withTimestampAssigner((event, timestamp) -> event.getTransactionTime())
//                );
//
//        // Process the events using allowed lateness
//        SingleOutputStreamOperator<TransactionAccountAggregation> processedStream = eventsStream
//                .map(event -> new TransactionAccountAggregation(event.getAccountId(), event.getAmount(), 1))
//                .keyBy(event -> event.accountId)
//                .window(TumblingEventTimeWindows.of(Time.milliseconds(10)))
//                .allowedLateness(Time.seconds(2))
//                .reduce((event1, event2) -> new TransactionAccountAggregation(event1.accountId, event1.totalAmount + event2.totalAmount, event1.totalNumber + event2.totalNumber));
//
//        // Print the processed events
//        processedStream.print();
//
//        // Execute the Flink job
//        env.execute("Event Time Processing Example");
//
//    }
//
//}
