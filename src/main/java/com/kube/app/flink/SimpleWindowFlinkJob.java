package com.kube.app.flink;

import com.kube.app.entity.Transaction;
import com.kube.app.entity.TransactionAccountAggregation;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class SimpleWindowFlinkJob {


    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // Use LocalStreamEnvironment for local execution
        LocalStreamEnvironment localEnv = StreamExecutionEnvironment.createLocalEnvironment();

        // Create a stream of records with event times
        DataStream<Transaction> eventsStream = localEnv
                .fromElements(
                        new Transaction("1", 109l, 100.0),
                        new Transaction("1", 107l, 100.0),
                        new Transaction("1", 118l, 100.0),
                        new Transaction("1", 121l, 100.0),
                        new Transaction("1", 1001L, 100.0),
                        new Transaction("1", 1011L, 100.0),
                        new Transaction("1", 1008L, 100.0)
                )
                // Assign event time timestamps to the stream elements
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Transaction>forBoundedOutOfOrderness(Duration.ofMillis(5))
                                .withTimestampAssigner((event, timestamp) -> event.getTransactionTime())
                );

        // Process the events using allowed lateness
        SingleOutputStreamOperator<TransactionAccountAggregation> processedStream = eventsStream
                .map(event -> new TransactionAccountAggregation(event.getAccountId(), event.getAmount(), 1))
                .keyBy(event -> event.accountId)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(10)))
                .allowedLateness(Time.milliseconds(3))
//                .reduce((event1, event2) -> new TransactionAccountAggregation(event1.accountId, event1.totalAmount + event2.totalAmount, event1.totalNumber + event2.totalNumber));
                .process(new ProcessWindowFunction<TransactionAccountAggregation, TransactionAccountAggregation, String, TimeWindow>() {
                    @Override
                    public void process(String accountId, Context context, Iterable<TransactionAccountAggregation> elements, Collector<TransactionAccountAggregation> out) throws Exception {
                        for (TransactionAccountAggregation element : elements) {
                            out.collect(element);
                        }
                    }
                });

        // Print the processed events
        processedStream.print();

        // Execute the Flink job
        localEnv.execute("Event Time Processing Example");
    }
//
//    public static void main2(String[] args) throws Exception {
//        // Set up the execution environment
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        // Set the time characteristic to EventTime
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//
//        // Create a stream of records with event times
//        DataStream<Transaction> eventStream = env.fromElements(
//                new Transaction("1", 109l, 100.0),
//                new Transaction("1", 107l, 100.0),
//                new Transaction("1", 118l, 100.0),
//                new Transaction("1", 121l, 100.0),
//                new Transaction("1", 1001L, 100.0),
//                new Transaction("1", 1011L, 100.0),
//                new Transaction("1", 1008L, 100.0)
//        );
//
//        // Assign event time timestamps and watermarks
//        DataStream<Tuple2<String, Long>> timestampedStream = eventStream.assignTimestampsAndWatermarks(
//                new AscendingTimestampExtractor<Tuple2<String, Long>>() {
//                    @Override
//                    public long extractAscendingTimestamp(Tuple2<String, Long> element) {
//                        return element.f1; // Event time is the second field in the tuple
//                    }
//                }
//        );
//
//        // Apply TumblingEventTimeWindows with a window size of 2 minutes
//        DataStream<String> resultStream = timestampedStream
//                .keyBy(data -> data.f0) // Key by the record field
//                .window(TumblingEventTimeWindows.of(Time.minutes(2)))
//                .sum(1) // Sum the values in the second field of the tuple
//                .map(result -> result.f0 + ": " + result.f1); // Format the result
//
//        // Print the result stream
//        resultStream.print();
//
//        // Execute the Flink job
//        env.execute("TumblingEventTimeWindows Example");
//    }

}
