package com.kube.app.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class TumblingEventTimeWindowsExample {

    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Use LocalStreamEnvironment for local execution
        LocalStreamEnvironment localEnv = StreamExecutionEnvironment.createLocalEnvironment();

        // Create a stream of records with event times
        DataStream<String> inputStream = localEnv.fromElements(
                "RecordA,1631301700000", // Event time: 2021-09-10T10:01:40.000Z
                "RecordB,1631301800000", // Event time: 2021-09-10T10:03:20.000Z
                "RecordC,1631301900000", // Event time: 2021-09-10T10:05:00.000Z
                "RecordD,1631302000000"  // Event time: 2021-09-10T10:06:40.000Z
        );

        // Parse the input stream and extract event time and record
        DataStream<Tuple2<String, Long>> parsedStream = inputStream.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) {
                String[] tokens = value.split(",");
                String record = tokens[0];
                long eventTime = Long.parseLong(tokens[1]);
                return Tuple2.of(record, eventTime);
            }
        });

        // Assign event time timestamps and watermarks
        DataStream<Tuple2<String, Long>> timestampedStream = parsedStream.assignTimestampsAndWatermarks(
                new AscendingTimestampExtractor<Tuple2<String, Long>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple2<String, Long> element) {
                        return element.f1; // Event time is the second field in the tuple
                    }
                }
        );

        // Apply TumblingEventTimeWindows with a window size of 2 minutes
        DataStream<String> resultStream = timestampedStream
                .keyBy(data -> data.f0) // Key by the record field
                .window(TumblingEventTimeWindows.of(Time.minutes(2)))
                .sum(1) // Sum the values in the second field of the tuple
                .map(result -> result.f0 + ": " + result.f1); // Format the result

        // Print the result stream
        resultStream.print();

        // Execute the Flink job
        localEnv.execute("TumblingEventTimeWindows Example");
    }
}

