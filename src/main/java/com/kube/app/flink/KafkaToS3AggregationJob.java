//package com.kube.app.flink;
//
//import com.kube.app.common.Constants;
//import com.kube.app.entity.Transaction;
//import com.kube.app.flink.utils.ReflectParquetWriterFactory;
//import com.kube.app.kafka.TransactionDeserializer;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.core.fs.Path;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
//import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
//import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
//import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
//import org.apache.flink.streaming.api.windowing.time.Time;
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
//import org.apache.flink.util.Collector;
//import org.apache.kafka.clients.consumer.ConsumerConfig;
//import org.apache.kafka.common.serialization.StringDeserializer;
//
//import java.time.Duration;
//import java.util.Properties;
//
//public class KafkaToS3AggregationJob {
//
//    public static void main(String[] args) throws Exception {
//        // Set up the streaming execution environment
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        // Configure Kafka consumer properties
//        Properties kafkaProps = new Properties();
//        kafkaProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_BROKER);
//        kafkaProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, Constants.KAFKA_CONSUMER_GROUP_FLINK);
//        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, TransactionDeserializer.class.getName());
//        kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//
//        // Create a Kafka consumer
//        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("transactions", new SimpleStringSchema(), kafkaProps);
//
//        // Create a data stream from Kafka events
//        DataStream<String> kafkaEvents = env.addSource(kafkaConsumer);
//
//        // Deserialize Kafka events as JSON objects
//        DataStream<Transaction> events = kafkaEvents.map(event -> Transaction.fromJson(event))
//                .assignTimestampsAndWatermarks(WatermarkStrategy
//                        .<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(5))
//                        .withTimestampAssigner((event, timestamp) -> event.getTransactionTime())
//                );
//
//        kafkaEvents.map(event -> Transaction.fromJson(event))
//        .keyBy(Transaction::getAccountId)
//                .window(TumblingEventTimeWindows.of(Time.seconds(30)))
//                .process(new AggregateIngestionProgressProcessWindowFunction())
//                .setParallelism(1);
//
//        StreamingFileSink<Transaction> s3Sink = StreamingFileSink.forBulkFormat(
//                        new Path("s3a://example-bucket/example-prefix"),
//                        ReflectParquetWriterFactory.forSchema(Transaction.class))
//                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyyMMdd/HH/mm"))
//                .build();
//
//        events.addSink(s3Sink).name("S3 Sink");
//
//
////        DataStream<T> input = ...;
////
////        input
////                .keyBy(<key selector>)
////    .window(<window assigner>)
////    .allowedLateness(<time>)
////    .<windowed transformation>(<window function>);
//
//        // Aggregate events by user
//
//        Object stream = events.keyBy(Transaction::getAccountId)
//                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
//                .allowedLateness(Time.seconds(5))
//                .process(new ProcessWindowFunction<Transaction, Object, String, TimeWindow>() {
//                    @Override
//                    public void process(String s, ProcessWindowFunction<Transaction, Object, String, TimeWindow>.Context context, Iterable<Transaction> elements, Collector<Object> out) throws Exception {
//                        System.out.println(s);
//                    }
//                });
//
//
////        DataStream<UserAggregation> aggregatedEvents = events
////                .map(event -> new UserAggregation(event.getAccountId(), 1, event.getAmount()))
////                .keyBy(event -> event.accountId)
////                .window(TumblingEventTimeWindows.of(Time.seconds(10))
////                .allowedLateness(Time.seconds(5)))
////                .reduce((event1, event2) -> new UserAggregation(
////                        event1.accountId,
////                        event1.count + event2.count,
////                        event1.totalAmount + event2.totalAmount
////                ));
////
////        // Configure Redis connection
////        FlinkJedisPoolConfig redisConfig = new FlinkJedisPoolConfig.Builder()
////                .setHost("localhost")
////                .setPort(6379)
////                .build();
////
////        // Create a Redis sink
////        RedisSink<UserAggregation> redisSink = new RedisSink<>(redisConfig, new UserAggregationRedisMapper());
////
////        // Write aggregated data to Redis
////        aggregatedEvents.addSink(redisSink);
////
////        // Execute the Flink job
//        env.execute("Kafka to S3 Aggregation Job");
//
//    }
//
//
//}
