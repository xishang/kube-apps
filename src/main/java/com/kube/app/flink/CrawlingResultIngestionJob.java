package com.kube.app.flink;

import com.kube.app.entity.CrawlingResult;
import com.kube.app.flink.source.HeartbeatSource;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;

public class CrawlingResultIngestionJob {

    private static final Logger LOG = LoggerFactory.getLogger(CrawlingResultIngestionJob.class);

    public static final String SCHEMA = "com.airbnb.jitney.event.logging.CrawlingResult:CrawlingResultCrawlingResultEvent:1.0.0";

    public static final String S3_BUCKET_STAGING = "s3://airbnb-emr/mdx/mi/aerial-tram/ingestion/staging/";

    private static CrawlingResult createEvent(long timestamp, String ds, String spiderName, String platform, String pipeline, String dataType, String dataJson) {
        String s3Path = String.format(
                "%s/%s/%s/%s/%s/",
                ds,
                spiderName,
                platform,
                pipeline,
                dataType
        );
        CrawlingResult event = new CrawlingResult(SCHEMA, timestamp, s3Path, dataJson);
        return event;
    }

    public static List<CrawlingResult> prepareInput() {
        long currentTime = System.currentTimeMillis();
        return Arrays.asList(
                createEvent(currentTime - 100 * 1000L, "2023-11-28", "Booking_beat_meet_lose_v2", "Booking", "beat_meet_lose_v2", "p3_page", "1: {\"ds\": \"2023-11-28\", \"platform\": \"Booking\", \"pipeline\": \"bml_v2_freshness\", \"name\": \"Casa Dos, casita de campo\", \"checkIn\": \"2023-12-01\", \"checkOut\": \"2023-12-08\", \"isAvailable\": true, \"city\": \"\", \"url\": \"https://www.booking.com/hotel/ar/casa-dos-casita-de-campo.en-gb.html\", \"p3SourceUrl\": \"s3://airbnb-emr/mdx/mi/aerial-tram/Booking/bml_v2_freshness/2023-11-28/result/page_sources/p3_3602a7688e5211ee98e14a1e5bdb93b4_14278_7307385.gz\", \"p4SourceUrl\": \"\", \"competitor\": \"Booking\", \"tsCrawled\": \"2023-11-29 05:05:23\"}"),
                createEvent(currentTime - 20 * 1000L, "2023-11-28", "Booking_beat_meet_lose_v2", "Booking", "beat_meet_lose_v2", "p4_page", "2: {\"ds\": \"2023-11-28\", \"competitor\": \"Booking\", \"pipeline\": \"bml_v2_freshness\", \"idBlock\": null, \"dsCheckin\": \"2024-01-19\", \"dsCheckout\": \"2024-01-22\", \"mLengthOfStay\": 3, \"isValid\": false, \"mFinalPrice\": -1, \"mTotalPrice\": -1, \"mDiscount\": -1, \"extraData\": null, \"p4SourceUrl\": \"s3://airbnb-emr/mdx/mi/aerial-tram/Booking/bml_v2_freshness/2023-11-28/result/page_sources/p4_366c4d128e5211ee98e14a1e5bdb93b4_12422_9962088.gz\", \"p3SourceUrl\": \"s3://airbnb-emr/mdx/mi/aerial-tram/Booking/bml_v2_freshness/2023-11-28/result/page_sources/p3_366c4d128e5211ee98e14a1e5bdb93b4_12409_710621.gz\", \"responseTime\": 2.8897504806518555, \"queryUrl\": \"https://secure.booking.com/book.html?hotel_id=6634514&label=gen173bo-1DCAsoRkIlbmV3LWFuZC1tb2Rlcm4tYXBhcnRtZW50LW9uLXRoZS1iZWFjaEgJWANoRogBAZgBCbgBB8gBDNgBA-gBAfgBA4gCAZgCAqgCA7gCleGaqwbAAgHSAiQ1ZDc2NjViYS00NmM3LTQwYmYtYTFmZS1iYTdiOTZjYzkzNDHYAgTgAgE&sid=d6d137a5f64a8179c8774e53dfe9bb84&room1=A,A&error_url=/hotel/es/new-and-modern-apartment-on-the-beach.en-gb.html?label=gen173bo-1DCAsoRkIlbmV3LWFuZC1tb2Rlcm4tYXBhcnRtZW50LW9uLXRoZS1iZWFjaEgJWANoRogBAZgBCbgBB8gBDNgBA-gBAfgBA4gCAZgCAqgCA7gCleGaqwbAAgHSAiQ1ZDc2NjViYS00NmM3LTQwYmYtYTFmZS1iYTdiOTZjYzkzNDHYAgTgAgE&sid=d6d137a5f64a8179c8774e53dfe9bb84&&hostname=www.booking.com&stage=1&checkin=2024-01-19&interval=3&children_extrabeds=&srpvid=&hp_visits_num=1&rt_pos_selected=&rt_pos_selected_within_room=&rt_num_blocks=1&rt_num_rooms=1&from_source=hotel&pset_discount_won=None&is_mup_pro=None&None=1&selected_currency=USD\", \"listingUrl\": \"https://www.booking.com/hotel/es/new-and-modern-apartment-on-the-beach.en-gb.html\", \"tsCrawled\": \"2023-11-29 03:31:44\"}"),
                createEvent(currentTime - 60 * 1000L, "2023-11-28", "Booking_beat_meet_lose_v2", "Booking", "beat_meet_lose_v2", "p4_page", "3: {\"ds\": \"2023-11-28\", \"competitor\": \"Booking\", \"pipeline\": \"bml_v2_freshness\", \"idBlock\": null}"),
                createEvent(currentTime - 30 * 1000L, "2023-11-28", "Booking_beat_meet_lose_v2", "Booking", "beat_meet_lose_v2", "p4_page", "4: {\"ds\": \"2023-11-28\", \"competitor\": \"Booking\", \"pipeline\": \"bml_v2_freshness\", \"idBlock\": null}"),
                createEvent(currentTime - 10 * 1000L, "2023-11-28", "Booking_beat_meet_lose_v2", "Booking", "beat_meet_lose_v2", "p4_page", "5: {\"ds\": \"2023-11-28\", \"competitor\": \"Booking\", \"pipeline\": \"bml_v2_freshness\", \"idBlock\": null}"),
                createEvent(currentTime - 120 * 1000L, "2023-11-28", "Booking_beat_meet_lose_v2", "Booking", "beat_meet_lose_v2", "p3_page", "6: {\"ds\": \"2023-11-28\", \"platform\": \"Booking\", \"pipeline\": \"bml_v2_freshness\", \"name\": \"Airport shuttle Travel Sustainable Level 2 Anukampa Paying Guest House\", \"checkIn\": \"2024-02-17\", \"checkOut\": \"2024-02-24\", \"isAvailable\": true, \"city\": \"\", \"url\": \"https://www.booking.com/hotel/in/anukampa-paying-guest-house-agra.en-gb.html\", \"p3SourceUrl\": \"s3://airbnb-emr/mdx/mi/aerial-tram/Booking/bml_v2_freshness/2023-11-28/result/page_sources/p3_aea113228e6f11ee923a4a1e5bdb93b4_5266_9197488.gz\", \"p4SourceUrl\": \"\", \"competitor\": \"Booking\", \"tsCrawled\": \"2023-11-29 05:36:46\"}"),
                createEvent(currentTime - 90 * 1000L, "2023-11-28", "Booking_beat_meet_lose_v2", "Booking", "beat_meet_lose_v2", "p3_page", "7: {\"ds\": \"2023-11-28\", \"platform\": \"Booking\", \"pipeline\": \"bml_v2_freshness\", \"name\": \"Casina sul Balichetto centro storico del Circeo\", \"checkIn\": \"2024-02-24\", \"checkOut\": \"2024-02-26\", \"isAvailable\": true, \"city\": \"\", \"url\": \"https://www.booking.com/hotel/it/appartamento-bilivello-centro-storico-del-circeo.en-gb.html\", \"p3SourceUrl\": \"s3://airbnb-emr/mdx/mi/aerial-tram/Booking/bml_v2_freshness/2023-11-28/result/page_sources/p3_4d56c1228e5411ee96984a1e5bdb93b4_20000_8117577.gz\", \"p4SourceUrl\": \"\", \"competitor\": \"Booking\", \"tsCrawled\": \"2023-11-29 05:38:41\"}"),
                createEvent(currentTime - 20 * 1000L, "2023-11-28", "Vrbo_toutian_bml_v3_spider", "Vrbo", "bml_v3_vrbo", "vrbo_bml_raw", "8: {\"ds\": \"2023-11-28\", \"competitor\": \"Vrbo\", \"pipeline\": \"bml_v3_vrbo\", \"idListing\": \"321.3582686.4155832\", \"dsCheckin\": \"2023-12-23\", \"dsCheckout\": \"2023-12-30\", \"mLengthOfStay\": 7, \"isAvailable\": true, \"isSuccess\": true, \"isSkipped\": false, \"attempt\": 0, \"market\": null, \"reason\": \"\", \"listingUrl\": \"https://www.vrbo.com/3582686\", \"rateShoppingUrl\": \"https://www.vrbo.com/3582686?chkin=2023-12-23&chkout=2023-12-30\", \"p3SourceUrl\": null, \"p4SourceUrl\": \"s3://airbnb-emr/mdx/mi/aerial-tram/Vrbo/bml_v3_vrbo/2023-11-28/result/page_sources/page_20f3ec448e6911ee874f4a1e5bdb93b4_626_5507585.gz\", \"priceDetails\": \"{\\\"data\\\": {\\\"priceDetails\\\": {\\\"lineItems\\\": [{\\\"amount\\\": \\\"$618\\\", \\\"title\\\": \\\"7 nights\\\", \\\"type\\\": \\\"RENT\\\"}, {\\\"amount\\\": \\\"$56\\\", \\\"title\\\": \\\"Service fee\\\", \\\"type\\\": \\\"SERVICE_FEE\\\"}, {\\\"amount\\\": \\\"$30\\\", \\\"title\\\": \\\"Host fee\\\", \\\"type\\\": \\\"OTHER\\\"}], \\\"totals\\\": [{\\\"amount\\\": \\\"$704\\\", \\\"title\\\": \\\"Total\\\", \\\"type\\\": \\\"\\\"}]}}}\", \"tsCrawled\": \"2023-11-29 04:53:41\"}"),
                createEvent(currentTime - 70 * 1000L, "2023-11-28", "Vrbo_toutian_bml_v3_spider", "Vrbo", "bml_v3_vrbo", "vrbo_bml_raw", "9: {\"ds\": \"2023-11-28\", \"competitor\": \"Vrbo\", \"pipeline\": \"bml_v3_vrbo\", \"idListing\": \"321.3582686.4155832\"}"),
                createEvent(currentTime + 10 * 1000L, "2023-11-28", "Vrbo_toutian_bml_v3_spider", "Vrbo", "bml_v3_vrbo", "vrbo_bml_raw", "10: {\"ds\": \"2023-11-28\", \"competitor\": \"Vrbo\", \"pipeline\": \"bml_v3_vrbo\", \"idListing\": \"321.3582686.4155832\"}"),
                createEvent(currentTime + 15 * 1000L, "2023-11-28", "Vrbo_toutian_bml_v3_spider", "Vrbo", "bml_v3_vrbo", "vrbo_bml_raw", "11: {\"ds\": \"2023-11-28\", \"competitor\": \"Vrbo\", \"pipeline\": \"bml_v3_vrbo\", \"idListing\": \"321.3582686.4155832\"}"),
                createEvent(currentTime + 20 * 1000L, "2023-11-28", "Vrbo_toutian_bml_v3_spider", "Vrbo", "bml_v3_vrbo", "vrbo_bml_raw", "12: {\"ds\": \"2023-11-28\", \"competitor\": \"Vrbo\", \"pipeline\": \"bml_v3_vrbo\", \"idListing\": \"321.3582686.4155832\"}")
        );
    }

    public static void main(String[] args) throws Exception {
        // for testing
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        DataStream<CrawlingResult> eventStream = env.fromCollection(prepareInput());

        DataStream<CrawlingResult> heartbeatStream =
                env.addSource(new HeartbeatSource(5))
                        .setParallelism(1)
                        .name("Health Check Source")
                        .uid("Health Check Source");

        eventStream
                .union(heartbeatStream)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<>(Time.seconds(30)) {
                    @Override
                    public long extractTimestamp(CrawlingResult element) {
                        return element.getEventTime();
                    }
                })
                .keyBy(CrawlingResult::getS3Path)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(30 * 1000L)))
                .process(
                        new ProcessWindowFunction<CrawlingResult, CrawlingResult, String, TimeWindow>() {
                            @Override
                            public void process(String key, Context context, Iterable<CrawlingResult> elements,
                                                Collector<CrawlingResult> out) throws Exception {
                                for (CrawlingResult element : elements) {
                                    if (SCHEMA.equals(element.getSchema())) {
                                        out.collect(element);
                                    }
                                }
                            }
                        })
                .setParallelism(1)
                .sinkTo(getSink())
                .setParallelism(1)
                .name("Crawling Results Ingestion")
                .uid("Crawling Results Ingestion");

        env.execute("Crawling Results Ingestion");

    }

    private static FileSink<CrawlingResult> getSink() {
        FileSink<CrawlingResult> sink = FileSink
                .forRowFormat(
                        new Path("/Users/shang_xi/resources/aerial-tram/"),
//                        new Path(S3_BUCKET_STAGING),
                        (Encoder<CrawlingResult>) (element, stream) -> {
                            LOG.info("Writing to file: {}", element.getDataJson());
                            stream.write(element.getDataJson().getBytes(Charset.forName("UTF-8")));
                            stream.write('\n');
                        }
                )
                .withBucketAssigner(new DateTimeBucketAssigner<CrawlingResult>() {
                    @Override
                    public String getBucketId(CrawlingResult element, Context context) {
                        return element.getS3Path();
                    }
                })
                .withOutputFileConfig(
                        OutputFileConfig.builder()
                                .withPartSuffix(".csv")
                                .build()
                )
                .withBucketCheckInterval(10 * 1000L)
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofMillis(10 * 1000L))
                                .withInactivityInterval(Duration.ofMillis(5 * 1000L))
                                .build()
                )
                .build();
        return sink;
    }

}
