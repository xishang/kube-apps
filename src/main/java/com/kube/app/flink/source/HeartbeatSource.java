package com.kube.app.flink.source;

import com.kube.app.entity.CrawlingResult;
import com.kube.app.flink.CrawlingResultIngestionJob;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * heartbeat source which send heartbeat event every 60 seconds
 * <p> this will also help update the watermark in case there is no event to trigger file upload
 */
public class HeartbeatSource extends RichSourceFunction<CrawlingResult> {

    public static final String HEARTBEAT_SCHEMA = "heartbeat";

    public static final int HEARTBEAT_INTERVAL = 10 * 1000; // 60 seconds

    private static final Logger LOG = LoggerFactory.getLogger(HeartbeatSource.class);

    private volatile boolean isRunning = true;

    private volatile int eventNumber;

    private volatile List<CrawlingResult> events;

    public HeartbeatSource(int eventNumber) {
        this.eventNumber = eventNumber;
        this.events = CrawlingResultIngestionJob.prepareInput();
    }

    @Override
    public void run(SourceContext<CrawlingResult> context) throws Exception {
        while (eventNumber-- > 0) {
            // Emit a heartbeat event
            long currentTime = System.currentTimeMillis();
            LOG.info("Emitting heartbeat event at {}", currentTime);
            CrawlingResult event = this.events.get(eventNumber);
            event.setEventTime(currentTime);
            context.collectWithTimestamp(event, currentTime);

//            context.collectWithTimestamp(event, currentTime);
            context.emitWatermark(new Watermark(currentTime));
//            context.markAsTemporarilyIdle();
            try {
                // Wait for 1 minute
                Thread.sleep(HEARTBEAT_INTERVAL);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

}
