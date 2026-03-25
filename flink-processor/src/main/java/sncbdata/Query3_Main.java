package sncbdata;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jnr.ffi.Pointer;
import functions.functions;
import functions.error_handler;
import functions.error_handler_fn;

/**
 * Query 3 - Trajectory Creation (SNCB dataset)
 *
 * <p>Reconstructs the trajectory of each train from its GPS positions
 * within a sliding window.
 *
 * <p>Original MobilityNebula pseudocode:
 * <pre>
 *   Query::from(GPS)
 *     .window(SlidingWindow::of(EventTime(ts), Seconds(10), Milliseconds(10))) // Line 2
 *     .apply(temporal_sequence(lon, lat, ts));                                  // Line 3
 * </pre>
 *
 * <p><b>HOW TO RUN:</b> change Dockerfile entrypoint to {@code sncbdata.Query3_Main},
 * then {@code mvn clean package && docker compose up --build}.
 */
public class Query3_Main {

    private static final Logger logger = LoggerFactory.getLogger(Query3_Main.class);

    public static void main(String[] args) throws Exception {

        logger.info("Java library path: {}", System.getProperty("java.library.path"));

        try {
            functions.meos_initialize_timezone("UTC");
            functions.meos_initialize_error_handler(new error_handler());

            final StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);

            KafkaSource<SNCBData> kafkaSource = KafkaSource.<SNCBData>builder()
                    .setBootstrapServers("kafka:29092")
                    .setGroupId("flink_consumer_q3")
                    .setTopics("sncbdata")
                    .setStartingOffsets(OffsetsInitializer.earliest())
                    .setValueOnlyDeserializer(new SNCBDataDeserializationSchema())
                    .build();

            WatermarkStrategy<SNCBData> watermarkStrategy =
                    WatermarkStrategy.<SNCBData>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                            .withTimestampAssigner((event, recordTs) -> event.getTimestamp())
                            .withIdleness(Duration.ofMinutes(1));

            DataStream<SNCBData> source = env.fromSource(kafkaSource, watermarkStrategy, "Kafka Source");

            source
                    .keyBy(SNCBData::getDeviceId)
                    .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.milliseconds(10)))
                    .process(new TrajectoryCreationWindowFunction())
                    .print();

            env.execute("Query 3 - Trajectory Creation (SNCB)");

        } catch (Exception e) {
            logger.error("Error during execution: {}", e.getMessage(), e);
            throw e;
        } finally {
            try { functions.meos_finalize(); }
            catch (Exception e) { logger.error("Error during MEOS finalization: {}", e.getMessage(), e); }
        }
    }

    public static class TrajectoryCreationWindowFunction
            extends ProcessWindowFunction<SNCBData, String, Integer, TimeWindow> {

        private static final Logger log = LoggerFactory.getLogger(TrajectoryCreationWindowFunction.class);
        private static final DateTimeFormatter TIMESTAMP_FMT =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssxxx");
        private transient error_handler_fn errorHandler;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            errorHandler = new error_handler();
            functions.meos_initialize_timezone("UTC");
            functions.meos_initialize_error_handler(errorHandler);
        }

        @Override
        public void process(Integer deviceId, Context context,
                            Iterable<SNCBData> elements, Collector<String> out) {

            String windowStart = millisToTimestamp(context.window().getStart());
            String windowEnd   = millisToTimestamp(context.window().getEnd());

            // Collect and sort by timestamp: MEOS requires strictly increasing order.
            List<SNCBData> sorted = new ArrayList<>();
            for (SNCBData e : elements) sorted.add(e);
            sorted.sort((a, b) -> Long.compare(a.getTimestamp(), b.getTimestamp()));
            if (sorted.isEmpty()) return;

            // Build tgeogpoint sequence literal: {POINT(lon lat)@ts, ...}
            StringBuilder seq = new StringBuilder("{");
            for (int i = 0; i < sorted.size(); i++) {
                SNCBData event = sorted.get(i);
                String ts = millisToTimestamp(event.getTimestamp());
                if (i > 0) seq.append(",");
                seq.append(String.format("POINT(%f %f)@%s", event.getLon(), event.getLat(), ts));
            }
            seq.append("}");

            Pointer trajectory = functions.tgeogpoint_in(seq.toString());
            if (trajectory == null) { log.error("tgeogpoint_in returned null for sequence: {}", seq); return; }

            String trajectoryWkt = functions.tspatial_as_ewkt(trajectory, 6);
            String output = String.format(
                    "[TRAJ][Q3] DeviceID=%-6d | points=%3d | window [%s - %s]%n           trajectory: %s",
                    deviceId, sorted.size(), windowStart, windowEnd, trajectoryWkt);

            log.info(output);
            out.collect(output);
        }

        private String millisToTimestamp(long millis) {
            return Instant.ofEpochMilli(millis).atOffset(ZoneOffset.UTC).format(TIMESTAMP_FMT);
        }
    }
}
