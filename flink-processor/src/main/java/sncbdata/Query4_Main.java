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
 * Query 4 - Trajectory Creation in a Restricted Space (SNCB dataset)
 *
 * <p>Applies a spatiotemporal box (STBox) filter to restrict trajectory
 * construction to the Brussels-South corridor (device 3) and the SNCB dataset date.
 *
 * <p>Original MobilityNebula pseudocode:
 * <pre>
 *   Query::from(GPS)
 *     .filter(tgeo_at_stbox(lon, lat, ts,
 *         stbox xt(((4.3,50),(4.5,50.6)),[2024-10-24,2024-11-26])) == 1) // Line 2
 *     .window(SlidingWindow::of(EventTime(ts), Seconds(10), Milliseconds(10))) // Line 3
 *     .apply(temporal_sequence(lon, lat, ts));                                  // Line 4
 * </pre>
 *
 * <p><b>HOW TO RUN:</b> change Dockerfile entrypoint to {@code sncbdata.Query4_Main},
 * then {@code mvn clean package && docker compose up --build}.
 */
public class Query4_Main {

    private static final Logger logger = LoggerFactory.getLogger(Query4_Main.class);

    // STBox: Brussels-South where device 3 operates (lon 4.35-4.40, lat 50.63-50.66)
    private static final double STBOX_XMIN = 4.35;
    private static final double STBOX_XMAX = 4.40;
    private static final double STBOX_YMIN = 50.63;
    private static final double STBOX_YMAX = 50.66;
    // Full day of the SNCB dataset
    private static final String STBOX_TSPAN = "[2024-08-01 00:00:00+00, 2024-08-02 00:00:00+00]";

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
                    .setGroupId("flink_consumer_q4")
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
                    .process(new RestrictedTrajectoryWindowFunction(
                            STBOX_XMIN, STBOX_XMAX, STBOX_YMIN, STBOX_YMAX, STBOX_TSPAN))
                    .print();

            env.execute("Query 4 - Trajectory Creation in a Restricted Space (SNCB)");

        } catch (Exception e) {
            logger.error("Error during execution: {}", e.getMessage(), e);
            throw e;
        } finally {
            try { functions.meos_finalize(); }
            catch (Exception e) { logger.error("Error during MEOS finalization: {}", e.getMessage(), e); }
        }
    }

    public static class RestrictedTrajectoryWindowFunction
            extends ProcessWindowFunction<SNCBData, String, Integer, TimeWindow> {

        private static final Logger log = LoggerFactory.getLogger(RestrictedTrajectoryWindowFunction.class);
        private static final DateTimeFormatter TIMESTAMP_FMT =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssxxx");

        private final double xmin, xmax, ymin, ymax;
        private final String tspanLiteral;

        // Built once per worker in open().
        private transient Pointer stbox;
        private transient error_handler_fn errorHandler;

        public RestrictedTrajectoryWindowFunction(
                double xmin, double xmax, double ymin, double ymax, String tspanLiteral) {
            this.xmin = xmin; this.xmax = xmax; this.ymin = ymin; this.ymax = ymax;
            this.tspanLiteral = tspanLiteral;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            errorHandler = new error_handler();
            functions.meos_initialize_timezone("UTC");
            functions.meos_initialize_error_handler(errorHandler);
            // Build the STBox once per worker, not per window call.
            Pointer tspan = functions.tstzspan_in(tspanLiteral);
            if (tspan == null) { log.error("tstzspan_in returned null for: {}", tspanLiteral); return; }
            stbox = functions.stbox_make(true, false, true, 4326, xmin, xmax, ymin, ymax, 0, 0, tspan);
            if (stbox == null) log.error("stbox_make returned null");
            else log.info("STBox built: xmin={} xmax={} ymin={} ymax={} tspan={}", xmin, xmax, ymin, ymax, tspanLiteral);
        }

        @Override
        public void process(Integer deviceId, Context context,
                            Iterable<SNCBData> elements, Collector<String> out) {

            if (stbox == null) return;

            String windowStart = millisToTimestamp(context.window().getStart());
            String windowEnd   = millisToTimestamp(context.window().getEnd());
            List<SNCBData> surviving = new ArrayList<>();

            for (SNCBData event : elements) {
                String ts = millisToTimestamp(event.getTimestamp());
                Pointer tpoint = functions.tgeogpoint_in(
                        String.format("POINT(%f %f)@%s", event.getLon(), event.getLat(), ts));
                if (tpoint == null) { log.error("tgeogpoint_in returned null for DeviceID={} ts={}", deviceId, ts); continue; }
                // Paper Line 2: tgeo_at_stbox returns null if outside the STBox
                if (functions.tgeo_at_stbox(tpoint, stbox, true) == null) continue;
                surviving.add(event);
            }

            if (surviving.isEmpty()) return;
            surviving.sort((a, b) -> Long.compare(a.getTimestamp(), b.getTimestamp()));

            StringBuilder seq = new StringBuilder("{");
            for (int i = 0; i < surviving.size(); i++) {
                SNCBData event = surviving.get(i);
                String ts = millisToTimestamp(event.getTimestamp());
                if (i > 0) seq.append(",");
                seq.append(String.format("POINT(%f %f)@%s", event.getLon(), event.getLat(), ts));
            }
            seq.append("}");

            Pointer trajectory = functions.tgeogpoint_in(seq.toString());
            if (trajectory == null) { log.error("tgeogpoint_in returned null for sequence: {}", seq); return; }

            String output = String.format(
                    "[TRAJ][Q4] DeviceID=%-6d | points=%3d | window [%s - %s]%n           trajectory: %s",
                    deviceId, surviving.size(), windowStart, windowEnd,
                    functions.tspatial_as_ewkt(trajectory, 6));

            log.info(output);
            out.collect(output);
        }

        private String millisToTimestamp(long millis) {
            return Instant.ofEpochMilli(millis).atOffset(ZoneOffset.UTC).format(TIMESTAMP_FMT);
        }
    }
}
