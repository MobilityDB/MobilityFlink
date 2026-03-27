package sncbdata;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import jnr.ffi.Memory;
import jnr.ffi.Pointer;
import jnr.ffi.Runtime;

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

import functions.functions;
import functions.error_handler;
import functions.error_handler_fn;
import types.temporal.TInterpolation;

/**
 * Query 5 - Trajectory Creation and High-Speed Alert (SNCB dataset)
 *
 * <p>Monitors trains within a geofenced area and emits alerts when average or
 * minimum speed exceeds defined thresholds. Two implementations are provided:
 *
 * <ul>
 *   <li><b>V1 (WKT)</b>: {@link HighSpeedAlertV1_WKT}: collects geofence-surviving
 *       points, builds a WKT string and calls {@code tgeogpoint_in()} once for the
 *       final trajectory.</li>
 *   <li><b>V2 (Expand)</b>: {@link HighSpeedAlertV2_Expand}: builds the surviving
 *       trajectory incrementally using {@code temporal_append_tinstant()}.</li>
 * </ul>
 *
 * <p>To switch implementation, change the {@code .process(...)} call in {@link #main}.
 *
 * <p>Original MobilityNebula pseudocode:
 * <pre>
 *   Query::from(GPS)
 *     .filter(edwithin_tgeo_geo(lon, lat, ts,
 *         POLYGON((4.32 50.60, 4.32 50.72, 4.48 50.72, 4.48 50.60, 4.32 50.60)), 1) == 1) // Line 2
 *     .groupBy(device_id)                                                                   // Line 3
 *     .window(SlidingWindow::of(EventTime(ts), Seconds(45), Seconds(5)))                    // Line 4
 *     .apply(temporal_sequence(lon, lat, ts), avg(gps_speed), min(gps_speed))              // Line 5
 *     .filter((avg_speed > 50) || (min_speed > 20));                                        // Line 6
 * </pre>
 *
 * <p><b>HOW TO RUN:</b> change Dockerfile entrypoint to {@code sncbdata.Query5_Main},
 * then {@code mvn clean package && docker compose up --build}.
 */
public class Query5_Main {

    private static final Logger logger = LoggerFactory.getLogger(Query5_Main.class);

    /** Conversion factor from km/h (gps_speed unit) to m/s. */
    private static final double KMH_TO_MS = 1.0 / 3.6;

    /** avg_speed > 50 m/s (~180 km/h) */
    private static final double AVG_SPEED_THRESHOLD_MS = 50.0;

    /** min_speed > 20 m/s (~72 km/h) */
    private static final double MIN_SPEED_THRESHOLD_MS = 20.0;

    /** Brussels area geofence polygon */
    private static final String GEOFENCE_WKT =
            "POLYGON((4.32 50.60, 4.32 50.72, 4.48 50.72, 4.48 50.60, 4.32 50.60))";

    /** Distance for edwithin_tgeo_geo */
    private static final double GEOFENCE_DISTANCE_METERS = 1.0;

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
                    .setGroupId("flink_consumer_q5")
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
                    .window(SlidingEventTimeWindows.of(Time.seconds(45), Time.seconds(5)))
                    // Switch here to compare implementations:
                    //.process(new HighSpeedAlertV1_WKT(
                    //        GEOFENCE_WKT, GEOFENCE_DISTANCE_METERS,
                    //        AVG_SPEED_THRESHOLD_MS, MIN_SPEED_THRESHOLD_MS))
                    .process(new HighSpeedAlertV2_Expand(
                            GEOFENCE_WKT, GEOFENCE_DISTANCE_METERS,
                            AVG_SPEED_THRESHOLD_MS, MIN_SPEED_THRESHOLD_MS))
                    .print();

            env.execute("Query 5 - Trajectory Creation and High-Speed Alert");

        } catch (Exception e) {
            logger.error("Error during execution: {}", e.getMessage(), e);
            throw e;
        } finally {
            try { functions.meos_finalize(); }
            catch (Exception e) { logger.error("Error during MEOS finalization: {}", e.getMessage(), e); }
        }
    }

    // =========================================================================
    // V1: Geofence filter + WKT StringBuilder + tgeogpoint_in
    // =========================================================================

    /**
     * Filters events through the geofence, accumulates survivors in a WKT string,
     * and calls {@code tgeogpoint_in()} once to build the final trajectory.
     */
    public static class HighSpeedAlertV1_WKT
            extends ProcessWindowFunction<SNCBData, String, Integer, TimeWindow> {

        private static final Logger log = LoggerFactory.getLogger(HighSpeedAlertV1_WKT.class);
        private static final DateTimeFormatter TIMESTAMP_FMT =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssxxx");

        private final String geofenceWkt;
        private final double geofenceDistMeters;
        private final double avgSpeedThresholdMs;
        private final double minSpeedThresholdMs;

        private transient error_handler_fn errorHandler;
        private transient Pointer geofence;

        public HighSpeedAlertV1_WKT(String geofenceWkt, double geofenceDistMeters,
                                    double avgSpeedThresholdMs, double minSpeedThresholdMs) {
            this.geofenceWkt         = geofenceWkt;
            this.geofenceDistMeters  = geofenceDistMeters;
            this.avgSpeedThresholdMs = avgSpeedThresholdMs;
            this.minSpeedThresholdMs = minSpeedThresholdMs;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            errorHandler = new error_handler();
            functions.meos_initialize_timezone("UTC");
            functions.meos_initialize_error_handler(errorHandler);
            geofence = functions.geog_in(geofenceWkt, -1);
            if (geofence == null) log.error("[V1] geog_in returned null for geofence: {}", geofenceWkt);
        }

        @Override
        public void process(Integer deviceId, Context context,
                            Iterable<SNCBData> elements, Collector<String> out) {

            if (geofence == null) return;

            String windowStart = millisToTs(context.window().getStart());
            String windowEnd   = millisToTs(context.window().getEnd());
            List<SNCBData> surviving = new ArrayList<>();

            for (SNCBData event : elements) {
                String wkt    = String.format("POINT(%f %f)@%s",
                        event.getLon(), event.getLat(), millisToTs(event.getTimestamp()));
                Pointer tpoint = functions.tgeogpoint_in(wkt);
                if (tpoint == null) continue;
                if (functions.edwithin_tgeo_geo(tpoint, geofence, geofenceDistMeters) != 1) continue;
                surviving.add(event);
            }

            if (surviving.isEmpty()) return;
            surviving.sort((a, b) -> Long.compare(a.getTimestamp(), b.getTimestamp()));

            StringBuilder seq = new StringBuilder("{");
            double speedSumMs = 0.0;
            double minSpeedMs = Double.MAX_VALUE;
            int survivingListSize = surviving.size();

            for (int i = 0; i < survivingListSize; i++) {
                SNCBData event = surviving.get(i);
                if (i > 0) seq.append(",");
                seq.append(String.format("POINT(%f %f)@%s",
                        event.getLon(), event.getLat(), millisToTs(event.getTimestamp())));
                double speedMs = event.getGpsSpeed() * KMH_TO_MS;
                speedSumMs += speedMs;
                if (speedMs < minSpeedMs) minSpeedMs = speedMs;
            }
            seq.append("}");

            double avgSpeedMs = speedSumMs / survivingListSize;

            // Line 6: (avg_speed > 50) || (min_speed > 20) in m/s
            if (avgSpeedMs <= avgSpeedThresholdMs && minSpeedMs <= minSpeedThresholdMs) return;

            Pointer trajectory = functions.tgeogpoint_in(seq.toString());
            String trajectoryEwkt = (trajectory != null)
                    ? functions.tspatial_as_ewkt(trajectory, 6) : seq.toString();

            emit(out, log, "V1", deviceId, survivingListSize, avgSpeedMs, minSpeedMs,
                    avgSpeedThresholdMs, minSpeedThresholdMs, windowStart, windowEnd, trajectoryEwkt);
        }

        private String millisToTs(long millis) {
            return Instant.ofEpochMilli(millis).atOffset(ZoneOffset.UTC).format(TIMESTAMP_FMT);
        }
    }

    // =========================================================================
    // V2: Geofence filter + tgeogpoint_in (instant) → temporal_append_tinstant
    // =========================================================================

    /**
     * Filters events through the geofence and builds the surviving trajectory
     * incrementally using {@code temporal_append_tinstant()}.
     */
    public static class HighSpeedAlertV2_Expand
            extends ProcessWindowFunction<SNCBData, String, Integer, TimeWindow> {

        private static final Logger log = LoggerFactory.getLogger(HighSpeedAlertV2_Expand.class);
        private static final DateTimeFormatter TIMESTAMP_FMT =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssxxx");

        private final String geofenceWkt;
        private final double geofenceDistMeters;
        private final double avgSpeedThresholdMs;
        private final double minSpeedThresholdMs;

        private transient error_handler_fn errorHandler;
        private transient Pointer geofence;

        public HighSpeedAlertV2_Expand(String geofenceWkt, double geofenceDistMeters,
                                       double avgSpeedThresholdMs, double minSpeedThresholdMs) {
            this.geofenceWkt         = geofenceWkt;
            this.geofenceDistMeters  = geofenceDistMeters;
            this.avgSpeedThresholdMs = avgSpeedThresholdMs;
            this.minSpeedThresholdMs = minSpeedThresholdMs;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            errorHandler = new error_handler();
            functions.meos_initialize_timezone("UTC");
            functions.meos_initialize_error_handler(errorHandler);
            geofence = functions.geog_in(geofenceWkt, -1);
            if (geofence == null) log.error("[V2] geog_in returned null for geofence: {}", geofenceWkt);
        }

        @Override
        public void process(Integer deviceId, Context context,
                            Iterable<SNCBData> elements, Collector<String> out) {

            if (geofence == null) return;

            String windowStart = millisToTs(context.window().getStart());
            String windowEnd   = millisToTs(context.window().getEnd());

            // Collect and sort first: MEOS requires strictly increasing timestamps.
            List<SNCBData> sorted = new ArrayList<>();
            for (SNCBData e : elements) sorted.add(e);
            sorted.sort((a, b) -> Long.compare(a.getTimestamp(), b.getTimestamp()));
            if (sorted.isEmpty()) return;

            Runtime runtime = Runtime.getSystemRuntime();
            Pointer trajectory = null;
            double speedSumMs  = 0.0;
            double minSpeedMs  = Double.MAX_VALUE;
            int count          = 0;

            for (SNCBData event : sorted) {
                String wkt    = String.format("POINT(%f %f)@%s",
                        event.getLon(), event.getLat(), millisToTs(event.getTimestamp()));
                Pointer inst = functions.tgeogpoint_in(wkt);
                if (inst == null) {
                    log.error("[V2] tgeogpoint_in returned null for DeviceID={} wkt={}", deviceId, wkt);
                    continue;
                }

                // Geofence filter: reuse the already-parsed instant pointer.
                if (functions.edwithin_tgeo_geo(inst, geofence, geofenceDistMeters) != 1) continue;

                double speedMs = event.getGpsSpeed() * KMH_TO_MS;
                speedSumMs += speedMs;
                if (speedMs < minSpeedMs) minSpeedMs = speedMs;

                if (trajectory == null) {
                    Pointer seedArray = Memory.allocate(runtime, Long.BYTES);
                    seedArray.putPointer(0, inst);
                    trajectory = functions.tsequence_make(
                            seedArray, 1, true, true, TInterpolation.LINEAR.getValue(), true);
                    if (trajectory == null) {
                        log.error("[V2] tsequence_make (seed) returned null for DeviceID={}", deviceId);
                        return;
                    }
                } else {
                    Pointer expanded = functions.temporal_append_tinstant(
                            trajectory, inst, TInterpolation.LINEAR.getValue(), 0.0, null, true);
                    if (expanded == null) {
                        log.error("[V2] temporal_append_tinstant returned null for DeviceID={} wkt={}", deviceId, wkt);
                        continue;
                    }
                    trajectory = expanded;
                }
                count++;
            }

            if (trajectory == null || count == 0) return;

            double avgSpeedMs = speedSumMs / count;
            if (avgSpeedMs <= avgSpeedThresholdMs && minSpeedMs <= minSpeedThresholdMs) return;

            String trajectoryEwkt = functions.tspatial_as_ewkt(trajectory, 6);
            emit(out, log, "V2", deviceId, count, avgSpeedMs, minSpeedMs,
                    avgSpeedThresholdMs, minSpeedThresholdMs, windowStart, windowEnd, trajectoryEwkt);
        }

        private String millisToTs(long millis) {
            return Instant.ofEpochMilli(millis).atOffset(ZoneOffset.UTC).format(TIMESTAMP_FMT);
        }
    }

    // =========================================================================
    // Shared output helper
    // =========================================================================

    private static void emit(Collector<String> out, Logger log, String version,
                             int deviceId, int points,
                             double avgSpeedMs, double minSpeedMs,
                             double avgThreshold, double minThreshold,
                             String windowStart, String windowEnd,
                             String trajectoryEwkt) {
        String triggerReason = (avgSpeedMs > avgThreshold && minSpeedMs > minThreshold)
                ? "AVG+MIN" : (avgSpeedMs > avgThreshold ? "AVG" : "MIN");
        String alert = String.format(
                "[ALERT][Q5][%s] DeviceID=%-6d | avgSpeed=%6.2f m/s (>%.1f)"
                        + " | minSpeed=%6.2f m/s (>%.1f) | points=%d | trigger=%s"
                        + " | window [%s - %s]%n"
                        + "             trajectory: %s",
                version, deviceId, avgSpeedMs, avgThreshold, minSpeedMs, minThreshold,
                points, triggerReason, windowStart, windowEnd, trajectoryEwkt);
        log.warn(alert);
        out.collect(alert);
    }
}