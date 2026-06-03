package aisdata;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jnr.ffi.Pointer;
import functions.functions;
import functions.error_handler;
import functions.error_handler_fn;

/**
 * Query 5 - Trajectory Creation and High-Speed Alert
 *
 * <p>This query is a Java/Flink adaptation of the MobilityNebula Query 5 described in the paper
 * <i>"MobilityNebula: A System for Processing Mobility Data Streams with MobilityDB"</i>.
 *
 * <p>The paper describes Query 5 as follows (Section 4.1):
 * <blockquote>
 *   "Query 5 performs trajectory creation and emits high-speed alerts in a geofenced area.
 *   It first keeps only records whose longitude, latitude, and timestamp are close to a given
 *   polygon using edwithin_tgeo_geo (Line 2). It then groups by device_id and applies a
 *   45-second sliding window with a 5-second advance over the event timestamp (Lines 3–4).
 *   Within each window, it computes the trajectory and aggregates avg_speed and min_speed
 *   (Line 5). It then filters where avg_speed exceeds 50 m/s or min_speed exceeds 20 m/s
 *   (Line 6). It then outputs the train position as a trajectory, average, and minimum speed."
 * </blockquote>
 *
 * <p>The original MobilityNebula pseudocode is:
 * <pre>
 *   Query::from(GPS)
 *     .filter(edwithin_tgeo_geo(lon, lat, ts,                               // Line 2
 *         POLYGON((4.32 50.60, 4.32 50.72, 4.48 50.72,
 *                  4.48 50.60, 4.32 50.60)), 1) == 1)
 *     .groupBy(device_id)                                                    // Line 3
 *     .window(SlidingWindow::of(EventTime(ts), Seconds(45), Seconds(5)))     // Line 4
 *     .apply(temporal_sequence(lon, lat, ts), avg(gps_speed), min(gps_speed))// Line 5
 *     .filter((avg_speed > 50) || (min_speed > 20));                         // Line 6
 * </pre>
 *
 * <p><b>Mapping to this implementation:</b>
 * <ul>
 *   <li><b>Line 2 (edwithin_tgeo_geo)</b>: Applied inside
 *       {@link HighSpeedAlertWindowFunction#process} for each event. The geofence polygon
 *       is the same geography type ({@code geog_in}) as in Query 1. Distance = 1 metre,
 *       which is effectively an intersection test: only points inside or on the boundary
 *       of the polygon pass the filter.</li>
 *   <li><b>Line 3 (groupBy device_id)</b>: Implemented with {@code keyBy(getMmsi)}.</li>
 *   <li><b>Line 4 (sliding window 45s / 5s)</b>: Implemented with
 *       {@code SlidingEventTimeWindows.of(Time.seconds(45), Time.seconds(5))}. This is
 *       larger than Queries 3–4 (10s/10ms): a new window opens every 5 seconds, producing
 *       9 overlapping windows simultaneously (45s / 5s = 9).</li>
 *   <li><b>Line 5 (temporal_sequence + avg + min)</b>: All three aggregations are computed
 *       in a single {@link ProcessWindowFunction} pass over the window events:
 *       <ul>
 *         <li>{@code temporal_sequence}: builds a {@code tgeogpoint}
 *             sequence from (lon, lat, ts) triplets.</li>
 *         <li>{@code avg(gps_speed)}: arithmetic mean of the speed values in the window.</li>
 *         <li>{@code min(gps_speed)}: minimum speed value in the window.</li>
 *       </ul>
 *   </li>
 *   <li><b>Line 6 (filter avg_speed &gt; 50 || min_speed &gt; 20)</b>: Applied after
 *       computing the aggregates. The condition uses OR: an alert fires if either the
 *       average speed is anomalously high OR if even the minimum speed is high (indicating
 *       the vessel never slowed down in the window). See {@link #AVG_SPEED_THRESHOLD_MS}
 *       and {@link #MIN_SPEED_THRESHOLD_MS}.</li>
 * </ul>
 *
 * <p><b>Speed unit conversion (knots → m/s):</b>
 * <br>The paper uses m/s thresholds (50 m/s avg, 20 m/s min) for SNCB trains. AIS speed
 * is reported in knots (1 kn = 0.5144 m/s). The AIS {@code speed} field is converted to
 * m/s before computing avg and min: see {@link #KNOTS_TO_MS}.
 * <ul>
 *   <li>{@link #AVG_SPEED_THRESHOLD_MS}: 7.5 m/s ≈ 14.6 kn - fast cruise speed in AIS</li>
 *   <li>{@link #MIN_SPEED_THRESHOLD_MS}: 5.0 m/s ≈ 9.7 kn - slow speed</li>
 * </ul>
 *
 * <p><b>HOW TO RUN</b>
 * <br>In the Dockerfile, change the entrypoint from {@code aisdata.Main} to
 * {@code aisdata.Query5_Main}, then run:
 * <pre>
 *   mvn clean package &amp;&amp; docker compose up --build
 * </pre>
 *
 * <p><b>TROUBLESHOOTING</b>
 * <ul>
 *   <li>MobilityDB {@code stable-1.3} or later could be required depending on your needs:
 *       {@code https://github.com/MobilityDB/MobilityDB.git -b stable-1.3}</li>
 *   <li>Make sure you're using a JMEOS version compatible with your MobilityDB version:
 *       {@code --branch fix-tests-using-docker https://github.com/MobilityDB/JMEOS}</li>
 * </ul>
 */
public class Query5_Main {

    private static final Logger logger = LoggerFactory.getLogger(Query5_Main.class);

    /**
     * Conversion factor from knots (AIS speed unit) to metres per second.
     * 1 knot = 1 nautical mile/hour = 1852 m / 3600 s ≈ 0.5144 m/s.
     *
     * https://www.calculateme.com/speed/meters-per-second/to-knots/7.5
     */
    private static final double KNOTS_TO_MS = 0.5144;

    /**
     * Average speed alert threshold in m/s from paper Line 6: {@code avg_speed > 50}.
     *
     * <p>The paper uses 50 m/s for SNCB trains. Adapted here to 7.5 m/s (~14.6 kn).
     */
    private static final double AVG_SPEED_THRESHOLD_MS = 7.5;

    /**
     * Minimum speed alert threshold in m/s from paper Line 6: {@code min_speed > 20}.
     *
     * <p>The paper uses 20 m/s for SNCB trains. Adapted here to 5.0 m/s (~9.7 kn).
     */
    private static final double MIN_SPEED_THRESHOLD_MS = 5.0;

    /**
     * Geofence polygon: the {@code POLYGON} argument in paper Line 2.
     *
     * <p>The paper uses a polygon covering the Brussels area (SNCB dataset):
     * <pre>
     *   POLYGON((4.32 50.60, 4.32 50.72, 4.48 50.72, 4.48 50.60, 4.32 50.60))
     * </pre>
     *
     * <p>Adapted here to cover area where MMSI 219027804 operates in the AIS dataset (lon 11.76–12.07, lat 55.82–55.95).
     * This vessel reaches speeds up to 31.8 kn (≈ 16.4 m/s), well above both average and minimum speed thresholds, ensuring
     * alerts fire on the AIS data.
     *
     * <p>Parsed with {@code geog_in(wkt, -1)}: SRID=4326 geography, consistent with
     * {@code tgeogpoint_in}. Distance = {@link #GEOFENCE_DISTANCE_METERS}.
     */
    private static final String GEOFENCE_WKT =
            "POLYGON((11.76 55.82, 11.76 55.95, 12.07 55.95, 12.07 55.82, 11.76 55.82))";

    /**
     * Distance parameter for {@code edwithin_tgeo_geo} in metres: paper Line 2 uses 1 m.
     *
     * <p>A distance of 1 m is effectively an intersection test: the point must be inside
     * or within 1 m of the polygon boundary. Kept at 1 m here to match the paper semantics.
     */
    private static final double GEOFENCE_DISTANCE_METERS = 1.0;

    public static void main(String[] args) throws Exception {

        logger.info("Java library path: {}", System.getProperty("java.library.path"));

        try {
            logger.info("Initializing MEOS library");
            functions.meos_initialize_timezone("UTC");
            functions.meos_initialize_error_handler(new error_handler());

            final StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);

            Properties properties = new Properties();
            properties.setProperty("bootstrap.servers", "kafka:29092");
            properties.setProperty("group.id", "flink_consumer_q5");
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName());
            properties.setProperty("auto.offset.reset", "earliest");

            KafkaSource<AISData> kafkaSource = KafkaSource.<AISData>builder()
                    .setBootstrapServers("kafka:29092")
                    .setGroupId("flink_consumer_q5")
                    .setTopics("aisdata")
                    .setStartingOffsets(OffsetsInitializer.earliest())
                    .setValueOnlyDeserializer(new AISDataDeserializationSchema())
                    .build();

            // Same watermark strategy as Queries 1–4 (see Query1_Main for detailed comments).
            DataStream<AISData> source = env
                    .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
                    .assignTimestampsAndWatermarks(
                            WatermarkStrategy.<AISData>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                    .withTimestampAssigner(
                                            (event, recordTs) -> event.getTimestamp())
                                    .withIdleness(Duration.ofMinutes(1)));

            // Pipeline implementing the MobilityNebula Query 5 pseudocode (paper Section 4.1):
            //
            //   keyBy(getMmsi)            → paper Line 3 (groupBy device_id): partitions
            //                               the stream per vessel.
            //   window(Sliding 45s/5s)    → paper Line 4: 9 overlapping windows open at
            //                               any moment (45s / 5s = 9). Coarser than Queries
            //                               3–4 but still produces frequent updates.
            //   process(WindowFunction)   → paper Lines 2, 5, 6:
            //                                 - Line 2: edwithin_tgeo_geo geofence filter
            //                                 - Line 5: temporal_sequence + avg + min speed
            //                                 - Line 6: (avg > threshold) || (min > threshold)
            //   print()                   → outputs trajectory + speed stats as alert string.
            source
                    .keyBy(AISData::getMmsi)
                    .window(SlidingEventTimeWindows.of(Time.seconds(45), Time.seconds(5)))
                    .process(new HighSpeedAlertWindowFunction(
                            GEOFENCE_WKT,
                            GEOFENCE_DISTANCE_METERS,
                            AVG_SPEED_THRESHOLD_MS,
                            MIN_SPEED_THRESHOLD_MS))
                    .print();

            env.execute("Query 5 - Trajectory Creation and High-Speed Alert");
            logger.info("Done");

        } catch (Exception e) {
            logger.error("Error during execution: {}", e.getMessage(), e);
            throw e;
        } finally {
            try {
                logger.info("Finalizing MEOS library");
                functions.meos_finalize();
            } catch (Exception e) {
                logger.error("Error during MEOS finalization: {}", e.getMessage(), e);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Window function
    // -----------------------------------------------------------------------

    /**
     * Flink {@link ProcessWindowFunction} implementing paper Lines 2, 5, and 6 for a single
     * (MMSI, 45-second sliding window) pair.
     *
     * <p>Processing order within each window:
     * <ol>
     *   <li><b>Line 2</b>: each event is tested against the geofence polygon via
     *       {@code edwithin_tgeo_geo}. Events outside the polygon are skipped.</li>
     *   <li><b>Line 5</b>: over surviving events, compute simultaneously:
     *       {@code temporal_sequence} (trajectory), {@code avg(speed)}, {@code min(speed)}.</li>
     *   <li><b>Line 6</b>: emit an alert only if
     *       {@code avgSpeed > AVG_SPEED_THRESHOLD_MS || minSpeed > MIN_SPEED_THRESHOLD_MS}.</li>
     * </ol>
     */
    public static class HighSpeedAlertWindowFunction
            extends ProcessWindowFunction<AISData, String, Integer, TimeWindow> {

        private static final Logger log =
                LoggerFactory.getLogger(HighSpeedAlertWindowFunction.class);

        private final String geofenceWkt;
        private final double geofenceDistMeters;
        private final double avgSpeedThresholdMs;
        private final double minSpeedThresholdMs;

        /** Initialised in {@link #open} since Pointer is not serialisable. */
        private transient error_handler_fn errorHandler;

        /**
         * Geofence polygon pointer, parsed once in {@link #open} for reuse across all windows.
         * Declared transient because JNR-FFI Pointer objects are not serialisable.
         */
        private transient Pointer geofence;

        private static final DateTimeFormatter TIMESTAMP_FMT =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssxxx");

        public HighSpeedAlertWindowFunction(
                String geofenceWkt,
                double geofenceDistMeters,
                double avgSpeedThresholdMs,
                double minSpeedThresholdMs) {
            this.geofenceWkt         = geofenceWkt;
            this.geofenceDistMeters  = geofenceDistMeters;
            this.avgSpeedThresholdMs = avgSpeedThresholdMs;
            this.minSpeedThresholdMs = minSpeedThresholdMs;
        }

        /**
         * Initialises MEOS and parses the geofence polygon once for all windows.
         * Same optimisation as Query 4's STBox: avoids re-parsing on every invocation.
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            errorHandler = new error_handler();
            functions.meos_initialize_timezone("UTC");
            functions.meos_initialize_error_handler(errorHandler);

            // geog_in(wkt, -1) → SRID=4326 geography, consistent with tgeogpoint_in.
            geofence = functions.geog_in(geofenceWkt, -1);
            if (geofence == null) {
                log.error("geog_in returned null for geofence: {}", geofenceWkt);
            } else {
                log.info("Geofence polygon parsed successfully: {}", geofenceWkt);
            }
            log.info("MEOS initialized in HighSpeedAlertWindowFunction.open()");
        }

        /**
         * Applies paper Lines 2, 5, and 6 for all events in one 45-second sliding window.
         *
         * <p><b>Paper Line 2</b>: each event is tested with {@code edwithin_tgeo_geo(point,
         * geofence, 1m) == 1}. Events outside the polygon are discarded.
         *
         * <p><b>Paper Line 5</b>: over surviving events, three aggregations are computed
         * in a single pass:
         * <ul>
         *   <li>{@code temporal_sequence(lon, lat, ts)}: identical to Queries 3–4.</li>
         *   <li>{@code avg(gps_speed)}: sum of speed values divided by count, in m/s.</li>
         *   <li>{@code min(gps_speed)}: minimum speed value in the window, in m/s.</li>
         * </ul>
         * Speed is converted from knots (AIS) to m/s via {@link #KNOTS_TO_MS} before
         * aggregation to match the paper's m/s thresholds.
         *
         * <p><b>Paper Line 6</b>: the OR condition {@code (avg_speed > 50) || (min_speed > 20)}
         * fires if either the average or the minimum speed crosses its threshold.
         *
         * @param mmsi      vessel identifier (the key used by keyBy, maps to device_id in paper)
         * @param context   window metadata (start/end timestamps)
         * @param elements  all AISData events in this (MMSI, window) pair
         * @param out       collector for the high-speed alert string
         */
        @Override
        public void process(
                Integer mmsi,
                Context context,
                Iterable<AISData> elements,
                Collector<String> out) {

            if (geofence == null) return;

            String windowStart = millisToTimestamp(context.window().getStart());
            String windowEnd   = millisToTimestamp(context.window().getEnd());

            // Collect events that pass the geofence filter, sorted by timestamp.
            List<AISData> surviving = new ArrayList<>();

            for (AISData event : elements) {

                String ts = millisToTimestamp(event.getTimestamp());

                Pointer tpoint = functions.tgeogpoint_in(
                        String.format("POINT(%f %f)@%s", event.getLon(), event.getLat(), ts));
                if (tpoint == null) {
                    log.error("tgeogpoint_in returned null for MMSI={} at ts={}", mmsi, ts);
                    continue;
                }

                // Paper Line 2: edwithin_tgeo_geo(lon, lat, ts, POLYGON, 1) == 1
                // Distance=1m is effectively an intersection test.
                if (functions.edwithin_tgeo_geo(tpoint, geofence, geofenceDistMeters) != 1) {
                    log.debug("MMSI={} skipped: outside geofence at ts={}", mmsi, ts);
                    continue;
                }

                surviving.add(event);
            }

            if (surviving.isEmpty()) return;

            surviving.sort((a, b) -> Long.compare(a.getTimestamp(), b.getTimestamp()));

            // Paper Line 5: three aggregations computation

            // (a) temporal_sequence(lon, lat, ts).
            StringBuilder seq = new StringBuilder("{");
            double speedSumMs = 0.0;
            double minSpeedMs = Double.MAX_VALUE;

            for (int i = 0; i < surviving.size(); i++) {
                AISData event = surviving.get(i);
                String ts = millisToTimestamp(event.getTimestamp());

                if (i > 0) seq.append(",");
                seq.append(String.format("POINT(%f %f)@%s", event.getLon(), event.getLat(), ts));

                // (b) avg(gps_speed) and (c) min(gps_speed):
                // Convert knots → m/s before accumulating.
                double speedMs = event.getSpeed() * KNOTS_TO_MS;
                speedSumMs += speedMs;
                if (speedMs < minSpeedMs) minSpeedMs = speedMs;
            }
            seq.append("}");

            double avgSpeedMs = speedSumMs / surviving.size();

            // Paper Line 6: (avg_speed > 50) || (min_speed > 20)
            // OR condition: alert if either aggregate crosses its threshold.
            if (avgSpeedMs <= avgSpeedThresholdMs && minSpeedMs <= minSpeedThresholdMs) return;

            // Build trajectory for output.
            Pointer trajectory = functions.tgeogpoint_in(seq.toString());
            String trajectoryEwkt = (trajectory != null)
                    ? functions.tspatial_as_ewkt(trajectory, 6)
                    : seq.toString(); // fallback to raw WKT if MEOS parse fails

            String triggerReason = buildTriggerReason(avgSpeedMs, minSpeedMs);

            String alert = String.format(
                    "[ALERT][Q5] MMSI=%-12d | avgSpeed=%6.2f m/s (>%.1f) | minSpeed=%6.2f m/s (>%.1f)"
                            + " | points=%d | trigger=%s | window [%s - %s]%n"
                            + "             trajectory: %s",
                    mmsi,
                    avgSpeedMs, avgSpeedThresholdMs,
                    minSpeedMs, minSpeedThresholdMs,
                    surviving.size(),
                    triggerReason,
                    windowStart, windowEnd,
                    trajectoryEwkt);

            log.warn(alert);
            out.collect(alert);
        }

        /**
         * Describes which condition(s) of the Line 6 OR filter triggered the alert.
         * Helps distinguish at a glance whether the alert was caused by high average speed,
         * high minimum speed, or both simultaneously.
         */
        private String buildTriggerReason(double avgSpeedMs, double minSpeedMs) {
            boolean avgTriggered = avgSpeedMs > avgSpeedThresholdMs;
            boolean minTriggered = minSpeedMs > minSpeedThresholdMs;
            if (avgTriggered && minTriggered) return "AVG+MIN";
            if (avgTriggered)                 return "AVG";
            return                                   "MIN";
        }

        private String millisToTimestamp(long millis) {
            Instant instant = Instant.ofEpochMilli(millis);
            OffsetDateTime dt = instant.atOffset(ZoneOffset.UTC);
            return dt.format(TIMESTAMP_FMT);
        }
    }
}