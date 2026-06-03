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
 * Query 2 - Brake System Monitoring
 *
 * <p>This query is a Java/Flink adaptation of the MobilityNebula Query 2 described in the paper
 * <i>"MobilityNebula: A System for Processing Mobility Data Streams with MobilityDB"</i>.
 *
 * <p>The paper describes Query 2 as follows (Section 4.1):
 * <blockquote>
 *   "Query 2 performs brake-system monitoring. It first filters the SNCB stream to include
 *   only points that are not in a maintenance area (INPolygons), by applying the function
 *   eintersects_tgeo_geo (Line 2). It then applies a ten-second sliding window with a
 *   ten-millisecond step over the event timestamp (Line 3). Within each window, the query
 *   computes the pressure variation for the automatic brake pipe pressure (FA) and the
 *   fictitious brake cylinder pressure (FF) using the variation operator (Line 4), with
 *   results varFA and varFF. varFA and varFF denote the statistical variance of the respective
 *   pressure attribute within the window, computed using a custom VAR aggregation. For a
 *   pressure signal X within one window, measures how much X fluctuates around its mean.
 *   Therefore, this variance is measured in bar² and captures how much the pressure fluctuates
 *   around its mean. The query retains only those data points where varFA exceeds 0.6 bar²,
 *   while varFF remains at or below 0.5 bar² (Line 5), indicating a potential brake issue."
 * </blockquote>
 *
 * <p>The original MobilityNebula pseudocode is:
 * <pre>
 *   Query::from(GPS)
 *     .filter(eintersects_tgeo_geo(lon, lat, ts, INPolygons) == 0)                // Line 2 - exclude maintenance areas
 *     .window(SlidingWindow::of(EventTime(ts), Seconds(10), Milliseconds(10)))    // Line 3 - sliding window
 *     .apply(variation(FA), variation(FF))                                        // Line 4 - pressure variance
 *     .filter(varFA > 0.6 && varFF <= 0.5);                                       // Line 5 - brake anomaly filter
 * </pre>
 *
 * <p><b>Mapping to this implementation:</b>
 * <ul>
 *   <li><b>Line 2 (eintersects_tgeo_geo == 0)</b>: Applied inside
 *       {@link BrakeMonitoringWindowFunction#process} before computing variance. Each event's
 *       position is tested against the maintenance area polygons; events that intersect are
 *       skipped. This is the inverse of Query 1's {@code edwithin_tgeo_geo}: here we exclude
 *       points that are inside a zone rather than alerting on proximity.</li>
 *   <li><b>Line 3 (sliding window 10s / 10ms)</b>: Implemented with
 *       {@code SlidingEventTimeWindows.of(Time.seconds(10), Time.milliseconds(10))}. A 10ms
 *       step produces approximately 1000 overlapping windows per second.</li>
 *   <li><b>Line 4 (variation(FA), variation(FF))</b>: The {@code variation} operator computes
 *       statistical variance (E[X²] − E[X]²) over the window. MEOS does not expose a
 *       {@code variation} function in its Java bindings, so the variance is computed
 *       directly in Java. See {@link #variance(List)}.</li>
 *   <li><b>Line 5 (varFA > 0.6 && varFF <= 0.5)</b>: Applied after computing the variance;
 *       only windows satisfying both conditions are emitted as brake anomaly alerts.</li>
 * </ul>
 *
 * <p><b>FA / FF field substitution:</b>
 * <br>The SNCB dataset contains the pressure fields FA (automatic brake pipe pressure, bar)
 * and FF (fictitious brake cylinder pressure, bar). The AIS dataset does not have pressure
 * sensors; instead:
 * <ul>
 *   <li><b>FA</b> is simulated from {@code speed} (knots), normalised to a [0, 7] bar range
 *       via {@code speed / MAX_SPEED_KNOTS * 7.0}. A vessel decelerating produces pressure
 *       fluctuation analogous to FA.</li>
 *   <li><b>FF</b> is simulated from {@code course} (degrees), normalised to a [0, 7] bar range
 *       via {@code course / 360.0 * 7.0}. Course changes introduce variation analogous to FF.</li>
 * </ul>
 * The thresholds {@link #VAR_FA_THRESHOLD} and {@link #VAR_FF_THRESHOLD} have been tuned
 * accordingly (see constants).
 *
 * <p><b>HOW TO RUN</b>
 * <br>In the Dockerfile, change the entrypoint from {@code aisdata.Main} to
 * {@code aisdata.Query2_Main}, then run:
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
public class Query2_Main {

    private static final Logger logger = LoggerFactory.getLogger(Query2_Main.class);

    /**
     * Variance threshold for FA (brake pipe pressure) in bar².
     *
     * <p>The paper condition is {@code varFA > 0.6}. Applied here to the speed-derived
     * FA proxy.
     */
    private static final double VAR_FA_THRESHOLD = 0.6;

    /**
     * Variance threshold for FF (fictitious brake cylinder pressure) in bar².
     *
     * <p>The paper condition is {@code varFF <= 0.5}. Applied here to the course-derived
     * FF proxy.
     */
    private static final double VAR_FF_THRESHOLD = 0.5;

    /**
     * Maximum expected vessel speed in knots, used to normalise {@code speed} to a
     * [0, 7] bar range as a proxy for FA. Typical max AIS speed for large vessels is ~25 kn.
     */
    private static final double MAX_SPEED_KNOTS = 25.0;

    /**
     * Maintenance area exclusion polygons - the <b>INPolygons</b> referenced in paper Line 2:
     * <i>"filters the SNCB stream to include only points that are not in a maintenance area
     * (INPolygons), by applying the function eintersects_tgeo_geo"</i>.
     *
     * <p>Plain WKT strings parsed with {@code geog_in(wkt, -1)} (SRID=4326 implicit),
     * consistent with the {@code tgeogpoint} created by {@code tgeogpoint_in}.
     *
     * <p>In the paper these represent railway maintenance depots where brake monitoring is
     * not meaningful. Here they are placed in open sea areas away from the vessel clusters
     * in {@code ais_instants.csv} so that virtually no AIS event is excluded, preserving
     * data volume for the variance computation demonstration.
     *
     * <p>To observe the exclusion filter in action, move a polygon to overlap a vessel cluster
     * (e.g. set ZONE 1 to cover the Kattegat position of MMSI 265513270).
     */
    private static final String[] MAINTENANCE_AREAS_WKT = {

            // Maintenance area A - open North Sea (no vessel traffic)
            "POLYGON((3.0000 56.0000, 3.0000 56.2000, 3.2000 56.2000, 3.2000 56.0000, 3.0000 56.0000))",

            // Maintenance area B - open Baltic (no vessel traffic)
            "POLYGON((15.0000 57.0000, 15.0000 57.2000, 15.2000 57.2000, 15.2000 57.0000, 15.0000 57.0000))"
    };

    // -----------------------------------------------------------------------
    // Entry point
    // -----------------------------------------------------------------------

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
            properties.setProperty("group.id", "flink_consumer_q2");
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName());
            properties.setProperty("auto.offset.reset", "earliest");

            KafkaSource<AISData> kafkaSource = KafkaSource.<AISData>builder()
                    .setBootstrapServers("kafka:29092")
                    .setGroupId("flink_consumer_q2")
                    .setTopics("aisdata")
                    .setStartingOffsets(OffsetsInitializer.earliest())
                    .setValueOnlyDeserializer(new AISDataDeserializationSchema())
                    .build();

            // Same watermark strategy as Query 1 (see Query1_Main for detailed comments).
            DataStream<AISData> source = env
                    .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
                    .assignTimestampsAndWatermarks(
                            WatermarkStrategy.<AISData>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                    .withTimestampAssigner(
                                            (event, recordTs) -> event.getTimestamp())
                                    .withIdleness(Duration.ofMinutes(1)));

            // Pipeline implementing the MobilityNebula Query 2 pseudocode (paper Section 4.1):
            //
            //   keyBy(getMmsi)            → partitions the stream per vessel.
            //   window(Sliding 10s/10ms)  → paper Line 3: overlapping 10-second windows
            //                               advancing every 10 milliseconds.
            //   process(WindowFunction)   → paper Lines 2, 4, 5:
            //                                 - Line 2: eintersects_tgeo_geo exclusion filter
            //                                 - Line 4: variance(FA) and variance(FF)
            //                                 - Line 5: varFA > 0.6 && varFF <= 0.5
            //   print()                   → output brake anomaly alerts.
            source
                    .keyBy(AISData::getMmsi)
                    .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.milliseconds(10)))
                    .process(new BrakeMonitoringWindowFunction(
                            MAINTENANCE_AREAS_WKT, VAR_FA_THRESHOLD, VAR_FF_THRESHOLD))
                    .print();

            env.execute("Query 2 - Brake System Monitoring");
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
    // Helpers
    // -----------------------------------------------------------------------

    /**
     * Computes the statistical variance of a list of values.
     *
     * <p>This is the Java implementation of the {@code variation} operator from the paper
     * (Line 4). MEOS does not expose a {@code variation} function in its Java bindings, so
     * the computation is performed directly here.
     *
     * Formula : Sigma([(val - avg)^2] for each val) / number_of_val
     *
     * @param values sample values within one window (e.g. FA or FF readings)
     * @return variance in the same unit² as the input (bar² for the SNCB dataset), or
     *         {@code 0.0} if the list has fewer than two elements
     */
    static double variance(List<Double> values) {
        if (values.size() < 2) return 0.0;
        double mean = values.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
        double sumSq = values.stream().mapToDouble(v -> (v - mean) * (v - mean)).sum();
        return sumSq / values.size(); // population variance
    }

    // -----------------------------------------------------------------------
    // Window function
    // -----------------------------------------------------------------------

    /**
     * Flink {@link ProcessWindowFunction} implementing paper Lines 2, 4, and 5 for a single
     * (MMSI, sliding window) pair.
     *
     * <p>Called by Flink once per closed window. Processing order within the window:
     * <ol>
     *   <li>For each event, test against maintenance areas via {@code eintersects_tgeo_geo}
     *       (paper Line 2). Skip the event if it intersects any area.</li>
     *   <li>Extract FA and FF proxy values from the surviving events.</li>
     *   <li>Compute {@code varFA} and {@code varFF} via {@link Query2_Main#variance}
     *       (paper Line 4).</li>
     *   <li>Emit an alert only if {@code varFA > VAR_FA_THRESHOLD && varFF <= VAR_FF_THRESHOLD}
     *       (paper Line 5).</li>
     * </ol>
     */
    public static class BrakeMonitoringWindowFunction
            extends ProcessWindowFunction<AISData, String, Integer, TimeWindow> {

        private static final Logger log =
                LoggerFactory.getLogger(BrakeMonitoringWindowFunction.class);

        private final String[] maintenanceAreasWkt;
        private final double varFaThreshold;
        private final double varFfThreshold;

        private transient Pointer[] maintenanceZones;

        private transient error_handler_fn errorHandler;

        private static final DateTimeFormatter TIMESTAMP_FMT =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssxxx");

        public BrakeMonitoringWindowFunction(
                String[] maintenanceAreasWkt,
                double varFaThreshold,
                double varFfThreshold) {
            this.maintenanceAreasWkt = maintenanceAreasWkt;
            this.varFaThreshold = varFaThreshold;
            this.varFfThreshold = varFfThreshold;
        }

        /** Initialises the MEOS library for this operator instance. */
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            errorHandler = new error_handler();
            functions.meos_initialize_timezone("UTC");
            functions.meos_initialize_error_handler(errorHandler);
            // Parse the maintenance area polygons only once per worker.
            // geog_in(wkt, -1) creates a geography type (SRID=4326 implicit), consistent
            // with the tgeogpoint created by tgeogpoint_in below.
            maintenanceZones = new Pointer[maintenanceAreasWkt.length];
            for (int i = 0; i < maintenanceAreasWkt.length; i++) {
                maintenanceZones[i] = functions.geog_in(maintenanceAreasWkt[i], -1);
                if (maintenanceZones[i] == null) {
                    log.error("geog_in returned null for maintenance area {}", i + 1);
                }
            }
            log.info("MEOS initialized in BrakeMonitoringWindowFunction.open(), {} maintenance zones parsed", maintenanceZones.length);
        }

        /**
         * Applies paper Lines 2, 4, and 5 for all events in one sliding window.
         *
         * @param mmsi     vessel identifier (the key used by keyBy)
         * @param context  window metadata (start/end timestamps)
         * @param elements all AISData events in this (MMSI, window) pair
         * @param out      collector for brake anomaly alert strings
         */
        @Override
        public void process(
                Integer mmsi,
                Context context,
                Iterable<AISData> elements,
                Collector<String> out) {

            String windowStart = millisToTimestamp(context.window().getStart());
            String windowEnd   = millisToTimestamp(context.window().getEnd());

            // Collect FA and FF values from events that pass the maintenance area filter.
            List<Double> faValues = new ArrayList<>();
            List<Double> ffValues = new ArrayList<>();

            for (AISData event : elements) {

                String ts = millisToTimestamp(event.getTimestamp());

                // Build a temporal geography point for this event: same approach as Query 1.
                String tpointWkt = String.format(
                        "POINT(%f %f)@%s", event.getLon(), event.getLat(), ts);

                Pointer tpoint = functions.tgeogpoint_in(tpointWkt);
                if (tpoint == null) {
                    log.error("tgeogpoint_in returned null for WKT: {}", tpointWkt);
                    continue;
                }

                // Paper Line 2: eintersects_tgeo_geo(lon, lat, ts, INPolygons) == 0
                // Skip the event if it intersects any maintenance area.
                // eintersects_tgeo_geo returns 1 if the temporal point ever intersects the
                // polygon, 0 otherwise. We keep only non-intersecting points (== 0).
                boolean inMaintenanceArea = false;
                for (Pointer zone : maintenanceZones) {
                    if (zone == null) continue;
                    if (functions.eintersects_tgeo_geo(tpoint, zone) == 1) {
                        inMaintenanceArea = true;
                        log.debug("MMSI={} skipped: point intersects maintenance area at ts={}",
                                mmsi, ts);
                        break;
                    }
                }
                if (inMaintenanceArea) continue;

                // Paper Line 4: extract FA and FF proxy values from surviving events.
                //
                // FA proxy: automatic brake pipe pressure (bar):
                //   Derived from speed (knots), normalised to [0, MAX_SPEED_KNOTS] → [0, 7] bar.
                double fa = (event.getSpeed() / MAX_SPEED_KNOTS) * 7.0;

                // FF proxy: fictitious brake cylinder pressure (bar):
                //   Derived from course (degrees [0, 360]), normalised to [0, 7] bar.
                double ff = (event.getCourse() / 360.0) * 7.0;

                faValues.add(fa);
                ffValues.add(ff);
            }

            if (faValues.isEmpty()) return; // no surviving events in this window

            // Paper Line 4: variation(FA) and variation(FF): statistical variance
            double varFA = variance(faValues);
            double varFF = variance(ffValues);

            // Paper Line 5: varFA > 0.6 && varFF <= 0.5
            if (varFA > varFaThreshold && varFF <= varFfThreshold) {
                String alert = String.format(
                        "[ALERT][Q2] MMSI=%-12d | varFA=%6.4f bar² (>%.1f) | varFF=%6.4f bar² (<=%.1f)"
                                + " | events=%d | window [%s - %s]",
                        mmsi,
                        varFA, varFaThreshold,
                        varFF, varFfThreshold,
                        faValues.size(),
                        windowStart, windowEnd);

                log.warn(alert);
                out.collect(alert);
            }
        }

        private String millisToTimestamp(long millis) {
            Instant instant = Instant.ofEpochMilli(millis);
            OffsetDateTime dt = instant.atOffset(ZoneOffset.UTC);
            return dt.format(TIMESTAMP_FMT);
        }
    }
}