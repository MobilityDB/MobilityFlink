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
 * Query 4 - Trajectory Creation in a Restricted Space
 *
 * <p>This query is a Java/Flink adaptation of the MobilityNebula Query 4 described in the paper
 * <i>"MobilityNebula: A System for Processing Mobility Data Streams with MobilityDB"</i>.
 *
 * <p>The paper describes Query 4 as follows (Section 4.1):
 * <blockquote>
 *   "Query 4 performs trajectory creation in a restricted space. It applies the function
 *   tgeo_at_stbox to keep records whose coordinates and timestamps fall within a specified
 *   spatiotemporal box (Line 2). It then applies a ten-second sliding window with a
 *   ten-millisecond step over the event timestamp (Line 3). Within each window, the query
 *   computes the trajectory using attributes longitude, latitude, and timestamp, and outputs
 *   the train position as a trajectory (Line 4)."
 * </blockquote>
 *
 * <p>The original MobilityNebula pseudocode is:
 * <pre>
 *   Query::from(GPS)
 *     .filter(tgeo_at_stbox(lon, lat, ts,                                    // Line 2
 *         stbox xt(((4.3,50),(4.5,50.6)),[2024-10-24,2024-11-26])) == 1)
 *     .window(SlidingWindow::of(EventTime(ts), Seconds(10), Milliseconds(10))) // Line 3
 *     .apply(temporal_sequence(lon, lat, ts));                                  // Line 4
 * </pre>
 *
 * <p><b>Mapping to this implementation:</b>
 * <ul>
 *   <li><b>Line 2 (tgeo_at_stbox)</b>: Applied inside
 *       {@link RestrictedTrajectoryWindowFunction#process} for each event. A {@code tgeogpoint}
 *       instant is constructed and passed to {@code tgeo_at_stbox} together with the
 *       spatiotemporal box. The function returns {@code null} if the point falls outside the
 *       box (spatially or temporally), or the restricted instant if it falls inside. Events
 *       returning {@code null} are skipped.
 *       <br><br>
 *       This differs from Queries 1 and 2 where the filter returned an {@code int} (0 or 1).
 *       {@code tgeo_at_stbox} returns the restricted temporal object itself & a null-check
 *       replaces the {@code == 1} comparison.</li>
 *   <li><b>Line 3 (sliding window 10s / 10ms)</b>:
 *       {@code SlidingEventTimeWindows.of(Time.seconds(10), Time.milliseconds(10))}.</li>
 *   <li><b>Line 4 (temporal_sequence)</b>: surviving events are
 *       assembled into a {@code tgeogpoint} sequence via {@code tgeogpoint_in} and serialised
 *       to EWKT via {@code tspatial_as_ewkt}.</li>
 * </ul>
 *
 * <p><b>What is a spatiotemporal box (STBox)?</b>
 * <br>An STBox constrains both space and time simultaneously:
 * <ul>
 *   <li><b>Spatial extent</b>: a bounding box defined by (xmin, ymin) and (xmax, ymax) in
 *       WGS-84 degrees. Only GPS points whose (lon, lat) falls within this rectangle are kept.</li>
 *   <li><b>Temporal extent</b>: a timestamp span [start, end]. Only events whose timestamp
 *       falls within this interval are kept.</li>
 * </ul>
 * The paper uses {@code stbox xt(((4.3,50),(4.5,50.6)),[2024-10-24,2024-11-26])} which covers
 * the Brussels region. Here the box is adapted to the AIS dataset (Danish waters, January 2021)
 *
 * <p><b>HOW TO RUN</b>
 * <br>In the Dockerfile, change the entrypoint from {@code aisdata.Main} to
 * {@code aisdata.Query4_Main}, then run:
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
public class Query4_Main {

    private static final Logger logger = LoggerFactory.getLogger(Query4_Main.class);

    /**
     * Spatiotemporal box used to restrict the stream — the {@code stbox xt} argument in
     * paper Line 2.
     *
     * <p>The paper uses the Brussels region with a 2024 timestamp range :
     * <pre>
     *   stbox xt(((4.3,50),(4.5,50.6)),[2024-10-24,2024-11-26])
     * </pre>
     *
     * <p>Here the box is adapted to the AIS dataset (Danish waters, January 2021):
     * <ul>
     *   <li><b>Spatial extent</b>: covers the Esbjerg / North Sea area where MMSI 566948000
     *       operates (lon 4.48–4.64, lat 55.55–55.66).</li>
     *   <li><b>Temporal extent</b>: covers the full day of the AIS dataset (2021-01-08).</li>
     * </ul>
     *
     */
    // Spatial bounds of the restricted zone (WGS-84 degrees).
    // Covers the Esbjerg / North Sea area where MMSI 566948000 operates.
    private static final double STBOX_XMIN = 4.48;
    private static final double STBOX_XMAX = 4.64;
    private static final double STBOX_YMIN = 55.55;
    private static final double STBOX_YMAX = 55.66;

    /**
     * Temporal bounds of the restricted zone as a MobilityDB tstzspan literal.
     * Covers the full day of the AIS dataset (2021-01-08).
     * Parsed by {@code tstzspan_in()} and passed to {@code stbox_make()}.
     *
     * The limits of the STBOX should let only the 566948000 MMSI ship pass and only its coordinates will be
     *      used to build the trajectory
     *
     * The 3 other ones should never appear since they don't fulfill the STBOX filter
     *          265513270
     *          219027804
     *          219001559
     */
    private static final String STBOX_TSPAN = "[2021-01-08 00:00:00+00, 2021-01-09 00:00:00+00]";


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
            properties.setProperty("group.id", "flink_consumer_q4");
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName());
            properties.setProperty("auto.offset.reset", "earliest");

            KafkaSource<AISData> kafkaSource = KafkaSource.<AISData>builder()
                    .setBootstrapServers("kafka:29092")
                    .setGroupId("flink_consumer_q4")
                    .setTopics("aisdata")
                    .setStartingOffsets(OffsetsInitializer.earliest())
                    .setValueOnlyDeserializer(new AISDataDeserializationSchema())
                    .build();

            // Same watermark strategy as Queries 1–3 (see Query1_Main for detailed comments).
            DataStream<AISData> source = env
                    .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
                    .assignTimestampsAndWatermarks(
                            WatermarkStrategy.<AISData>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                    .withTimestampAssigner(
                                            (event, recordTs) -> event.getTimestamp())
                                    .withIdleness(Duration.ofMinutes(1)));

            // Pipeline implementing the MobilityNebula Query 4 pseudocode (paper Section 4.1):
            //
            //   keyBy(getMmsi)            → partitions the stream per vessel.
            //   window(Sliding 10s/10ms)  → paper Line 3: overlapping 10-second windows
            //                               advancing every 10 milliseconds.
            //   process(WindowFunction)   → paper Lines 2 and 4:
            //                                 - Line 2: tgeo_at_stbox filter
            //                                 - Line 4: temporal_sequence assembly
            //   print()                   → outputs each restricted trajectory as EWKT.
            source
                    .keyBy(AISData::getMmsi)
                    .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.milliseconds(10)))
                    .process(new RestrictedTrajectoryWindowFunction(STBOX_XMIN, STBOX_XMAX, STBOX_YMIN, STBOX_YMAX, STBOX_TSPAN))
                    .print();

            env.execute("Query 4 - Trajectory Creation in a Restricted Space");
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
     * Flink {@link ProcessWindowFunction} implementing paper Lines 2 and 4 for a single
     * (MMSI, sliding window) pair.
     *
     * <p>Difference from Query 3's {@code TrajectoryCreationWindowFunction}: before assembling
     * the trajectory, each event is filtered through {@code tgeo_at_stbox} (paper Line 2).
     * Only events whose position AND timestamp fall within {@link #stbox} contribute to the
     * trajectory. Windows where no event survives the filter produce no output.
     */
    public static class RestrictedTrajectoryWindowFunction
            extends ProcessWindowFunction<AISData, String, Integer, TimeWindow> {

        private static final Logger log =
                LoggerFactory.getLogger(RestrictedTrajectoryWindowFunction.class);

        private final double xmin, xmax, ymin, ymax;
        private final String tspanLiteral;

        /**
         * Parsed STBox pointer. Initialised once in {@link #open} via {@code stbox_make()}
         * and reused across all window invocations since construction is expensive and the box
         * never changes. Declared transient because JNR-FFI Pointer objects are not serialisable.
         */
        private transient Pointer stbox;

        /** Initialised in {@link #open}: Pointer is not serialisable. */
        private transient error_handler_fn errorHandler;

        private static final DateTimeFormatter TIMESTAMP_FMT =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssxxx");

        public RestrictedTrajectoryWindowFunction(
                double xmin, double xmax, double ymin, double ymax, String tspanLiteral) {
            this.xmin = xmin;
            this.xmax = xmax;
            this.ymin = ymin;
            this.ymax = ymax;
            this.tspanLiteral = tspanLiteral;
        }

        /**
         * Initialises the MEOS library and parses the STBox once for all windows.
         *
         * <p>Parsing the STBox in {@code open()} rather than inside {@code process()} avoids
         * re-parsing the same WKT string on every window invocation (which can be thousands
         * per second with a 10ms step).
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            errorHandler = new error_handler();
            functions.meos_initialize_timezone("UTC");
            functions.meos_initialize_error_handler(errorHandler);

           // stbox_make parameters:
            //   hasx=true   → include spatial (XY) dimensions
            //   hasz=false  → no Z (altitude) dimension
            //   geodetic=true → geography/WGS-84, consistent with tgeogpoint_in
            //   srid=4326   → WGS-84
            //   xmin/xmax/ymin/ymax → spatial bounds (lon/lat in degrees)
            //   zmin/zmax=0 → unused (hasz=false)
            //   s           → temporal span pointer (tstzspan)
            Pointer tspan = functions.tstzspan_in(tspanLiteral);
            if (tspan == null) {
                log.error("tstzspan_in returned null for: {}", tspanLiteral);
                return;
            }
            stbox = functions.stbox_make(true, false, true, 4326,
                    xmin, xmax, ymin, ymax, 0, 0, tspan);
            if (stbox == null) {
                log.error("stbox_make returned null");
            } else {
                log.info("STBox built successfully: xmin={} xmax={} ymin={} ymax={} tspan={}",
                        xmin, xmax, ymin, ymax, tspanLiteral);
            }
            log.info("MEOS initialized in RestrictedTrajectoryWindowFunction.open()");
        }

        /**
         * Applies paper Lines 2 and 4 for all events in one sliding window.
         *
         * <p><b>Paper Line 2</b>: {@code tgeo_at_stbox(lon, lat, ts, stbox) == 1}:
         * <br>Each event is converted to a {@code tgeogpoint} instant and passed to
         * {@code tgeo_at_stbox}. The function returns:
         * <ul>
         *   <li>{@code null} if the point is outside the STBox (spatially or temporally):
         *       the event is skipped.</li>
         *   <li>a non-null {@code Pointer} if the point falls within the STBox: the event
         *       contributes to the trajectory.</li>
         * </ul>
         *
         * <p><b>Paper Line 4</b>: {@code temporal_sequence(lon, lat, ts)}:
         * <br>surviving events are sorted, concatenated into a sequence
         * literal, parsed by {@code tgeogpoint_in}, and serialised via {@code tspatial_as_ewkt}.
         *
         * @param mmsi     vessel identifier (the key used by keyBy)
         * @param context  window metadata (start/end timestamps)
         * @param elements all AISData events in this (MMSI, window) pair
         * @param out      collector for the restricted trajectory EWKT string
         */
        @Override
        public void process(
                Integer mmsi,
                Context context,
                Iterable<AISData> elements,
                Collector<String> out) {

            if (stbox == null) return; // STBox failed to parse in open()

            String windowStart = millisToTimestamp(context.window().getStart());
            String windowEnd   = millisToTimestamp(context.window().getEnd());

            // Collect events that pass the STBox filter, sorted by timestamp.
            List<AISData> surviving = new ArrayList<>();

            for (AISData event : elements) {

                String ts = millisToTimestamp(event.getTimestamp());

                // Build the tgeogpoint instant for this event.
                String tpointWkt = String.format(
                        "POINT(%f %f)@%s", event.getLon(), event.getLat(), ts);

                Pointer tpoint = functions.tgeogpoint_in(tpointWkt);
                if (tpoint == null) {
                    log.error("tgeogpoint_in returned null for WKT: {}", tpointWkt);
                    continue;
                }

                // Paper Line 2: tgeo_at_stbox(lon, lat, ts, stbox)
                // Returns null  → point is outside the STBox → skip.
                // Returns non-null → point is inside the STBox → keep.
                // border_inc=true means the box boundaries are inclusive ([xmin,xmax],
                // [ymin,ymax], [tsmin,tsmax]), matching the paper's closed-interval notation.
                Pointer restricted = functions.tgeo_at_stbox(tpoint, stbox, true);
                if (restricted == null) {
                    log.debug("MMSI={} skipped: point outside STBox at ts={}", mmsi, ts);
                    continue;
                }

                surviving.add(event);
            }

            if (surviving.isEmpty()) return; // no event survived the STBox filter

            // Sort by timestamp: required by tgeogpoint_in for sequence construction.
            surviving.sort((a, b) -> Long.compare(a.getTimestamp(), b.getTimestamp()));

            // Paper Line 4: temporal_sequence(lon, lat, ts).
            StringBuilder seq = new StringBuilder("{");
            for (int i = 0; i < surviving.size(); i++) {
                AISData event = surviving.get(i);
                String ts = millisToTimestamp(event.getTimestamp());
                if (i > 0) seq.append(",");
                seq.append(String.format("POINT(%f %f)@%s", event.getLon(), event.getLat(), ts));
            }
            seq.append("}");

            Pointer trajectory = functions.tgeogpoint_in(seq.toString());
            if (trajectory == null) {
                log.error("tgeogpoint_in returned null for sequence: {}", seq);
                return;
            }

            String trajectoryEwkt = functions.tspatial_as_ewkt(trajectory, 6);

            String output = String.format(
                    "[TRAJ][Q4] MMSI=%-12d | points=%3d | window [%s - %s]%n           trajectory: %s",
                    mmsi,
                    surviving.size(),
                    windowStart, windowEnd,
                    trajectoryEwkt);

            log.info(output);
            out.collect(output);
        }

        private String millisToTimestamp(long millis) {
            Instant instant = Instant.ofEpochMilli(millis);
            OffsetDateTime dt = instant.atOffset(ZoneOffset.UTC);
            return dt.format(TIMESTAMP_FMT);
        }
    }
}