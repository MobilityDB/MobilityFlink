package aisdata;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jnr.ffi.Pointer;
import functions.functions;
import functions.error_handler;

/**
 * Query 7 - Global Closest Device Pairs (Top-k Closest Pairs)
 *
 * <p>This query is a Java/Flink adaptation of the MobilityNebula Query 7 described in the paper
 * <i>"MobilityNebula: A System for Processing Mobility Data Streams with MobilityDB"</i>.
 *
 * <p>The paper describes Query 7 as follows (Section 4.1):
 * <blockquote>
 *   "Query 7 computes the closest device pairs (top-k) per window. It performs a join,
 *   keeping only pairs with device_id &lt; device_id2 (Line 2). In each ten-second tumbling
 *   window (Line 3), it computes a distance score using nearest_approach_distance (Line 4).
 *   Then it retains the smallest k results per window via a top-k selection (Line 5), emitting
 *   them to the sink (Line 6). The TopK operator performs a per-window global ranking and
 *   selection. Given all candidate result tuples in one window w with a numeric score attribute
 *   (here, mindist in meters), topK keeps only the k tuples with the smallest scores."
 * </blockquote>
 *
 * <p>The original MobilityNebula pseudocode is:
 * <pre>
 *   Query::from(GPS)
 *     .joinWith(GPS2, device_id &lt; device_id2)                             // Line 2
 *     .window(TumblingWindow::of(EventTime(ts), Seconds(10)))              // Line 3
 *     .apply(nearest_approach_distance(lon, lat, ts, lon2, lat2, ts2))     // Line 4
 *     .apply(topK(mindist, 10));                                           // Line 5–6
 * </pre>
 *
 * <p><b>Mapping to this implementation:</b>
 * <ul>
 *   <li><b>Line 2 (joinWith GPS2, device_id &lt; device_id2)</b>: Unlike Query 6 which
 *       self-joins on equality ({@code device_id == device_id2}), here the condition only pairs <em>different</em>
 *       vessels where the left MMSI is numerically smaller than the right.
 *       This ensures each unordered pair is produced exactly once (e.g. {A,B} but not {B,A}).
 *       <br><br>
 *       Flink's {@code join().where().equalTo()} API requires key equality and cannot
 *       express inequality predicates. We therefore use {@code coGroup} with a constant key
 *       (all events routed to the same partition), which delivers all left and right events
 *       in the window to a single function call. The {@code mmsi1 &lt; mmsi2} filter is then
 *       applied manually inside {@link ClosestPairsCoGroupFunction}.</li>
 *   <li><b>Line 3 (10-second tumbling window)</b>: {@code TumblingEventTimeWindows.of(Duration.ofSeconds(10))},
 *       same as Queries 1 and 6.</li>
 *   <li><b>Line 4 (nearest_approach_distance)</b>: Same as Query 6:
 *       {@code geog_distance(temporal_end_value(p1), temporal_end_value(p2))} gives the
 *       geodetic distance between two positions. See Query 6 Javadoc for the discussion on
 *       why {@code nad_tgeo_tgeo} cannot be used directly on TInstants.</li>
 *   <li><b>Lines 5–6 (topK(mindist, 10))</b>: After computing all inter-vessel distances
 *       within the window, the pairs are sorted by {@code mindist} ascending and only the
 *       {@link #TOP_K} smallest are emitted. This is the per-window global ranking described
 *       in the paper.</li>
 * </ul>
 *
 * <p><b>Why coGroup instead of join?</b>
 * <br>Flink's windowed join API ({@code stream.join(other).where(k1).equalTo(k2)}) requires
 * a key equality predicate, which maps to {@code device_id == device_id2}. {@code coGroup} sidesteps this by routing all
 * events from both streams in the window to a single function that receives two iterables
 * (left events, right events). We then compute the full cross-product manually and apply the
 * inequality filter.
 *
 * <p><b>HOW TO RUN</b>
 * <br>In the Dockerfile, change the entrypoint from {@code aisdata.Main} to
 * {@code aisdata.Query7_Main}, then run:
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
public class Query7_Main {

    private static final Logger logger = LoggerFactory.getLogger(Query7_Main.class);

    /**
     * Number of closest pairs to retain per window: the {@code k} in {@code topK(mindist, 10)}.
     * The paper uses k=10.
     */
    private static final int TOP_K = 10;

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
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName());
            properties.setProperty("auto.offset.reset", "earliest");

            // GPS: left stream
            KafkaSource<AISData> kafkaSourceLeft = KafkaSource.<AISData>builder()
                    .setBootstrapServers("kafka:29092")
                    .setGroupId("flink_consumer_q7_left")
                    .setTopics("aisdata")
                    .setStartingOffsets(OffsetsInitializer.earliest())
                    .setValueOnlyDeserializer(new AISDataDeserializationSchema())
                    .build();

            // GPS2: right stream (second logical view, separate consumer group)
            KafkaSource<AISData> kafkaSourceRight = KafkaSource.<AISData>builder()
                    .setBootstrapServers("kafka:29092")
                    .setGroupId("flink_consumer_q7_right")
                    .setTopics("aisdata")
                    .setStartingOffsets(OffsetsInitializer.earliest())
                    .setValueOnlyDeserializer(new AISDataDeserializationSchema())
                    .build();

            WatermarkStrategy<AISData> watermarkStrategy =
                    WatermarkStrategy.<AISData>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                            .withTimestampAssigner((event, recordTs) -> event.getTimestamp())
                            .withIdleness(Duration.ofMinutes(1));

            DataStream<AISData> gps  = env.fromSource(kafkaSourceLeft,  watermarkStrategy, "Kafka GPS");
            DataStream<AISData> gps2 = env.fromSource(kafkaSourceRight, watermarkStrategy, "Kafka GPS2");

            // Pipeline implementing the MobilityNebula Query 7 pseudocode (paper Section 4.1):
            //
            //   coGroup with constant key   → routes all events from both streams into the
            //                                 same window partition.
            //   window(Tumbling 10s)         → paper Line 3.
            //   apply(CoGroupFunction)       → paper Lines 2, 4, 5:
            //                                    - Line 2: mmsi1 < mmsi2 filter
            //                                    - Line 4: geog_distance
            //                                    - Line 5: sort by mindist, emit top-K
            //   print()                      → paper Line 6: sink.
            gps.coGroup(gps2)
                    .where(e -> 1)      // constant key: all events go to the same partition
                    .equalTo(e -> 1)    // same constant on right side
                    .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
                    .apply(new ClosestPairsCoGroupFunction(TOP_K))
                    .print();

            env.execute("Query 7 - Global Closest Device Pairs (Top-K)");
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
    // CoGroup function
    // -----------------------------------------------------------------------

    /**
     * Flink {@link RichCoGroupFunction} for one 10-second tumbling window.
     *
     * <p>Receives two iterables: all GPS events (left) and all GPS2 events (right) in the
     * window and processes them in three steps:
     * <ol>
     *   <li><b>Cross-product with filter (Line 2)</b>: builds all pairs
     *       {@code (left_i, right_j)} where {@code left_i.mmsi &lt; right_j.mmsi}, ensuring
     *       each unordered vessel pair appears exactly once.</li>
     *   <li><b>Distance computation (Line 4)</b>: for each pair, computes the geodetic
     *       distance between the two positions using {@code geog_distance}
     *       (see {@link Query6_Main} for the discussion on NAD approximation).</li>
     *   <li><b>Top-K selection (Line 5)</b>: sorts all pairs by {@code mindist} ascending
     *       and emits only the {@link #topK} smallest.</li>
     * </ol>
     */
    public static class ClosestPairsCoGroupFunction
            extends RichCoGroupFunction<AISData, AISData, String> {

        private static final Logger log =
                LoggerFactory.getLogger(ClosestPairsCoGroupFunction.class);

        private final int topK;

        private static final DateTimeFormatter TIMESTAMP_FMT =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssxxx");

        public ClosestPairsCoGroupFunction(int topK) {
            this.topK = topK;
        }

        private transient error_handler errorHandler;

        @Override
        public void open(OpenContext parameters) throws Exception {
            super.open(parameters);
            errorHandler = new error_handler();
            functions.meos_initialize_timezone("UTC");
            functions.meos_initialize_error_handler(errorHandler);
        }

        /**
         * Processes one window: computes all inter-vessel distances, sorts, emits top-K.
         *
         * @param leftEvents  all GPS events in this window (all MMSIs)
         * @param rightEvents all GPS2 events in this window (all MMSIs, same data)
         * @param out         collector for the top-K closest pair strings
         */
        @Override
        public void coGroup(
                Iterable<AISData> leftEvents,
                Iterable<AISData> rightEvents,
                Collector<String> out) throws Exception {

            List<AISData> lefts  = new ArrayList<>();
            List<AISData> rights = new ArrayList<>();
            for (AISData e : leftEvents)  lefts.add(e);
            for (AISData e : rightEvents) rights.add(e);

            if (lefts.isEmpty() || rights.isEmpty()) return;

            // Precalculate geo Pointers for every event in lefts and rights BEFORE the
            // double loop. Without this, tgeogpoint_in + temporal_end_value would be called
            // once per pair (i.e. lefts.size() × rights.size() times), meaning each event's
            // pointer is recomputed N times instead of once.
            //
            // Example: 20 navires × 5 events each → 100 events per side.
            // Without precalculation : 100 × 100 = 10 000 native calls.
            // With precalculation    : 100 + 100  =    200 native calls.
            List<Pointer> geoLefts  = new ArrayList<>(lefts.size());
            for (AISData left : lefts) {
                String ts = millisToTimestamp(left.getTimestamp());
                Pointer tp  = functions.tgeogpoint_in(
                        String.format("POINT(%f %f)@%s", left.getLon(), left.getLat(), ts));
                Pointer geo = (tp != null) ? functions.tgeo_end_value(tp) : null;
                geoLefts.add(geo); // null if tgeogpoint_in or temporal_end_value failed
            }

            List<Pointer> geoRights = new ArrayList<>(rights.size());
            for (AISData right : rights) {
                String ts = millisToTimestamp(right.getTimestamp());
                Pointer tp  = functions.tgeogpoint_in(
                        String.format("POINT(%f %f)@%s", right.getLon(), right.getLat(), ts));
                Pointer geo = (tp != null) ? functions.tgeo_end_value(tp) : null;
                geoRights.add(geo);
            }

            // Step 1 & 2: cross-product with mmsi1 < mmsi2 filter + distance computation.
            // Map keyed by "mmsi1:mmsi2" to keep only the minimum distance per unique pair.
            // value: [mmsi1, mmsi2, dist, lon1, lat1, lon2, lat2].
            // Without this deduplication, the cross-product would generate O(n^2) entries for
            // the same vessel pair (e.g. 10 events x 10 events = 100 combinations), and the
            // top-K would select 10 instances of the same pair rather than 10 distinct pairs.
            Map<String, double[]> pairMap = new HashMap<>();

            for (int i = 0; i < lefts.size(); i++) {
                AISData left    = lefts.get(i);
                Pointer geoLeft = geoLefts.get(i);
                if (geoLeft == null) continue;

                for (int j = 0; j < rights.size(); j++) {
                    AISData right    = rights.get(j);
                    Pointer geoRight = geoRights.get(j);
                    if (geoRight == null) continue;

                    // Paper Line 2: device_id < device_id2
                    if (left.getMmsi() >= right.getMmsi()) continue;

                    double dist = functions.geog_distance(geoLeft, geoRight);

                    // Keep only the minimum distance per unique (mmsi1, mmsi2) pair.
                    String key = left.getMmsi() + ":" + right.getMmsi();
                    if (!pairMap.containsKey(key) || dist < pairMap.get(key)[2]) {
                        pairMap.put(key, new double[]{
                                left.getMmsi(), right.getMmsi(), dist,
                                left.getLon(), left.getLat(),
                                right.getLon(), right.getLat()});
                    }
                }
            }

            if (pairMap.isEmpty()) return;

            // Step 3: top-K selection (paper Line 5): sort all pairs by mindist ascending,
            // keep only the topK smallest.
            List<double[]> pairs = new ArrayList<>(pairMap.values());
            pairs.sort(Comparator.comparingDouble(e -> e[2]));

            int emitCount = Math.min(topK, pairs.size());
            for (int rank = 0; rank < emitCount; rank++) {
                double[] p = pairs.get(rank);

                String result = String.format(
                        "[TOPK][Q7] rank=%2d/%d | MMSI1=%-12d (lon=%9.5f lat=%8.5f)"
                                + " | MMSI2=%-12d (lon=%9.5f lat=%8.5f)"
                                + " | mindist=%10.3f m",
                        rank + 1, emitCount,
                        (long) p[0], p[3], p[4],
                        (long) p[1], p[5], p[6],
                        p[2]);

                log.info(result);
                out.collect(result);
            }
        }

        private String millisToTimestamp(long millis) {
            Instant instant = Instant.ofEpochMilli(millis);
            OffsetDateTime dt = instant.atOffset(ZoneOffset.UTC);
            return dt.format(TIMESTAMP_FMT);
        }
    }
}