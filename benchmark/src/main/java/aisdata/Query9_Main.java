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
import org.apache.flink.api.common.functions.CoGroupFunction;
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
import functions.GeneratedFunctions;
import functions.error_handler;

/**
 * Query 9 - Windowed Per-Device kNN Join
 *
 * <p>This query is a Java/Flink adaptation of the MobilityNebula Query 9 described in the paper
 * <i>"MobilityNebula: A System for Processing Mobility Data Streams with MobilityDB"</i>.
 *
 * <p>The paper describes Query 9 as follows (Section 4.1):
 * <blockquote>
 *   "Query 9 performs a windowed kNN. It forms pairs by joining the stream and excluding
 *   device_id == device_id2 (Line 2). Within each ten-second tumbling window (Line 3), it
 *   computes pairwise distances using nearest_approach_distance (Line 4). It then groups by
 *   device_id and applies knn_agg(mindist, device_id2, 3) to retain the 3 nearest neighbors
 *   (Lines 5–6), emitting a neighbor summary (Line 7). The knn_agg operator is a per-window,
 *   per-device_id aggregation that keeps the k nearest neighbors according to a pre-computed
 *   distance attribute."
 * </blockquote>
 *
 * <p>The original MobilityNebula pseudocode is:
 * <pre>
 *   Query::from(GPS)
 *     .joinWith(GPS2, device_id != device_id2)                            // Line 2
 *     .window(TumblingWindow::of(EventTime(ts), Seconds(10)))             // Line 3
 *     .apply(nearest_approach_distance(lon, lat, ts, lon2, lat2, ts2))    // Line 4
 *     .groupBy(device_id)                                                 // Line 5
 *     .apply(knn_agg(mindist, device_id2, 3));                            // Line 6–7
 * </pre>
 *
 * <p><b>Difference from Query 7:</b>
 * <br>Query 7 uses {@code device_id &lt; device_id2} and computes a <em>global</em> top-K
 * across all vessel pairs. Query 9 uses {@code device_id != device_id2} and computes a
 * <em>per-device</em> top-K: each vessel independently finds its k nearest neighbours.
 * This means each pair {A,B} appears twice: once in A's neighbour list and once in B's
 * whereas in Query 7 each pair appears exactly once.
 *
 * <p><b>Mapping to this implementation:</b>
 * <ul>
 *   <li><b>Lines 2–4</b>: same {@code coGroup} approach as Query 7 (see {@link Query7_Main}),
 *       with the filter changed from {@code mmsi1 &lt; mmsi2} to {@code mmsi1 != mmsi2}.
 *   <li><b>Line 5 (groupBy device_id)</b>: after computing all pairwise distances, results
 *       are grouped by the left-side MMSI. Each vessel has its own list of (neighbour, dist)
 *       entries.</li>
 *   <li><b>Line 6 (knn_agg(mindist, device_id2, 3))</b>: for each vessel, the neighbour
 *       list is sorted by distance ascending and truncated to {@link #K} entries.</li>
 * </ul>
 *
 * <p><b>HOW TO RUN</b>
 * <br>In the Dockerfile, change the entrypoint from {@code aisdata.Main} to
 * {@code aisdata.Query9_Main}, then run:
 * <pre>
 *   mvn clean package &amp;&amp; docker compose up --build
 * </pre>
 */
public class Query9_Main {

    private static final Logger logger = LoggerFactory.getLogger(Query9_Main.class);

    /** Number of nearest neighbours to retain per device: the {@code k} in knn_agg. */
    private static final int K = 3;

    public static void main(String[] args) throws Exception {

        logger.info("Java library path: {}", System.getProperty("java.library.path"));

        try {
            GeneratedFunctions.meos_initialize_timezone("UTC");
            GeneratedFunctions.meos_initialize_error_handler(new error_handler());

            final StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);

            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName());
            properties.setProperty("auto.offset.reset", "earliest");

            KafkaSource<AISData> kafkaSourceLeft = KafkaSource.<AISData>builder()
                    .setBootstrapServers("kafka:29092")
                    .setGroupId("flink_consumer_q9_left")
                    .setTopics("aisdata")
                    .setStartingOffsets(OffsetsInitializer.earliest())
                    .setValueOnlyDeserializer(new AISDataDeserializationSchema())
                    .build();

            KafkaSource<AISData> kafkaSourceRight = KafkaSource.<AISData>builder()
                    .setBootstrapServers("kafka:29092")
                    .setGroupId("flink_consumer_q9_right")
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

            // Pipeline implementing the MobilityNebula Query 9 pseudocode:
            //
            //   coGroup (constant key) → delivers all events from both streams to one function.
            //   window(Tumbling 10s)   → paper Line 3.
            //   apply(KnnCoGroupFn)    → paper Lines 2, 4, 5, 6:
            //                             - mmsi1 != mmsi2 filter (Line 2)
            //                             - geog_distance per pair (Line 4)
            //                             - groupBy mmsi1 (Line 5)
            //                             - top-K per device (Line 6)
            //   print()                → paper Line 7.
            gps.coGroup(gps2)
                    .where(e -> 1)
                    .equalTo(e -> 1)
                    .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
                    .apply(new KnnCoGroupFunction(K))
                    .print();

            env.execute("Query 9 - Windowed Per-Device kNN Join");
            logger.info("Done");

        } catch (Exception e) {
            logger.error("Error during execution: {}", e.getMessage(), e);
            throw e;
        } finally {
            try {
                GeneratedFunctions.meos_finalize();
            } catch (Exception e) {
                logger.error("Error during MEOS finalization: {}", e.getMessage(), e);
            }
        }
    }

    // -----------------------------------------------------------------------
    // CoGroup function
    // -----------------------------------------------------------------------

    /**
     * Flink {@link CoGroupFunction} implementing paper Lines 2, 4, 5, and 6 for one
     * 10-second tumbling window.
     *
     * <p>Difference from {@link Query7_Main.ClosestPairsCoGroupFunction}:
     * <ul>
     *   <li>Filter: {@code mmsi1 != mmsi2} instead of {@code mmsi1 &lt; mmsi2}: each
     *       ordered pair (A→B) and (B→A) is kept separately, because A's kNN list and
     *       B's kNN list are independent.</li>
     *   <li>Aggregation: after computing the minimum distance per (mmsi1, mmsi2) pair,
     *       results are grouped by mmsi1 and the k smallest distances are retained per
     *       device (knn_agg). Query 7 sorted all pairs globally; here each device has
     *       its own sorted neighbour list of size k.</li>
     * </ul>
     */
    public static class KnnCoGroupFunction
            extends RichCoGroupFunction<AISData, AISData, String> {

        private static final Logger log = LoggerFactory.getLogger(KnnCoGroupFunction.class);

        private final int k;

        private static final DateTimeFormatter TIMESTAMP_FMT =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssxxx");

        public KnnCoGroupFunction(int k) {
            this.k = k;
        }

        private transient error_handler errorHandler;

        @Override
        public void open(OpenContext parameters) throws Exception {
            super.open(parameters);
            errorHandler = new error_handler();
            GeneratedFunctions.meos_initialize_timezone("UTC");
            GeneratedFunctions.meos_initialize_error_handler(errorHandler);
        }

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

            // Same precalculation logic as in Query 7
            List<Pointer> geoLefts  = new ArrayList<>(lefts.size());
            for (AISData left : lefts) {
                String ts = millisToTimestamp(left.getTimestamp());
                Pointer tp  = GeneratedFunctions.tgeogpoint_in(
                        String.format("POINT(%f %f)@%s", left.getLon(), left.getLat(), ts));
                Pointer geo = (tp != null) ? GeneratedFunctions.tgeo_end_value(tp) : null;
                geoLefts.add(geo); // null if tgeogpoint_in or temporal_end_value failed
            }

            List<Pointer> geoRights = new ArrayList<>(rights.size());
            for (AISData right : rights) {
                String ts = millisToTimestamp(right.getTimestamp());
                Pointer tp  = GeneratedFunctions.tgeogpoint_in(
                        String.format("POINT(%f %f)@%s", right.getLon(), right.getLat(), ts));
                Pointer geo = (tp != null) ? GeneratedFunctions.tgeo_end_value(tp) : null;
                geoRights.add(geo);
            }

            // Step 1: cross-product with mmsi1 != mmsi2, keeping min dist per (mmsi1, mmsi2).
            //
            // Unlike Query 7 (mmsi1 < mmsi2), here we keep BOTH (A,B) and (B,A) because
            // A's kNN list and B's kNN list are computed independently.
            // The deduplication map ensures we keep the minimum distance over all timestamp
            // combinations for each directed pair.
            Map<String, double[]> minDistMap = new HashMap<>();
            // value: [mmsi1, mmsi2, dist, lon1, lat1, lon2, lat2]

            for (int i = 0; i < lefts.size(); i++) {
                AISData left    = lefts.get(i);
                Pointer geoLeft = geoLefts.get(i);
                if (geoLeft == null) continue;

                for (int j = 0; j < rights.size(); j++) {
                    AISData right    = rights.get(j);
                    Pointer geoRight = geoRights.get(j);
                    if (geoRight == null) continue;

                    // Paper Line 2: device_id != device_id2
                    if (left.getMmsi() == right.getMmsi()) continue;

                    double dist = GeneratedFunctions.geog_distance(geoLeft, geoRight);

                    // Key: directed pair "mmsi1→mmsi2"
                    String key = left.getMmsi() + "->" + right.getMmsi();
                    if (!minDistMap.containsKey(key) || dist < minDistMap.get(key)[2]) {
                        minDistMap.put(key, new double[]{
                                left.getMmsi(), right.getMmsi(), dist,
                                left.getLon(), left.getLat(),
                                right.getLon(), right.getLat()});
                    }
                }
            }

            if (minDistMap.isEmpty()) return;

            // Step 2: groupBy device_id (paper Line 5):
            // Build a per-mmsi1 list of (mmsi2, dist) entries.
            Map<Integer, List<double[]>> byDevice = new HashMap<>();
            for (double[] entry : minDistMap.values()) {
                int mmsi1 = (int) entry[0];
                byDevice.computeIfAbsent(mmsi1, x -> new ArrayList<>()).add(entry);
            }

            // Step 3: knn_agg(mindist, device_id2, k) (paper Line 6):
            // For each device, sort by dist ascending and keep the k nearest neighbours.
            for (Map.Entry<Integer, List<double[]>> deviceEntry : byDevice.entrySet()) {
                int          mmsi1     = deviceEntry.getKey();
                List<double[]> neighbours = deviceEntry.getValue();

                neighbours.sort(Comparator.comparingDouble(e -> e[2]));

                int emitCount = Math.min(k, neighbours.size());
                StringBuilder sb = new StringBuilder();
                sb.append(String.format("[KNN][Q9] MMSI=%-12d | k=%d/%d neighbours:%n",
                        mmsi1, emitCount, neighbours.size()));

                for (int rank = 0; rank < emitCount; rank++) {
                    double[] nb = neighbours.get(rank);
                    sb.append(String.format(
                            "           rank=%d | neighbour=%-12d"
                                    + " (lon=%9.5f lat=%8.5f) | mindist=%10.3f m%n",
                            rank + 1, (long) nb[1], nb[5], nb[6], nb[2]));
                }

                String result = sb.toString().stripTrailing();
                log.info(result);
                out.collect(result);
            }
        }

        private String millisToTimestamp(long millis) {
            OffsetDateTime dt = Instant.ofEpochMilli(millis).atOffset(ZoneOffset.UTC);
            return dt.format(TIMESTAMP_FMT);
        }
    }
}