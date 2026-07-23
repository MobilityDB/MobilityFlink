package sncbdata;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jnr.ffi.Pointer;
import functions.GeneratedFunctions;
import functions.error_handler;

/**
 * Query 9 - Windowed Per-Device kNN Join (SNCB dataset)
 *
 * <p>For each train, finds its k nearest neighbours within each 10-second window.
 * With only 3 trains, K=2 (max 2 neighbours per device).
 *
 * <p>Original MobilityNebula pseudocode:
 * <pre>
 *   Query::from(GPS)
 *     .joinWith(GPS2, device_id != device_id2)                         // Line 2
 *     .window(TumblingWindow::of(EventTime(ts), Seconds(10)))          // Line 3
 *     .apply(nearest_approach_distance(lon, lat, ts, lon2, lat2, ts2)) // Line 4
 *     .groupBy(device_id)                                              // Line 5
 *     .apply(knn_agg(mindist, device_id2, 3));                        // Lines 6-7
 * </pre>
 *
 * <p><b>HOW TO RUN:</b> change Dockerfile entrypoint to {@code sncbdata.Query9_Main},
 * then {@code mvn clean package && docker compose up --build}.
 */
public class Query9_Main {

    private static final Logger logger = LoggerFactory.getLogger(Query9_Main.class);

    /**
     * K=2: with 3 trains each device has at most 2 neighbours.
     * Restore to 3+ for larger datasets.
     */
    private static final int K = 2;

    public static void main(String[] args) throws Exception {

        logger.info("Java library path: {}", System.getProperty("java.library.path"));

        try {
            GeneratedFunctions.meos_initialize_timezone("UTC");
            GeneratedFunctions.meos_initialize_error_handler(new error_handler());

            final StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);

            WatermarkStrategy<SNCBData> watermarkStrategy =
                    WatermarkStrategy.<SNCBData>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                            .withTimestampAssigner((event, recordTs) -> event.getTimestamp())
                            .withIdleness(Duration.ofMinutes(1));

            KafkaSource<SNCBData> kafkaSourceLeft = KafkaSource.<SNCBData>builder()
                    .setBootstrapServers("kafka:29092")
                    .setGroupId("flink_consumer_q9_left")
                    .setTopics("sncbdata")
                    .setStartingOffsets(OffsetsInitializer.earliest())
                    .setValueOnlyDeserializer(new SNCBDataDeserializationSchema())
                    .build();

            KafkaSource<SNCBData> kafkaSourceRight = KafkaSource.<SNCBData>builder()
                    .setBootstrapServers("kafka:29092")
                    .setGroupId("flink_consumer_q9_right")
                    .setTopics("sncbdata")
                    .setStartingOffsets(OffsetsInitializer.earliest())
                    .setValueOnlyDeserializer(new SNCBDataDeserializationSchema())
                    .build();

            DataStream<SNCBData> gps  = env.fromSource(kafkaSourceLeft,  watermarkStrategy, "Kafka GPS");
            DataStream<SNCBData> gps2 = env.fromSource(kafkaSourceRight, watermarkStrategy, "Kafka GPS2");

            gps.coGroup(gps2)
                    .where(e -> 1)
                    .equalTo(e -> 1)
                    .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
                    .apply(new KnnCoGroupFunction(K))
                    .print();

            env.execute("Query 9 - Windowed Per-Device kNN Join (SNCB)");

        } catch (Exception e) {
            logger.error("Error during execution: {}", e.getMessage(), e);
            throw e;
        } finally {
            try { GeneratedFunctions.meos_finalize(); }
            catch (Exception e) { logger.error("Error during MEOS finalization: {}", e.getMessage(), e); }
        }
    }

    public static class KnnCoGroupFunction
            extends RichCoGroupFunction<SNCBData, SNCBData, String> {

        private static final Logger log = LoggerFactory.getLogger(KnnCoGroupFunction.class);
        private static final DateTimeFormatter TIMESTAMP_FMT =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssxxx");

        private final int k;

        // transient: error_handler wraps a native C callback — not serializable by Flink.
        // Initialized once per worker in open().
        private transient error_handler errorHandler;

        public KnnCoGroupFunction(int k) { this.k = k; }

        @Override
        public void open(OpenContext parameters) throws Exception {
            super.open(parameters);
            errorHandler = new error_handler();
            GeneratedFunctions.meos_initialize_timezone("UTC");
            GeneratedFunctions.meos_initialize_error_handler(errorHandler);
        }

        @Override
        public void coGroup(Iterable<SNCBData> leftEvents, Iterable<SNCBData> rightEvents,
                Collector<String> out) throws Exception {

            List<SNCBData> lefts  = new ArrayList<>();
            List<SNCBData> rights = new ArrayList<>();
            for (SNCBData e : leftEvents)  lefts.add(e);
            for (SNCBData e : rightEvents) rights.add(e);
            if (lefts.isEmpty() || rights.isEmpty()) return;

            // Precalculate geo Pointers for every event BEFORE the double loop.
            // Without this, tgeogpoint_in + temporal_end_value would be called once per
            // pair: O(N×M).
            List<Pointer> geoLefts = new ArrayList<>(lefts.size());
            for (SNCBData left : lefts) {
                String ts = millisToTimestamp(left.getTimestamp());
                Pointer tp  = GeneratedFunctions.tgeogpoint_in(
                        String.format("POINT(%f %f)@%s", left.getLon(), left.getLat(), ts));
                Pointer geo = (tp != null) ? GeneratedFunctions.tgeo_end_value(tp) : null;
                geoLefts.add(geo);
            }

            List<Pointer> geoRights = new ArrayList<>(rights.size());
            for (SNCBData right : rights) {
                String ts = millisToTimestamp(right.getTimestamp());
                Pointer tp  = GeneratedFunctions.tgeogpoint_in(
                        String.format("POINT(%f %f)@%s", right.getLon(), right.getLat(), ts));
                Pointer geo = (tp != null) ? GeneratedFunctions.tgeo_end_value(tp) : null;
                geoRights.add(geo);
            }

            // Cross-product with deviceId1 != deviceId2 (Line 2), min-dist per directed pair.
            // value: [deviceId1, deviceId2, dist, lon1, lat1, lon2, lat2]
            // Both (A→B) and (B→A) are kept because A's kNN list and B's are independent.
            Map<String, double[]> minDistMap = new HashMap<>();

            int leftsSize = lefts.size();
            int rightsSize = rights.size();
            for (int i = 0; i < leftsSize; i++) {
                SNCBData left    = lefts.get(i);
                Pointer geoLeft  = geoLefts.get(i);
                if (geoLeft == null) continue;

                for (int j = 0; j < rightsSize; j++) {
                    SNCBData right   = rights.get(j);
                    Pointer geoRight = geoRights.get(j);
                    if (geoRight == null) continue;

                    // Paper Line 2: device_id != device_id2
                    if (left.getDeviceId() == right.getDeviceId()) continue;

                    double dist = GeneratedFunctions.geog_distance(geoLeft, geoRight);
                    String key = left.getDeviceId() + "->" + right.getDeviceId();
                    if (!minDistMap.containsKey(key) || dist < minDistMap.get(key)[2]) {
                        minDistMap.put(key, new double[]{
                                left.getDeviceId(), right.getDeviceId(), dist,
                                left.getLon(), left.getLat(),
                                right.getLon(), right.getLat()});
                    }
                }
            }

            if (minDistMap.isEmpty()) return;

            // Paper Line 5: groupBy device_id (left side)
            Map<Integer, List<double[]>> byDevice = new HashMap<>();
            for (double[] entry : minDistMap.values()) {
                int deviceId1 = (int) entry[0];
                byDevice.computeIfAbsent(deviceId1, x -> new ArrayList<>()).add(entry);
            }

            // Paper Line 6: knn_agg: keep k nearest neighbours per device
            for (Map.Entry<Integer, List<double[]>> deviceEntry : byDevice.entrySet()) {
                int            deviceId1  = deviceEntry.getKey();
                List<double[]> neighbours = deviceEntry.getValue();

                neighbours.sort(Comparator.comparingDouble(e -> e[2]));

                int emitCount = Math.min(k, neighbours.size());
                StringBuilder sb = new StringBuilder();
                sb.append(String.format("[KNN][Q9] DeviceID=%-6d | k=%d/%d neighbours:%n",
                        deviceId1, emitCount, neighbours.size()));

                for (int rank = 0; rank < emitCount; rank++) {
                    double[] nb = neighbours.get(rank);
                    sb.append(String.format(
                            "           rank=%d | neighbour=%-6d"
                                    + " (lon=%9.5f lat=%8.5f) | mindist=%10.3f m%n",
                            rank + 1, (int) nb[1], nb[5], nb[6], nb[2]));
                }

                String result = sb.toString().stripTrailing();
                log.info(result);
                out.collect(result);
            }
        }

        private String millisToTimestamp(long millis) {
            return Instant.ofEpochMilli(millis).atOffset(ZoneOffset.UTC).format(TIMESTAMP_FMT);
        }
    }
}
