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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jnr.ffi.Pointer;
import functions.functions;
import functions.error_handler;

/**
 * Query 7 - Global Closest Device Pairs (Top-k)
 *
 * <p>Finds the globally closest train pairs within each 10-second window.
 *
 * <p>Original MobilityNebula pseudocode:
 * <pre>
 *   Query::from(GPS)
 *     .joinWith(GPS2, device_id &lt; device_id2)                        // Line 2
 *     .window(TumblingWindow::of(EventTime(ts), Seconds(10)))          // Line 3
 *     .apply(nearest_approach_distance(lon, lat, ts, lon2, lat2, ts2)) // Line 4
 *     .apply(topK(mindist, 10));                                       // Lines 5-6
 * </pre>
 *
 * <p><b>HOW TO RUN:</b> change Dockerfile entrypoint to {@code sncbdata.Query7_Main},
 * then {@code mvn clean package && docker compose up --build}.
 */
public class Query7_Main {

    private static final Logger logger = LoggerFactory.getLogger(Query7_Main.class);

    /**
     * TOP_K=3: with 3 trains in input_sncb.csv there are at most 3 unique pairs (3×4, 3×5, 4×5).
     * Restore to 10+ for larger datasets.
     */
    private static final int TOP_K = 3;

    public static void main(String[] args) throws Exception {

        logger.info("Java library path: {}", System.getProperty("java.library.path"));

        try {
            functions.meos_initialize_timezone("UTC");
            functions.meos_initialize_error_handler(new error_handler());

            final StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);

            WatermarkStrategy<SNCBData> watermarkStrategy =
                    WatermarkStrategy.<SNCBData>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                            .withTimestampAssigner((event, recordTs) -> event.getTimestamp())
                            .withIdleness(Duration.ofMinutes(1));

            KafkaSource<SNCBData> kafkaSourceLeft = KafkaSource.<SNCBData>builder()
                    .setBootstrapServers("kafka:29092")
                    .setGroupId("flink_consumer_q7_left")
                    .setTopics("sncbdata")
                    .setStartingOffsets(OffsetsInitializer.earliest())
                    .setValueOnlyDeserializer(new SNCBDataDeserializationSchema())
                    .build();

            KafkaSource<SNCBData> kafkaSourceRight = KafkaSource.<SNCBData>builder()
                    .setBootstrapServers("kafka:29092")
                    .setGroupId("flink_consumer_q7_right")
                    .setTopics("sncbdata")
                    .setStartingOffsets(OffsetsInitializer.earliest())
                    .setValueOnlyDeserializer(new SNCBDataDeserializationSchema())
                    .build();

            DataStream<SNCBData> gps  = env.fromSource(kafkaSourceLeft,  watermarkStrategy, "Kafka GPS");
            DataStream<SNCBData> gps2 = env.fromSource(kafkaSourceRight, watermarkStrategy, "Kafka GPS2");

            // Constant key: all events routed to the same partition/worker for cross-device pairing
            gps.coGroup(gps2)
                    .where(e -> 1)
                    .equalTo(e -> 1)
                    .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                    .apply(new ClosestPairsCoGroupFunction(TOP_K))
                    .print();

            env.execute("Query 7 - Global Closest Device Pairs Top-K (SNCB)");

        } catch (Exception e) {
            logger.error("Error during execution: {}", e.getMessage(), e);
            throw e;
        } finally {
            try { functions.meos_finalize(); }
            catch (Exception e) { logger.error("Error during MEOS finalization: {}", e.getMessage(), e); }
        }
    }

    public static class ClosestPairsCoGroupFunction
            extends RichCoGroupFunction<SNCBData, SNCBData, String> {

        private static final Logger log = LoggerFactory.getLogger(ClosestPairsCoGroupFunction.class);
        private static final DateTimeFormatter TIMESTAMP_FMT =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssxxx");

        private final int topK;

        // Initialized once per worker in open().
        private transient error_handler errorHandler;

        public ClosestPairsCoGroupFunction(int topK) { this.topK = topK; }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            errorHandler = new error_handler();
            functions.meos_initialize_timezone("UTC");
            functions.meos_initialize_error_handler(errorHandler);
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
                Pointer tp  = functions.tgeogpoint_in(
                        String.format("POINT(%f %f)@%s", left.getLon(), left.getLat(), ts));
                Pointer geo = (tp != null) ? functions.tgeo_end_value(tp) : null;
                geoLefts.add(geo);
            }

            List<Pointer> geoRights = new ArrayList<>(rights.size());
            for (SNCBData right : rights) {
                String ts = millisToTimestamp(right.getTimestamp());
                Pointer tp  = functions.tgeogpoint_in(
                        String.format("POINT(%f %f)@%s", right.getLon(), right.getLat(), ts));
                Pointer geo = (tp != null) ? functions.tgeo_end_value(tp) : null;
                geoRights.add(geo);
            }

            // Cross-product with deviceId1 < deviceId2 (Line 2) + min-distance.
            // Single HashMap: value = [deviceId1, deviceId2, dist, lon1, lat1, lon2, lat2].
            Map<String, double[]> pairMap = new HashMap<>();

            int leftSize = lefts.size();
            int rightSize = rights.size();
            for (int i = 0; i < leftSize; i++) {
                SNCBData left     = lefts.get(i);
                Pointer geoLeft  = geoLefts.get(i);
                if (geoLeft == null) continue;

                for (int j = 0; j < rightSize; j++) {
                    SNCBData right    = rights.get(j);
                    Pointer geoRight = geoRights.get(j);
                    if (geoRight == null) continue;

                    // Paper Line 2: device_id < device_id2
                    if (left.getDeviceId() >= right.getDeviceId()) continue;

                    double dist = functions.geog_distance(geoLeft, geoRight);
                    String key = left.getDeviceId() + ":" + right.getDeviceId();
                    if (!pairMap.containsKey(key) || dist < pairMap.get(key)[2]) {
                        pairMap.put(key, new double[]{
                                left.getDeviceId(), right.getDeviceId(), dist,
                                left.getLon(), left.getLat(),
                                right.getLon(), right.getLat()});
                    }
                }
            }

            if (pairMap.isEmpty()) return;

            // Paper Lines 5-6: sort by mindist ascending, keep top-K
            List<double[]> pairs = new ArrayList<>(pairMap.values());
            pairs.sort(Comparator.comparingDouble(e -> e[2]));

            int emitCount = Math.min(topK, pairs.size());
            for (int rank = 0; rank < emitCount; rank++) {
                double[] p = pairs.get(rank);
                String result = String.format(
                        "[TOPK][Q7] rank=%2d/%d"
                                + " | Device1=%-6d (lon=%9.5f lat=%8.5f)"
                                + " | Device2=%-6d (lon=%9.5f lat=%8.5f)"
                                + " | mindist=%10.3f m",
                        rank + 1, emitCount,
                        (int) p[0], p[3], p[4],
                        (int) p[1], p[5], p[6],
                        p[2]);
                log.info(result);
                out.collect(result);
            }
        }

        private String millisToTimestamp(long millis) {
            return Instant.ofEpochMilli(millis).atOffset(ZoneOffset.UTC).format(TIMESTAMP_FMT);
        }
    }
}
