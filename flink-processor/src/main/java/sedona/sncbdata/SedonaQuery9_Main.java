package sedona.sncbdata;

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
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sncbdata.SNCBData;
import sncbdata.SNCBDataDeserializationSchema;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * SedonaQuery9_Main (SNCB): Windowed Per-Device kNN Join
 *
 * MEOS (Query9_Main.java):
 *   - Line 2: coGroup on constant key, filter device_id1 != device_id2
 *   - Line 3: TumblingEventTimeWindows(10s)
 *   - Line 4: geog_distance(tgeo_end_value(tp1), tgeo_end_value(tp2))
 *   - Line 5: groupBy device_id1
 *   - Lines 6-7: knn_agg(mindist, device_id2, k)
 *
 * Sedona/JTS:
 *   - Line 2: identical
 *   - Line 3: identical
 *   - Line 4: JTS point.distance(point) * 111000
 *   - Line 5: identical
 *   - Lines 6-7: identical
 *
 * Difference from Q7:
 *   Q7: filter id1 < id2, global top-K across all pairs
 *   Q9: filter id1 != id2, per-device top-K (both (A->B) and (B->A) kept)
 *
 * Why no Table API:
 *   Same reason as Q7: cross-product across all devices requires collecting both
 *   iterables simultaneously. StreamTableEnvironment is not serializable and cannot
 *   be used inside a worker.
 */
public class SedonaQuery9_Main {

    private static final Logger logger = LoggerFactory.getLogger(SedonaQuery9_Main.class);

    private static final int K = 2;

    // MEOS: geog_distance(tgeo_end_value(tp1), tgeo_end_value(tp2))
    // JTS:  factory.createPoint(lon, lat).distance(factory.createPoint(lon2, lat2)) * 111000
    //
    // Difference from SedonaQuery7 ClosestPairsCoGroupFunction:
    //   - filter: id1 != id2 (both directions kept) instead of id1 < id2
    //   - aggregation: per-device kNN instead of global top-K
    static class KnnCoGroupFunction
            extends RichCoGroupFunction<SNCBData, SNCBData, String>
            implements Serializable {

        private static final Logger log = LoggerFactory.getLogger(KnnCoGroupFunction.class);

        private final int k;
        private transient GeometryFactory factory;

        public KnnCoGroupFunction(int k) { this.k = k; }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            factory = new GeometryFactory();
        }

        @Override
        public void coGroup(Iterable<SNCBData> leftEvents, Iterable<SNCBData> rightEvents,
                            Collector<String> out) throws Exception {

            List<SNCBData> lefts  = new ArrayList<>();
            List<SNCBData> rights = new ArrayList<>();
            for (SNCBData e : leftEvents)  lefts.add(e);
            for (SNCBData e : rightEvents) rights.add(e);
            if (lefts.isEmpty() || rights.isEmpty()) return;

            // Pre-compute JTS Points for every event before the double loop.
            List<Point> pointLefts = new ArrayList<>(lefts.size());
            for (SNCBData left : lefts) {
                pointLefts.add(factory.createPoint(new Coordinate(left.getLon(), left.getLat())));
            }

            List<Point> pointRights = new ArrayList<>(rights.size());
            for (SNCBData right : rights) {
                pointRights.add(factory.createPoint(new Coordinate(right.getLon(), right.getLat())));
            }

            // Cross-product with device_id1 != device_id2
            // Both (A->B) and (B->A) are kept because kNN lists are per-device and independent.
            // value: [id1, id2, dist, lon1, lat1, lon2, lat2]
            Map<String, double[]> minDistMap = new HashMap<>();

            for (int i = 0; i < lefts.size(); i++) {
                SNCBData left   = lefts.get(i);
                Point    ptLeft = pointLefts.get(i);

                for (int j = 0; j < rights.size(); j++) {
                    SNCBData right   = rights.get(j);
                    Point    ptRight = pointRights.get(j);

                    // Paper Line 2: device_id1 != device_id2
                    if (left.getDeviceId() == right.getDeviceId()) continue;

                    // JTS distance() returns degrees, * 111000 gives approx. meters
                    double dist = ptLeft.distance(ptRight) * 111_000.0;

                    // Directed pair key: "id1->id2"
                    String key = left.getDeviceId() + "->" + right.getDeviceId();
                    if (!minDistMap.containsKey(key) || dist < minDistMap.get(key)[2]) {
                        minDistMap.put(key, new double[]{
                                left.getDeviceId(),  right.getDeviceId(), dist,
                                left.getLon(),        left.getLat(),
                                right.getLon(),       right.getLat()});
                    }
                }
            }

            if (minDistMap.isEmpty()) return;

            // groupBy device_id1 - build per-device neighbour lists
            Map<Integer, List<double[]>> byDevice = new HashMap<>();
            for (double[] entry : minDistMap.values()) {
                int deviceId1 = (int) entry[0];
                byDevice.computeIfAbsent(deviceId1, x -> new ArrayList<>()).add(entry);
            }

            // For each device, sort by distance ascending and keep the k nearest neighbours.
            for (Map.Entry<Integer, List<double[]>> deviceEntry : byDevice.entrySet()) {
                int            deviceId1  = deviceEntry.getKey();
                List<double[]> neighbours = deviceEntry.getValue();

                neighbours.sort(Comparator.comparingDouble(e -> e[2]));

                int emitCount = Math.min(k, neighbours.size());
                StringBuilder sb = new StringBuilder();
                sb.append(String.format(
                        "[KNN][Q9-Sedona-SNCB] DeviceID=%-6d | k=%d/%d neighbours:%n",
                        deviceId1, emitCount, neighbours.size()));

                for (int rank = 0; rank < emitCount; rank++) {
                    double[] nb = neighbours.get(rank);
                    sb.append(String.format(
                            "           rank=%d | neighbour=%-6d"
                                    + " (lon=%10.5f lat=%9.5f) | mindist=%10.3f m%n",
                            rank + 1, (int) nb[1], nb[5], nb[6], nb[2]));
                }

                String result = sb.toString().stripTrailing();
                log.info(result);
                out.collect(result);
            }
        }
    }

    // =========================================================================
    // MAIN
    // =========================================================================
    public static void main(String[] args) throws Exception {
        logger.info("=== SedonaQuery9_Main (SNCB), starting (no MEOS) ===");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        WatermarkStrategy<SNCBData> watermarkStrategy =
                WatermarkStrategy.<SNCBData>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner((event, ts) -> event.getTimestamp())
                        .withIdleness(Duration.ofMinutes(1));

        KafkaSource<SNCBData> kafkaSourceLeft = KafkaSource.<SNCBData>builder()
                .setBootstrapServers("kafka:29092")
                .setGroupId("flink_consumer_sedona_sncb_q9_left")
                .setTopics("sncbdata")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SNCBDataDeserializationSchema())
                .build();

        KafkaSource<SNCBData> kafkaSourceRight = KafkaSource.<SNCBData>builder()
                .setBootstrapServers("kafka:29092")
                .setGroupId("flink_consumer_sedona_sncb_q9_right")
                .setTopics("sncbdata")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SNCBDataDeserializationSchema())
                .build();

        DataStream<SNCBData> gps  = env.fromSource(kafkaSourceLeft,  watermarkStrategy, "Kafka GPS");
        DataStream<SNCBData> gps2 = env.fromSource(kafkaSourceRight, watermarkStrategy, "Kafka GPS2");

        // Constant key: all events routed to the same partition for cross-device pairing
        gps.coGroup(gps2)
                .where(e -> 1)
                .equalTo(e -> 1)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new KnnCoGroupFunction(K))
                .print();

        logger.info("Job running...");
        env.execute("SedonaQuery9-SNCB, Windowed Per-Device kNN Join (no MEOS)");
    }
}