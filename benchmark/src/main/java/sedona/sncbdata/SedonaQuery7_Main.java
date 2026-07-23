package sedona.sncbdata;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
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
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SedonaQuery7_Main {

    private static final Logger logger = LoggerFactory.getLogger(SedonaQuery7_Main.class);

    private static final int TOP_K = 3;

    static class ClosestPairsCoGroupFunction
            extends RichCoGroupFunction<SNCBData, SNCBData, String>
            implements Serializable {

        private static final Logger log =
                LoggerFactory.getLogger(ClosestPairsCoGroupFunction.class);

        private static final DateTimeFormatter FMT =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssxxx");

        private final int topK;
        private transient GeometryFactory factory;

        public ClosestPairsCoGroupFunction(int topK) { this.topK = topK; }

        @Override
        public void open(OpenContext parameters) throws Exception {
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

            // Cross-product with device_id1 < device_id2  + min-distance per pair.
            // pairMap key: "id1:id2", value: [id1, id2, dist, lon1, lat1, lon2, lat2]
            Map<String, double[]> pairMap = new HashMap<>();

            int leftSize  = lefts.size();
            int rightSize = rights.size();

            for (int i = 0; i < leftSize; i++) {
                SNCBData left   = lefts.get(i);
                Point    ptLeft = pointLefts.get(i);

                for (int j = 0; j < rightSize; j++) {
                    SNCBData right   = rights.get(j);
                    Point    ptRight = pointRights.get(j);

                    // Paper Line 2: device_id1 < device_id2 - avoids duplicate pairs
                    if (left.getDeviceId() >= right.getDeviceId()) continue;

                    // Paper Line 4: nearest_approach_distance
                    // JTS distance() returns degrees, * 111000 gives approx. meters
                    double dist = ptLeft.distance(ptRight) * 111_000.0;

                    String key = left.getDeviceId() + ":" + right.getDeviceId();
                    if (!pairMap.containsKey(key) || dist < pairMap.get(key)[2]) {
                        pairMap.put(key, new double[]{
                                left.getDeviceId(), right.getDeviceId(), dist,
                                left.getLon(),      left.getLat(),
                                right.getLon(),     right.getLat()});
                    }
                }
            }

            if (pairMap.isEmpty()) return;

            // sort by mindist ascending, keep top-K
            List<double[]> pairs = new ArrayList<>(pairMap.values());
            pairs.sort(Comparator.comparingDouble(e -> e[2]));

            int emitCount = Math.min(topK, pairs.size());
            for (int rank = 0; rank < emitCount; rank++) {
                double[] p = pairs.get(rank);
                String result = String.format(
                        "[TOPK][Q7-Sedona-SNCB] rank=%2d/%d"
                                + " | Device1=%-6d (lon=%10.5f lat=%9.5f)"
                                + " | Device2=%-6d (lon=%10.5f lat=%9.5f)"
                                + " | mindist=%10.3f m",
                        rank + 1, emitCount,
                        (int) p[0], p[3], p[4],
                        (int) p[1], p[5], p[6],
                        p[2]);
                log.info(result);
                out.collect(result);
            }
        }
    }

    // =========================================================================
    // MAIN
    // =========================================================================
    public static void main(String[] args) throws Exception {
        logger.info("=== SedonaQuery7_Main (SNCB), starting (no MEOS) ===");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        WatermarkStrategy<SNCBData> watermarkStrategy =
                WatermarkStrategy.<SNCBData>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner((event, ts) -> event.getTimestamp())
                        .withIdleness(Duration.ofMinutes(1));

        KafkaSource<SNCBData> kafkaSourceLeft = KafkaSource.<SNCBData>builder()
                .setBootstrapServers("kafka:29092")
                .setGroupId("flink_consumer_sedona_sncb_q7_left")
                .setTopics("sncbdata")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SNCBDataDeserializationSchema())
                .build();

        KafkaSource<SNCBData> kafkaSourceRight = KafkaSource.<SNCBData>builder()
                .setBootstrapServers("kafka:29092")
                .setGroupId("flink_consumer_sedona_sncb_q7_right")
                .setTopics("sncbdata")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SNCBDataDeserializationSchema())
                .build();

        DataStream<SNCBData> gps  = env.fromSource(kafkaSourceLeft,  watermarkStrategy, "Kafka GPS");
        DataStream<SNCBData> gps2 = env.fromSource(kafkaSourceRight, watermarkStrategy, "Kafka GPS2");

        // Constant key: all events routed to the same partition for cross-device pairing
        // coGroup instead of join: we need both iterables simultaneously to build the cross-product
        gps.coGroup(gps2)
                .where(e -> 1)
                .equalTo(e -> 1)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
                .apply(new ClosestPairsCoGroupFunction(TOP_K))
                .print();

        logger.info("Job running...");
        env.execute("SedonaQuery7-SNCB, Global Closest Device Pairs Top-K (no MEOS)");
    }
}