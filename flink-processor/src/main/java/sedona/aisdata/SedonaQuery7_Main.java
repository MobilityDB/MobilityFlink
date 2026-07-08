package sedona.aisdata;

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

import aisdata.AISData;
import aisdata.AISDataDeserializationSchema;

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

/**
 * SedonaQuery7_Main (AIS): Global Closest Device Pairs (Top-k)
 *
 * MEOS (Query7_Main.java):
 *   - Line 2: coGroup on constant key, filter mmsi1 < mmsi2
 *   - Line 3: TumblingEventTimeWindows(10s)
 *   - Line 4: geog_distance(tgeo_end_value(tp1), tgeo_end_value(tp2))
 *   - Lines 5-6: sort by mindist ascending, keep top-K pairs
 *
 * Sedona/JTS:
 *   - Line 2: identical
 *   - Line 3: identical
 *   - Line 4: JTS point.distance(point) * 111000
 *   - Lines 5-6: identical, sort + top-K
 *
 * Why no Table API here:
 *   Q7 is a cross-product across all devices within a window. This requires
 *   collecting all events from both streams into memory within the coGroup function.
 *   StreamTableEnvironment is not serializable and cannot be used inside a worker.
 */
public class SedonaQuery7_Main {

    private static final Logger logger = LoggerFactory.getLogger(SedonaQuery7_Main.class);

    private static final int TOP_K = 10;

    static class ClosestPairsCoGroupFunction
            extends RichCoGroupFunction<AISData, AISData, String>
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
        public void coGroup(Iterable<AISData> leftEvents, Iterable<AISData> rightEvents,
                            Collector<String> out) throws Exception {

            List<AISData> lefts  = new ArrayList<>();
            List<AISData> rights = new ArrayList<>();
            for (AISData e : leftEvents)  lefts.add(e);
            for (AISData e : rightEvents) rights.add(e);
            if (lefts.isEmpty() || rights.isEmpty()) return;

            // Pre-compute JTS Points for every event before the double loop.
            List<Point> pointLefts = new ArrayList<>(lefts.size());
            for (AISData left : lefts) {
                pointLefts.add(factory.createPoint(new Coordinate(left.getLon(), left.getLat())));
            }

            List<Point> pointRights = new ArrayList<>(rights.size());
            for (AISData right : rights) {
                pointRights.add(factory.createPoint(new Coordinate(right.getLon(), right.getLat())));
            }

            // Cross-product with mmsi1 < mmsi2 + min-distance per pair.
            // pairMap key: "mmsi1:mmsi2", value: [mmsi1, mmsi2, dist, lon1, lat1, lon2, lat2]
            Map<String, double[]> pairMap = new HashMap<>();

            int leftSize  = lefts.size();
            int rightSize = rights.size();

            for (int i = 0; i < leftSize; i++) {
                AISData left      = lefts.get(i);
                Point   ptLeft    = pointLefts.get(i);

                for (int j = 0; j < rightSize; j++) {
                    AISData right  = rights.get(j);
                    Point   ptRight = pointRights.get(j);

                    // mmsi1 < mmsi2 - avoids duplicate pairs
                    if (left.getMmsi() >= right.getMmsi()) continue;

                    // JTS distance() returns degrees, * 111000 gives approx. meters
                    double dist = ptLeft.distance(ptRight) * 111_000.0;

                    String key = left.getMmsi() + ":" + right.getMmsi();
                    if (!pairMap.containsKey(key) || dist < pairMap.get(key)[2]) {
                        pairMap.put(key, new double[]{
                                left.getMmsi(), right.getMmsi(), dist,
                                left.getLon(),  left.getLat(),
                                right.getLon(), right.getLat()});
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
                        "[TOPK][Q7-Sedona-AIS] rank=%2d/%d"
                                + " | MMSI1=%-12d (lon=%10.5f lat=%9.5f)"
                                + " | MMSI2=%-12d (lon=%10.5f lat=%9.5f)"
                                + " | mindist=%10.3f m",
                        rank + 1, emitCount,
                        (long) p[0], p[3], p[4],
                        (long) p[1], p[5], p[6],
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
        logger.info("=== SedonaQuery7_Main (AIS), starting (no MEOS) ===");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        WatermarkStrategy<AISData> watermarkStrategy =
                WatermarkStrategy.<AISData>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner((event, ts) -> event.getTimestamp())
                        .withIdleness(Duration.ofMinutes(1));

        KafkaSource<AISData> kafkaSourceLeft = KafkaSource.<AISData>builder()
                .setBootstrapServers("kafka:29092")
                .setGroupId("flink_consumer_sedona_ais_q7_left")
                .setTopics("aisdata")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new AISDataDeserializationSchema())
                .build();

        KafkaSource<AISData> kafkaSourceRight = KafkaSource.<AISData>builder()
                .setBootstrapServers("kafka:29092")
                .setGroupId("flink_consumer_sedona_ais_q7_right")
                .setTopics("aisdata")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new AISDataDeserializationSchema())
                .build();

        DataStream<AISData> gps  = env.fromSource(kafkaSourceLeft,  watermarkStrategy, "Kafka GPS");
        DataStream<AISData> gps2 = env.fromSource(kafkaSourceRight, watermarkStrategy, "Kafka GPS2");

        // Constant key: all events routed to the same partition for cross-device pairing
        // coGroup instead of join: we need both iterables simultaneously to build the cross-product
        gps.coGroup(gps2)
                .where(e -> 1)
                .equalTo(e -> 1)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
                .apply(new ClosestPairsCoGroupFunction(TOP_K))
                .print();

        logger.info("Job running...");
        env.execute("SedonaQuery7-AIS, Global Closest Device Pairs Top-K (no MEOS)");
    }
}