package sedona.aisdata;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.sedona.flink.SedonaContext;
import org.locationtech.jts.geom.CoordinateXYM;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.WKTWriter;
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
import java.util.List;

/**
 * SedonaQuery4_Main (AIS): Trajectory Creation in a Restricted Space
 *
 * MEOS (Query4_Main.java):
 *   - Line 2 : tgeo_at_stbox(tpoint, stbox, true)
 *              --> spatio-temporal native filter in MEOS (STBox: spatial rectangle + temporal span)
 *   - Line 3 : SlidingEventTimeWindows(10s, 10ms)
 *   - Line 4 : temporal_sequence(lon, lat, ts) --> tgeogpoint_in("{POINT(lon lat)@ts,...}")
 *
 * Sedona:
 *   - Line 2 : STBox-like filter recreated in Sedona SQL:
 *              --> spatial filter  : ST_Within(ST_Point(lon, lat), ST_GeomFromText(bbox_wkt))
 *              --> temporal filter : ts_ms BETWEEN stbox_tmin_ms AND stbox_tmax_ms
 *   - Line 3 : SlidingEventTimeWindows(10s, 10ms)
 *   - Line 4 : building a JTS LINESTRING M in the window function, same as in Q3
 *
 * Key difference: MEOS vs Sedona concerning the STBox filter:
 *   MEOS : tgeo_at_stbox operates on tgeogpoint (spatio-temporal paired type)
 *   Sedona SQL : ST_Within operates on 2D points, the temporal filter is separated (BETWEEN)
 *   --> the results should be identical
 */
public class SedonaQuery4_Main {

    private static final Logger logger = LoggerFactory.getLogger(SedonaQuery4_Main.class);

    // Covers the Esbjerg / North Sea area where MMSI 566948000 operates.
    private static final double STBOX_XMIN = 4.48;
    private static final double STBOX_XMAX = 4.64;
    private static final double STBOX_YMIN = 55.55;
    private static final double STBOX_YMAX = 55.66;
    // Complete day for the AIS dataset (epoch ms)
    private static final long STBOX_TMIN_MS = 1610064000000L; // 2021-01-08 00:00:00 UTC
    private static final long STBOX_TMAX_MS = 1610150400000L; // 2021-01-09 00:00:00 UTC

    // WKT representing the spatial rectangle (POLYGON)
    private static final String STBOX_BBOX_WKT = String.format(
            "POLYGON((%f %f, %f %f, %f %f, %f %f, %f %f))",
            STBOX_XMIN, STBOX_YMIN,
            STBOX_XMIN, STBOX_YMAX,
            STBOX_XMAX, STBOX_YMAX,
            STBOX_XMAX, STBOX_YMIN,
            STBOX_XMIN, STBOX_YMIN);

    static class RowTimestampAssigner
            implements SerializableTimestampAssigner<Row>, Serializable {
        @Override
        public long extractTimestamp(Row row, long previousTs) {
            Long ts = row.getFieldAs("ts_ms");
            return (ts != null) ? ts : previousTs;
        }
    }

    static class MmsiKeySelector implements KeySelector<Row, Integer>, Serializable {
        @Override
        public Integer getKey(Row row) {
            return row.getFieldAs("mmsi");
        }
    }

    // The STBox filter has already been applied by Sedona SQL.
    // Here we're only receiving the records validated by the spatio-temporal filter.
    // We build the trajectory using JTS (same approach as in Q3).
    static class RestrictedTrajectoryWindowFunction
            extends ProcessWindowFunction<Row, String, Integer, TimeWindow>
            implements Serializable {

        private static final DateTimeFormatter FMT =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssxxx");

        private static final GeometryFactory FACTORY = new GeometryFactory();

        @Override
        public void process(
                Integer mmsi,
                Context ctx,
                Iterable<Row> elements,
                Collector<String> out) {

            String wStart = millisToTs(ctx.window().getStart());
            String wEnd   = millisToTs(ctx.window().getEnd());

            // Sort by ts_ms
            List<Row> sorted = new ArrayList<>();
            for (Row row : elements) sorted.add(row);
            sorted.sort((a, b) -> {
                Long ta = a.getFieldAs("ts_ms");
                Long tb = b.getFieldAs("ts_ms");
                if (ta == null) return -1;
                if (tb == null) return 1;
                return Long.compare(ta, tb);
            });

            if (sorted.isEmpty()) return;

            // Build the JTS coordinates with dimension M = timestamp
            // CoordinateXYM(lon, lat, epoch_ms) ~= "POINT(lon lat)@ts" in MEOS
            CoordinateXYM[] coords = new CoordinateXYM[sorted.size()];
            for (int i = 0; i < sorted.size(); i++) {
                Row    row = sorted.get(i);
                Double lon = row.getFieldAs("lon");
                Double lat = row.getFieldAs("lat");
                Long   ts  = row.getFieldAs("ts_ms");
                coords[i] = new CoordinateXYM(lon, lat, ts != null ? ts : 0L);
            }

            // Build the JTS trajectory
            // WKTWriter(3) outputs: "LINESTRING M (lon lat ts, ...)"
            WKTWriter writer = new WKTWriter(3);
            String geometryWkt;

            if (sorted.size() == 1) {
                Point point = FACTORY.createPoint(coords[0]);
                geometryWkt = writer.write(point);
            } else {
                LineString trajectory = FACTORY.createLineString(coords);
                geometryWkt = writer.write(trajectory);
            }

            String output = String.format(
                    "[TRAJ][Q4-Sedona-AIS] MMSI=%-12d | points=%3d | window [%s – %s]%n"
                            + "    geometry : %s",
                    mmsi,
                    sorted.size(),
                    wStart, wEnd,
                    geometryWkt);

            logger.info(output);
            out.collect(output);
        }

        private String millisToTs(long ms) {
            return Instant.ofEpochMilli(ms).atOffset(ZoneOffset.UTC).format(FMT);
        }
    }

    // =========================================================================
    // MAIN
    // =========================================================================
    public static void main(String[] args) throws Exception {
        logger.info("=== SedonaQuery4_Main (AIS), starting (no MEOS) ===");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().disableClosureCleaner(); // Fix 1

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        SedonaContext.create(env, tableEnv);
        logger.info("Sedona initialisé");

        KafkaSource<AISData> kafkaSource = KafkaSource.<AISData>builder()
                .setBootstrapServers("kafka:29092")
                .setGroupId("flink_consumer_sedona_ais_q4")
                .setTopics("aisdata")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new AISDataDeserializationSchema())
                .build();

        WatermarkStrategy<AISData> aisWatermark =
                WatermarkStrategy.<AISData>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner((event, ts) -> event.getTimestamp())
                        .withIdleness(Duration.ofMinutes(1));

        DataStream<AISData> source = env.fromSource(
                kafkaSource, aisWatermark, "Kafka AIS Source");

        Table aisTable = tableEnv.fromDataStream(
                source,
                org.apache.flink.table.api.Expressions.$("mmsi"),
                org.apache.flink.table.api.Expressions.$("lon"),
                org.apache.flink.table.api.Expressions.$("lat"),
                org.apache.flink.table.api.Expressions.$("timestamp").as("ts_ms")
        );
        tableEnv.createTemporaryView("ais", aisTable);

        // MEOS : tgeo_at_stbox(tpoint, stbox, true)
        //
        // Sedona SQL : decomposed in 2 independent filters:
        //   --> spatial filter: ST_Within(ST_Point(lon, lat), ST_GeomFromText(bbox_wkt))
        //                       --> verifies whether the point is in the rectangle or not
        //   --> filtre temporel : ts_ms BETWEEN stbox_tmin_ms AND stbox_tmax_ms
        //                       --> verifies whether the timestamp is included in the interval
        String sql = String.format(
                "SELECT mmsi, lon, lat, ts_ms FROM ais " +
                        "WHERE ST_Within(ST_Point(lon, lat), ST_GeomFromText('%s')) " +
                        "  AND ts_ms BETWEEN %d AND %d",
                STBOX_BBOX_WKT, STBOX_TMIN_MS, STBOX_TMAX_MS);

        logger.info("SQL STBox filter : {}", sql);

        DataStream<Row> filteredStream = tableEnv
                .toDataStream(tableEnv.sqlQuery(sql))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner(new RowTimestampAssigner())
                                .withIdleness(Duration.ofMinutes(1))
                );

        // keyBy(mmsi) + SlidingWindow(10s, 10ms)
        filteredStream
                .keyBy(new MmsiKeySelector())
                .window(SlidingEventTimeWindows.of(Duration.ofSeconds(10), Duration.ofMillis(10)))
                .process(new RestrictedTrajectoryWindowFunction())
                .print();

        logger.info("Job starting...");
        env.execute("SedonaQuery4-AIS, Trajectory Creation in a Restricted Space (no MEOS)");
    }
}