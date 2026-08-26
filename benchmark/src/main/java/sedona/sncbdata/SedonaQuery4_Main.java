package sedona.sncbdata;

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

import sncbdata.SNCBData;
import sncbdata.SNCBDataDeserializationSchema;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class SedonaQuery4_Main {

    private static final Logger logger = LoggerFactory.getLogger(SedonaQuery4_Main.class);

    // Brussels-South where the device 3 operates (identical to Query4_Main.java MEOS)
    private static final double STBOX_XMIN = 4.35;
    private static final double STBOX_XMAX = 4.40;
    private static final double STBOX_YMIN = 50.63;
    private static final double STBOX_YMAX = 50.66;
    private static final long STBOX_TMIN_MS = 1722470400000L; // 2024-08-01 00:00:00 UTC
    private static final long STBOX_TMAX_MS = 1722556800000L; // 2024-08-02 00:00:00 UTC

    // WKT of the spatial rectangle (POLYGON of 5 points)
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

    static class DeviceIdKeySelector implements KeySelector<Row, Integer>, Serializable {
        @Override
        public Integer getKey(Row row) {
            return row.getFieldAs("device_id");
        }
    }

    // MEOS V1 --> tgeogpoint_in("{POINT(lon lat)@ts,...}") --> tgeogpoint TSequence
    // MEOS V2 --> tsequence_make + temporal_append_tinstant
    // JTS     --> FACTORY.createLineString(CoordinateXYM[]) --> LINESTRING M
    static class RestrictedTrajectoryWindowFunction
            extends ProcessWindowFunction<Row, String, Integer, TimeWindow>
            implements Serializable {

        private static final DateTimeFormatter FMT =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssxxx");

        private static final GeometryFactory FACTORY = new GeometryFactory();

        @Override
        public void process(
                Integer deviceId,
                Context ctx,
                Iterable<Row> elements,
                Collector<String> out) {

            String wStart = millisToTs(ctx.window().getStart());
            String wEnd   = millisToTs(ctx.window().getEnd());

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

            // CoordinateXYM(lon, lat, epoch_ms) ~ "POINT(lon lat)@ts" en MEOS
            CoordinateXYM[] coords = new CoordinateXYM[sorted.size()];
            for (int i = 0; i < sorted.size(); i++) {
                Row    row = sorted.get(i);
                Double lon = row.getFieldAs("lon");
                Double lat = row.getFieldAs("lat");
                Long   ts  = row.getFieldAs("ts_ms");
                coords[i] = new CoordinateXYM(lon, lat, ts != null ? ts : 0L);
            }

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
                    "[TRAJ][Q4-Sedona-SNCB] DeviceID=%-6d | points=%3d | window [%s – %s]%n"
                            + "    geometry : %s",
                    deviceId,
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
        logger.info("=== SedonaQuery4_Main (SNCB), starting (no MEOS) ===");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().disableClosureCleaner(); // Fix 1

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        SedonaContext.create(env, tableEnv);
        logger.info("Sedona initialisé");

        KafkaSource<SNCBData> kafkaSource = KafkaSource.<SNCBData>builder()
                .setBootstrapServers("kafka:29092")
                .setGroupId("flink_consumer_sedona_sncb_q4")
                .setTopics("sncbdata")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SNCBDataDeserializationSchema())
                .build();

        WatermarkStrategy<SNCBData> sncbWatermark =
                WatermarkStrategy.<SNCBData>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner((event, ts) -> event.getTimestamp())
                        .withIdleness(Duration.ofMinutes(1));

        DataStream<SNCBData> source = env.fromSource(
                kafkaSource, sncbWatermark, "Kafka SNCB Source");

        Table sncbTable = tableEnv.fromDataStream(
                source,
                org.apache.flink.table.api.Expressions.$("deviceId").as("device_id"),
                org.apache.flink.table.api.Expressions.$("lon"),
                org.apache.flink.table.api.Expressions.$("lat"),
                org.apache.flink.table.api.Expressions.$("timestamp").as("ts_ms")
        );
        tableEnv.createTemporaryView("sncb", sncbTable);

        String sql = String.format(
                "SELECT device_id, lon, lat, ts_ms FROM sncb " +
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

        // keyBy(device_id) + SlidingWindow(10s, 10ms)
        filteredStream
                .keyBy(new DeviceIdKeySelector())
                .window(SlidingEventTimeWindows.of(Duration.ofSeconds(10), Duration.ofMillis(10)))
                .process(new RestrictedTrajectoryWindowFunction())
                .print();

        logger.info("Job starting...");
        env.execute("SedonaQuery4-SNCB, Trajectory Creation in a Restricted Space (sans MEOS)");
    }
}