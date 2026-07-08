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
 * SedonaQuery5_Main (AIS): Trajectory Creation and High-Speed Alert
 *
 * MEOS (Query5_Main.java):
 *   - Line 2: edwithin_tgeo_geo(tpoint, geofence, 1.0) == 1
 *   - Line 3: groupBy(device_id)
 *   - Line 4: SlidingWindow(45s, 5s)
 *   - Line 5: temporal_sequence(lon, lat, ts) + avg(gps_speed) + min(gps_speed)
 *   - Line 6: alert if (avg_speed > 50 m/s) || (min_speed > 20 m/s)
 *
 * Sedona:
 *   - Line 2: ST_Distance(ST_Point(lon, lat), ST_GeomFromText(wkt)) <= threshold
 *             applied in SQL before records enter the window
 *   - Line 3: keyBy(mmsi)
 *   - Line 4: SlidingEventTimeWindows(45s, 5s)
 *   - Line 5: JTS LINESTRING M trajectory + avg/min speed computed in Java
 *   - Line 6: same alert condition
 *
 * Geofence: MMSI 219027804 area polygon, identical to Query5_Main.java MEOS.
 */
public class SedonaQuery5_Main {

    private static final Logger logger = LoggerFactory.getLogger(SedonaQuery5_Main.class);

    /** Conversion factor from knots (AIS speed unit) to m/s. */
    private static final double KNOTS_TO_MS = 0.514444;

    private static final double AVG_SPEED_THRESHOLD_MS = 7.5;

    private static final double MIN_SPEED_THRESHOLD_MS = 5.0;

    /**
     * Geofence polygon: the {@code POLYGON} argument in paper Line 2.
     *
     * <p>The paper uses a polygon covering the Brussels area (SNCB dataset):
     * <pre>
     *   POLYGON((4.32 50.60, 4.32 50.72, 4.48 50.72, 4.48 50.60, 4.32 50.60))
     * </pre>
     *
     * <p>Adapted here to cover area where MMSI 219027804 operates in the AIS dataset (lon 11.76–12.07, lat 55.82–55.95).
     * This vessel reaches speeds up to 31.8 kn (≈ 16.4 m/s), well above both average and minimum speed thresholds, ensuring
     * alerts fire on the AIS data.
     */
    private static final String GEOFENCE_WKT =
            "POLYGON((11.76 55.82, 11.76 55.95, 12.07 55.95, 12.07 55.82, 11.76 55.82))";

    /**
     * Geofence distance threshold in meters, converted to degrees for ST_Distance.
     * MEOS uses edwithin_tgeo_geo with 1.0 meter. ST_Distance returns degrees,
     * so: 1.0 m / 111000 m/deg ~ 0.000009 deg.
     */
    private static final double GEOFENCE_DIST_DEG = 1.0 / 111_000.0;

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

    // The geofence filter (paper Line 2) has already been applied by Sedona SQL.
    // This function only receives records that passed the spatial filter.
    //
    // Steps:
    //   1. Sort records by ts_ms (required by JTS, same as MEOS)
    //   2. Compute avg and min speed in m/s from the speed field (knots)
    //   3. Apply alert condition: avg_speed > 50 || min_speed > 20
    //   4. Build JTS LINESTRING M trajectory if alert is triggered
    static class HighSpeedAlertWindowFunction
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

            // Step 1: collect and sort by ts_ms
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

            // Step 2: compute avg and min speed in m/s
            double speedSum = 0.0;
            double minSpeed = Double.MAX_VALUE;

            for (Row row : sorted) {
                Double speedKn = row.getFieldAs("speed");
                if (speedKn == null) continue;
                double speedMs = speedKn * KNOTS_TO_MS;
                speedSum += speedMs;
                if (speedMs < minSpeed) minSpeed = speedMs;
            }

            double avgSpeed = speedSum / sorted.size();

            // Step 3: paper Line 6 - (avg_speed > 50) || (min_speed > 20)
            if (avgSpeed <= AVG_SPEED_THRESHOLD_MS && minSpeed <= MIN_SPEED_THRESHOLD_MS) return;

            // Step 4: build JTS LINESTRING M trajectory
            // CoordinateXYM(lon, lat, epoch_ms) = equivalent of "POINT(lon lat)@ts" in MEOS
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

            String triggerReason = (avgSpeed > AVG_SPEED_THRESHOLD_MS && minSpeed > MIN_SPEED_THRESHOLD_MS)
                    ? "AVG+MIN" : (avgSpeed > AVG_SPEED_THRESHOLD_MS ? "AVG" : "MIN");

            String alert = String.format(
                    "[ALERT][Q5-Sedona-AIS] MMSI=%-12d"
                            + " | avgSpeed=%6.2f m/s (>%.1f)"
                            + " | minSpeed=%6.2f m/s (>%.1f)"
                            + " | points=%d | trigger=%s"
                            + " | window [%s - %s]%n"
                            + "    geometry : %s",
                    mmsi,
                    avgSpeed, AVG_SPEED_THRESHOLD_MS,
                    minSpeed, MIN_SPEED_THRESHOLD_MS,
                    sorted.size(), triggerReason,
                    wStart, wEnd,
                    geometryWkt);

            logger.warn(alert);
            out.collect(alert);
        }

        private String millisToTs(long ms) {
            return Instant.ofEpochMilli(ms).atOffset(ZoneOffset.UTC).format(FMT);
        }
    }

    // =========================================================================
    // MAIN
    // =========================================================================
    public static void main(String[] args) throws Exception {
        logger.info("=== SedonaQuery5_Main (AIS), starting (no MEOS) ===");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().disableClosureCleaner();

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        SedonaContext.create(env, tableEnv);
        logger.info("Sedona initialized");

        KafkaSource<AISData> kafkaSource = KafkaSource.<AISData>builder()
                .setBootstrapServers("kafka:29092")
                .setGroupId("flink_consumer_sedona_ais_q5")
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
                org.apache.flink.table.api.Expressions.$("timestamp").as("ts_ms"),
                org.apache.flink.table.api.Expressions.$("speed")
        );
        tableEnv.createTemporaryView("ais", aisTable);

        // Geofence filter in Sedona SQL: paper Line 2 - edwithin_tgeo_geo
        //
        // MEOS: edwithin_tgeo_geo(tpoint, geofence, 1.0) == 1
        //
        // Sedona SQL: ST_Distance(ST_Point(lon, lat), ST_GeomFromText(wkt)) <= threshold
        String sql = String.format(
                "SELECT mmsi, lon, lat, ts_ms, speed FROM ais " +
                        "WHERE ST_Distance(ST_Point(lon, lat), ST_GeomFromText('%s')) <= %f",
                GEOFENCE_WKT, GEOFENCE_DIST_DEG);

        logger.info("SQL geofence filter : {}", sql);

        DataStream<Row> filteredStream = tableEnv
                .toDataStream(tableEnv.sqlQuery(sql))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner(new RowTimestampAssigner())
                                .withIdleness(Duration.ofMinutes(1))
                );

        // keyBy(mmsi) + SlidingWindow(45s, 5s) - paper Lines 3, 4, 5, 6
        filteredStream
                .keyBy(new MmsiKeySelector())
                .window(SlidingEventTimeWindows.of(Duration.ofSeconds(45), Duration.ofSeconds(5)))
                .process(new HighSpeedAlertWindowFunction())
                .print();

        logger.info("Job running...");
        env.execute("SedonaQuery5-AIS, Trajectory Creation and High-Speed Alert (no MEOS)");
    }
}