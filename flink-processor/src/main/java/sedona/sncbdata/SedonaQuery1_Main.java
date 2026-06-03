package sedona.sncbdata;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.sedona.flink.SedonaContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sncbdata.SNCBData;
import sncbdata.SNCBDataDeserializationSchema;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * SedonaQuery1_Main (SNCB): High-Risk Zone Proximity Monitoring
 * MEOS:
 *   Pointer zone   = functions.geog_in(wkt, -1);
 *   Pointer tpoint = functions.tgeogpoint_in("POINT(lon lat)@ts");
 *   functions.edwithin_tgeo_geo(tpoint, zone, 20.0) == 1
 *
 * Sedona SQL:
 *   ST_Point(lon, lat)                                      → 2D Point
 *   ST_Distance(point, polygon) <= 20.0 / 111_000.0         → distance in degrees
 *   ST_Distance(...) * 111_000.0                            → approx. conversion in meters
 */
public class SedonaQuery1_Main {

    private static final Logger logger = LoggerFactory.getLogger(SedonaQuery1_Main.class);

    /**
     * Threshold in meters.
     * Converted in degrees for ST_Distance: 20 / 111 000 ≈ 0.00018°
     */
    private static final double ALERT_DISTANCE_METERS = 20.0;

    /**
     * INPolygons: high-risk zones
     *
     * Zone 1 - Brussels-South area
     * Zone 2 - Brussels-North / Schaerbeek area
     * Zone 3 - Ghent area
     * Zone 4 - Antwerp area
     */
    private static final String[] HIGH_RISK_ZONES_WKT = {
            "POLYGON((4.3550 50.6350, 4.3550 50.6550, 4.3750 50.6550, 4.3750 50.6350, 4.3550 50.6350))",
            "POLYGON((4.3500 50.8600, 4.3500 50.8800, 4.3700 50.8800, 4.3700 50.8600, 4.3500 50.8600))",
            "POLYGON((3.7100 51.0200, 3.7100 51.0400, 3.7300 51.0400, 3.7300 51.0200, 3.7100 51.0200))",
            "POLYGON((4.4100 51.2900, 4.4100 51.3100, 4.4300 51.3100, 4.4300 51.2900, 4.4100 51.2900))"
    };

    // =========================================================================
    // TimestampAssigner on Row
    //      toDataStream() sets all timestamps at Long.MIN_VALUE as a default value
    //          So we have to access "ts_ms" from the Row and reassign it as event-time.
    // =========================================================================
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

    // =========================================================================
    // ProcessAllWindowFunction
    // =========================================================================
    static class AlertWindowFunction
            extends ProcessWindowFunction<Row, String, Integer, TimeWindow>
            implements Serializable {

        private static final DateTimeFormatter FMT =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssxxx");

        @Override
        public void process(Integer deviceId, Context ctx, Iterable<Row> elements, Collector<String> out) {
            String wStart = millisToTs(ctx.window().getStart());
            String wEnd   = millisToTs(ctx.window().getEnd());

            for (Row row : elements) {
                Double  lon        = row.getFieldAs("lon");
                Double  lat        = row.getFieldAs("lat");
                Double  speed      = row.getFieldAs("gps_speed");
                Double  distMeters = row.getFieldAs("dist_meters");
                Integer zoneIndex  = row.getFieldAs("zone_index");

                String alert = String.format(
                        "[ALERT][Q1-Sedona-SNCB] DeviceID=%-6d | lon=%10.5f lat=%9.5f"
                                + " | speed=%.2f km/h"
                                + " | dist=%.2f m (<=%.0f m) | ZONE %d"
                                + " | window [%s – %s]",
                        deviceId, lon, lat, speed,
                        distMeters, ALERT_DISTANCE_METERS,
                        zoneIndex + 1, wStart, wEnd);

                logger.warn(alert);
                out.collect(alert);
            }
        }

        private String millisToTs(long ms) {
            return Instant.ofEpochMilli(ms).atOffset(ZoneOffset.UTC).format(FMT);
        }
    }

    // =========================================================================
    // MAIN
    // =========================================================================
    public static void main(String[] args) throws Exception {
        logger.info("=== SedonaQuery1_Main (SNCB): starting ===");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().disableClosureCleaner();

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        SedonaContext.create(env, tableEnv);
        logger.info("Sedona initialized");

        // ------------------------------------------------------------------
        // 1. Source Kafka
        KafkaSource<SNCBData> kafkaSource = KafkaSource.<SNCBData>builder()
                .setBootstrapServers("kafka:29092")
                .setGroupId("flink_consumer_sedona_sncb_q1")
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

        // ------------------------------------------------------------------
        // 2. DataStream<SNCBData> → Table Sedona
        //    Mapping SNCBData fields to SQL columns.
        // ------------------------------------------------------------------
        Table sncbTable = tableEnv.fromDataStream(
                source,
                org.apache.flink.table.api.Expressions.$("deviceId").as("device_id"),
                org.apache.flink.table.api.Expressions.$("lon"),
                org.apache.flink.table.api.Expressions.$("lat"),
                org.apache.flink.table.api.Expressions.$("timestamp").as("ts_ms"),
                org.apache.flink.table.api.Expressions.$("gpsSpeed").as("gps_speed")
        );
        tableEnv.createTemporaryView("sncb", sncbTable);

        // ------------------------------------------------------------------
        // 3. SQL Sedona queries: one per zone
        //
        //    MEOS (Query1_Main.java) :
        //      Pointer tpoint = tgeogpoint_in("POINT(lon lat)@ts");
        //      edwithin_tgeo_geo(tpoint, zone, 20.0) == 1
        //
        //    Sedona SQL :
        //      ST_Point(lon, lat)                 → 2D Point (no timestamp)
        //      ST_GeomFromText(wkt)               → Polygon
        //      ST_Distance(pt, poly) <= threshold → filtering
        //      ST_Distance(...) * 111000          → conversion in meters
        // ------------------------------------------------------------------
        final double thresholdDeg = ALERT_DISTANCE_METERS / 111_000.0;

        DataStream<Row> alertStream = null;

        for (int i = 0; i < HIGH_RISK_ZONES_WKT.length; i++) {
            final String zoneWkt = HIGH_RISK_ZONES_WKT[i];
            final int    zoneIdx = i;

            String sql = String.format(
                    "SELECT " +
                            "  device_id, lon, lat, ts_ms, gps_speed, " +
                            "  ST_Distance(ST_Point(lon, lat), ST_GeomFromText('%s')) * 111000.0 AS dist_meters, " +
                            "  %d AS zone_index " +
                            "FROM sncb " +
                            "WHERE ST_Distance(ST_Point(lon, lat), ST_GeomFromText('%s')) <= %f",
                    zoneWkt, zoneIdx, zoneWkt, thresholdDeg
            );

            // FIX: reassigning the timestamps after toDataStream()
            // (going from the Table API to the DataStream API causes the loss of Flink watermarks)
            DataStream<Row> zoneStream = tableEnv
                    .toDataStream(tableEnv.sqlQuery(sql))
                    .assignTimestampsAndWatermarks(
                            WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                    .withTimestampAssigner(new RowTimestampAssigner())
                                    .withIdleness(Duration.ofMinutes(1))
                    );

            alertStream = (alertStream == null)
                    ? zoneStream
                    : alertStream.union(zoneStream);
        }

        // ------------------------------------------------------------------
        // 4. Tumbling Window of 10 secondes
        // ------------------------------------------------------------------
        assert alertStream != null;
        alertStream
                .keyBy(new DeviceIdKeySelector())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new AlertWindowFunction())
                .print();

        logger.info("Job starting...");
        env.execute("SedonaQuery1-SNCB - High-Risk Zone Proximity Monitoring (sans MEOS)");
    }
}