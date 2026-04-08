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
 * SedonaQuery2_Main (AIS): Brake System Monitoring
 *
 * MEOS (Query2_Main.java) :
 *   - Line 2 : eintersects_tgeo_geo(tpoint, maintenanceZone) == 0 --> exclude points in a maintenance zone
 *   - Line 3 : SlidingEventTimeWindows(10s, 10ms)
 *   - Line 4 : variation(FA), variation(FF) --> variance computation
 *   - Line 5 : varFA > 0.6 && varFF <= 0.5 --> alert
 *
 * Sedona SQL :
 *   - Line 2 : NOT ST_Intersects(ST_Point(lon, lat), ST_GeomFromText(wkt))
 *              → direct SQL filter, an AND clause per maintenance zone
 *   - Line 3 : SlidingEventTimeWindows(10s, 10ms) --> similar, native Flink
 *   - Line 4 : variance computation --> similar
 *   - Line 5 : varFA > 0.6 && varFF <= 0.5 --> similar
 */
public class SedonaQuery2_Main {

    private static final Logger logger = LoggerFactory.getLogger(SedonaQuery2_Main.class);

    /** FA variance threshold: varFA > 0.6 --> potentially an anomaly */
    private static final double VAR_FA_THRESHOLD = 0.6;

    /** FF variance threshold: varFF <= 0.5 --> potentially an anomaly */
    private static final double VAR_FF_THRESHOLD = 0.5;

    private static final double MAX_SPEED_KNOTS = 25.0;

    /**
     * Maintenance zones to exclude.
     */
    private static final String[] MAINTENANCE_AREAS_WKT = {
            // Maintenance area A - open North Sea (no vessel traffic)
            "POLYGON((3.0000 56.0000, 3.0000 56.2000, 3.2000 56.2000, 3.2000 56.0000, 3.0000 56.0000))",
            // Maintenance area B - open Baltic (no vessel traffic)
            "POLYGON((15.0000 57.0000, 15.0000 57.2000, 15.2000 57.2000, 15.2000 57.0000, 15.0000 57.0000))"
    };

    static class RowTimestampAssigner
            implements SerializableTimestampAssigner<Row>, Serializable {
        @Override
        public long extractTimestamp(Row row, long previousTs) {
            Long ts = row.getFieldAs("ts_ms");
            return (ts != null) ? ts : previousTs;
        }
    }

    // KeySelector : extract the mmsi from a Row for the keyBy
    static class MmsiKeySelector implements KeySelector<Row, Integer>, Serializable {
        @Override
        public Integer getKey(Row row) {
            return row.getFieldAs("mmsi");
        }
    }

    // Receives all Row of a Window (10s) for a given mmsi.
    // Computes varFA and varFF & emits an alert if the threshold are crossed
    static class BrakeMonitoringWindowFunction
            extends ProcessWindowFunction<Row, String, Integer, TimeWindow>
            implements Serializable {

        private static final DateTimeFormatter FMT =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssxxx");

        @Override
        public void process(
                Integer mmsi,
                Context ctx,
                Iterable<Row> elements,
                Collector<String> out) {

            String wStart = millisToTs(ctx.window().getStart());
            String wEnd   = millisToTs(ctx.window().getEnd());

            List<Double> faValues = new ArrayList<>();
            List<Double> ffValues = new ArrayList<>();

            for (Row row : elements) {
                Double speed  = row.getFieldAs("speed");
                Double course = row.getFieldAs("course");
                if (speed == null || course == null) continue;

                // FA proxy : speed (nodes) normalized --> [0, 7] bar
                double fa = (speed / MAX_SPEED_KNOTS) * 7.0;

                // FF proxy : course (degrees [0, 360]) normalized → [0, 7] bar
                double ff = (course / 360.0) * 7.0;

                faValues.add(fa);
                ffValues.add(ff);
            }

            if (faValues.isEmpty()) return;

            double varFA = variance(faValues);
            double varFF = variance(ffValues);

            if (varFA > VAR_FA_THRESHOLD && varFF <= VAR_FF_THRESHOLD) {
                String alert = String.format(
                        "[ALERT][Q2-Sedona-AIS] MMSI=%-12d"
                                + " | varFA=%6.4f bar² (>%.1f)"
                                + " | varFF=%6.4f bar² (<=%.1f)"
                                + " | events=%d"
                                + " | window [%s – %s]",
                        mmsi,
                        varFA, VAR_FA_THRESHOLD,
                        varFF, VAR_FF_THRESHOLD,
                        faValues.size(),
                        wStart, wEnd);

                logger.warn(alert);
                out.collect(alert);
            }
        }

        private String millisToTs(long ms) {
            return Instant.ofEpochMilli(ms).atOffset(ZoneOffset.UTC).format(FMT);
        }
    }

    // variance: computes the variance of a list of values
    //
    // Formula : Σ[(val - mean)²] / N
    static double variance(List<Double> values) {
        if (values.size() < 2) return 0.0;
        double mean = values.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
        double sumSq = values.stream().mapToDouble(v -> (v - mean) * (v - mean)).sum();
        return sumSq / values.size();
    }

    // =========================================================================
    // MAIN
    // =========================================================================
    public static void main(String[] args) throws Exception {
        logger.info("=== SedonaQuery2_Main (AIS) - starting (no MEOS) ===");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().disableClosureCleaner();

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        SedonaContext.create(env, tableEnv);
        logger.info("Sedona initialized");

        KafkaSource<AISData> kafkaSource = KafkaSource.<AISData>builder()
                .setBootstrapServers("kafka:29092")
                .setGroupId("flink_consumer_sedona_ais_q2")
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
                org.apache.flink.table.api.Expressions.$("speed"),
                org.apache.flink.table.api.Expressions.$("course")
        );
        tableEnv.createTemporaryView("ais", aisTable);

        // SQL Sedona: filter to exclude maintenance zones
        //
        //    MEOS (Query2_Main.java) :
        //      eintersects_tgeo_geo(tpoint, maintenanceZone) == 0
        //
        //    Sedona SQL:
        //      NOT ST_Intersects(ST_Point(lon, lat), ST_GeomFromText(wkt))
        StringBuilder whereClause = new StringBuilder("WHERE 1=1");
        for (String wkt : MAINTENANCE_AREAS_WKT) {
            whereClause.append(String.format(
                    " AND NOT ST_Intersects(ST_Point(lon, lat), ST_GeomFromText('%s'))", wkt));
        }

        String sql = "SELECT mmsi, lon, lat, ts_ms, speed, course FROM ais " + whereClause;
        logger.info("SQL filtre maintenance : {}", sql);

        // Going from the Table API to the DataStream API set Long.MIN_VALUE as the timestamp for each record
        // We "re-read" ts_ms from the Row to rebuild the event-time timestamp.
        DataStream<Row> filteredStream = tableEnv
                .toDataStream(tableEnv.sqlQuery(sql))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner(new RowTimestampAssigner())
                                .withIdleness(Duration.ofMinutes(1))
                );

        filteredStream
                .keyBy(new MmsiKeySelector())
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.milliseconds(10)))
                .process(new BrakeMonitoringWindowFunction())
                .print();

        logger.info("Job starting...");
        env.execute("SedonaQuery2-AIS - Brake System Monitoring (sans MEOS)");
    }
}