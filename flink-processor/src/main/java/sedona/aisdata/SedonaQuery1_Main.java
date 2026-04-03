package sedona.aisdata;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
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

import aisdata.AISData;
import aisdata.AISDataDeserializationSchema;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * SedonaQuery1_Main (AIS): High-Risk Zone Proximity Monitoring
 *
 * FIX appliqué : lorsqu'on passe d'un DataStream à une Table via la Table API,
 * puis qu'on repasse en DataStream avec toDataStream(), les timestamps et
 * watermarks sont PERDUS. Flink remet Long.MIN_VALUE sur chaque record.
 * La fenêtre TumblingEventTimeWindows crashe alors immédiatement.
 *
 * SOLUTION : réassigner les timestamps + watermarks après chaque toDataStream()
 * en lisant la colonne "ts_ms" du Row produit par la requête SQL.
 */
public class SedonaQuery1_Main {

    private static final Logger logger = LoggerFactory.getLogger(SedonaQuery1_Main.class);

    private static final double ALERT_DISTANCE_METERS = 2000.0;

    private static final String[] HIGH_RISK_ZONES_WKT = {
            // ZONE 1
            "POLYGON((12.2524 57.0390, 12.2524 57.0790, 12.2924 57.0790, 12.2924 57.0390, 12.2524 57.0390))",
            // ZONE 2
            "POLYGON((9.9555 57.5720, 9.9555 57.6120, 9.9955 57.6120, 9.9955 57.5720, 9.9555 57.5720))",
            // ZONE 3
            "POLYGON((11.9900 55.9200, 11.9900 55.9600, 12.0800 55.9600, 12.0800 55.9200, 11.9900 55.9200))",
            // ZONE 4
            "POLYGON((4.4800 55.5500, 4.4800 55.6600, 4.6400 55.6600, 4.6400 55.5500, 4.4800 55.5500))"
    };

    // =========================================================================
    // FIX: TimestampAssigner sur Row
    //
    // POURQUOI : tableEnv.toDataStream() remet tous les timestamps à Long.MIN_VALUE.
    // On doit relire "ts_ms" depuis le Row et le réassigner comme event-time.
    // Classe statique + Serializable pour éviter ClosureCleaner + Java 21.
    // =========================================================================
    static class RowTimestampAssigner
            implements SerializableTimestampAssigner<Row>, Serializable {
        @Override
        public long extractTimestamp(Row row, long previousTs) {
            Long ts = row.getFieldAs("ts_ms");
            return (ts != null) ? ts : previousTs;
        }
    }

    // =========================================================================
    // ProcessAllWindowFunction: formate les alertes par fenêtre de 10 secondes
    // =========================================================================
    static class AlertWindowFunction
            extends ProcessAllWindowFunction<Row, String, TimeWindow>
            implements Serializable {

        private static final DateTimeFormatter FMT =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssxxx");

        @Override
        public void process(Context ctx, Iterable<Row> elements, Collector<String> out) {
            String wStart = millisToTs(ctx.window().getStart());
            String wEnd   = millisToTs(ctx.window().getEnd());

            for (Row row : elements) {
                Integer mmsi       = row.getFieldAs("mmsi");
                Double  lon        = row.getFieldAs("lon");
                Double  lat        = row.getFieldAs("lat");
                Double  speed      = row.getFieldAs("speed");
                Double  course     = row.getFieldAs("course");
                Double  distMeters = row.getFieldAs("dist_meters");
                Integer zoneIndex  = row.getFieldAs("zone_index");

                String alert = String.format(
                        "[ALERT][Q1-Sedona-AIS] MMSI=%-12d | lon=%10.5f lat=%9.5f"
                                + " | speed=%.1f kn | course=%.1f°"
                                + " | dist=%.1f m (<=%.0f m) | ZONE %d"
                                + " | window [%s – %s]",
                        mmsi, lon, lat, speed, course,
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
        logger.info("=== SedonaQuery1_Main (AIS) — démarrage (sans MEOS) ===");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().disableClosureCleaner();

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        SedonaContext.create(env, tableEnv);
        logger.info("Sedona initialisé");

        // ------------------------------------------------------------------
        // 1. Source Kafka avec timestamps assignés
        // ------------------------------------------------------------------
        KafkaSource<AISData> kafkaSource = KafkaSource.<AISData>builder()
                .setBootstrapServers("kafka:29092")
                .setGroupId("flink_consumer_sedona_ais_q1")
                .setTopics("aisdata")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new AISDataDeserializationSchema())
                .build();

        WatermarkStrategy<AISData> aisWatermark =
                WatermarkStrategy.<AISData>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                        .withTimestampAssigner((event, ts) -> event.getTimestamp())
                        .withIdleness(Duration.ofMinutes(2));

        DataStream<AISData> source = env.fromSource(
                kafkaSource, aisWatermark, "Kafka AIS Source");

        // ------------------------------------------------------------------
        // 2. DataStream<AISData> --> Table
        // ------------------------------------------------------------------
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

        // ------------------------------------------------------------------
        // 3. Requêtes SQL Sedona: une par zone
        //
        //    On NE sélectionne PAS de colonne Geometry (ST_Point) dans le SELECT
        //    final pour éviter les problèmes de sérialisation Flink <--> Geometry.
        //    On utilise uniquement les résultats scalaires (dist_meters, booléens).
        //
        //    ST_Distance retourne des degrés (plan euclidien).
        //    Conversion : 2000 m ÷ 111 000 m/° ≈ 0.018°
        // ------------------------------------------------------------------
        final double thresholdDeg = ALERT_DISTANCE_METERS / 111_000.0;

        DataStream<Row> alertStream = null;

        for (int i = 0; i < HIGH_RISK_ZONES_WKT.length; i++) {
            final String zoneWkt = HIGH_RISK_ZONES_WKT[i];
            final int    zoneIdx = i;

            String sql = String.format(
                    "SELECT " +
                            "  mmsi, lon, lat, ts_ms, speed, course, " +
                            // ST_Distance retourne des degrés — on multiplie par 111 000 pour des mètres approx.
                            "  ST_Distance(ST_Point(lon, lat), ST_GeomFromText('%s')) * 111000.0 AS dist_meters, " +
                            "  %d AS zone_index " +
                            "FROM ais " +
                            "WHERE ST_Distance(ST_Point(lon, lat), ST_GeomFromText('%s')) <= %f",
                    zoneWkt, zoneIdx, zoneWkt, thresholdDeg
            );

            Table zoneTable = tableEnv.sqlQuery(sql);

            // ------------------------------------------------------------------
            // FIX: réassigner les timestamps après toDataStream()
            //
            // POURQUOI toDataStream() perd les timestamps :
            //   Le passage DataStream → Table → DataStream traverse la "boundary"
            //   entre l'API DataStream et la Table API. Flink ne propage pas
            //   automatiquement les event-time watermarks à travers cette frontière.
            //   Le Row résultant a Long.MIN_VALUE comme timestamp interne, ce qui
            //   fait crasher TumblingEventTimeWindows avec :
            //   "Record has Long.MIN_VALUE timestamp (= no timestamp marker)"
            //
            // SOLUTION : lire "ts_ms" depuis le Row et appeler
            //   assignTimestampsAndWatermarks() sur le DataStream<Row>.
            // ------------------------------------------------------------------
            DataStream<Row> zoneStream = tableEnv
                    .toDataStream(zoneTable)
                    .assignTimestampsAndWatermarks(
                            WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                                    .withTimestampAssigner(new RowTimestampAssigner())
                                    .withIdleness(Duration.ofMinutes(2))
                    );

            alertStream = (alertStream == null)
                    ? zoneStream
                    : alertStream.union(zoneStream);
        }

        // ------------------------------------------------------------------
        // 4. Fenêtre tumbling 10 secondes --> alertes
        // ------------------------------------------------------------------
        assert alertStream != null;
        alertStream
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new AlertWindowFunction())
                .print();

        logger.info("Lancement du job...");
        env.execute("SedonaQuery1-AIS — High-Risk Zone Proximity Monitoring (sans MEOS)");
    }
}