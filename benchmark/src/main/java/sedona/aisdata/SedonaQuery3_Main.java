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
 * SedonaQuery3_Main (AIS): Trajectory Creation
 *
 * MEOS (Query3_Main.java):
 *   - Line 2 : SlidingEventTimeWindows(10s, 10ms)
 *   - Line 3 : temporal_sequence(lon, lat, ts)
 *              → tgeogpoint_in("{POINT(lon lat)@ts, ...}")
 *              → type spatio-temporel natif MEOS, timestamps couplés à la géométrie
 *
 * Sedona isn't able to implement Q3:
 *   - Sedona operates on indiviual records (ST_Distance(record, polygon), ST_Intersects(record, polygon), etc.)
 *   - Q3 is an agregation: we collect N records of a window to build a unique trajectory
 *
 * "Countermeasure": JTS in the window function :
 *   - JTS is the library which Sedona is built upon
 *   - CoordinateXYM : 2D coordinates + dimension M (measure) = timestamp
 *   - WKTWriter(3) : sérialise en WKT avec les 3 dimensions (X, Y, M)
 */
public class SedonaQuery3_Main {

    private static final Logger logger = LoggerFactory.getLogger(SedonaQuery3_Main.class);

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

    // JTS CoordinateXYM :
    //   X = longitude, Y = latitude, M = timestamp
    //
    // WKTWriter(3) : writes the 3 dimensions (X, Y, M) in the WKT.
    static class TrajectoryCreationWindowFunction
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

            // 1: collect and sort by ts_ms for JTS
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

            // 2: build the JTS coordinates
            //
            // CoordinateXYM(x, y, m) :
            //   x = longitude, y = latitude, m = epoch ms (timestamp)
            //   JTS equivalent of "POINT(lon lat)@ts" in MEOS.
            CoordinateXYM[] coords = new CoordinateXYM[sorted.size()];
            for (int i = 0; i < sorted.size(); i++) {
                Row    row = sorted.get(i);
                Double lon = row.getFieldAs("lon");
                Double lat = row.getFieldAs("lat");
                Long   ts  = row.getFieldAs("ts_ms");
                coords[i] = new CoordinateXYM(lon, lat, ts != null ? ts : 0L);
            }

            // 3: build the JTS geometry and serialize in WKT
            //
            // MEOS → tgeogpoint_in("{POINT(lon lat)@ts,...}") → tgeogpoint TSequence
            // JTS  → FACTORY.createLineString(coords)          → LineString M
            //
            // WKTWriter(3) produit : "LINESTRING M (lon1 lat1 ts1, lon2 lat2 ts2, ...)"
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
                    "[TRAJ][Q3-Sedona-AIS] MMSI=%-12d | points=%3d | window [%s – %s]%n"
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
        logger.info("=== SedonaQuery3_Main (AIS), starting (no MEOS) ===");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().disableClosureCleaner();

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        SedonaContext.create(env, tableEnv);
        logger.info("Sedona initialized");

        KafkaSource<AISData> kafkaSource = KafkaSource.<AISData>builder()
                .setBootstrapServers("kafka:29092")
                .setGroupId("flink_consumer_sedona_ais_q3")
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

        DataStream<Row> stream = tableEnv
                .toDataStream(tableEnv.sqlQuery("SELECT mmsi, lon, lat, ts_ms FROM ais"))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner(new RowTimestampAssigner())
                                .withIdleness(Duration.ofMinutes(1))
                );

        stream
                .keyBy(new MmsiKeySelector())
                .window(SlidingEventTimeWindows.of(Duration.ofSeconds(10), Duration.ofMillis(10)))
                .process(new TrajectoryCreationWindowFunction())
                .print();

        logger.info("Job running...");
        env.execute("SedonaQuery3-AIS, Trajectory Creation (no MEOS)");
    }
}