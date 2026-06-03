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
import org.apache.flink.streaming.api.windowing.time.Time;
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

    static class DeviceIdKeySelector implements KeySelector<Row, Integer>, Serializable {
        @Override
        public Integer getKey(Row row) {
            return row.getFieldAs("device_id");
        }
    }

    static class TrajectoryCreationWindowFunction
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
                    "[TRAJ][Q3-Sedona-SNCB] DeviceID=%-6d | points=%3d | window [%s – %s]%n"
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
        logger.info("=== SedonaQuery3_Main (SNCB), starting (no MEOS) ===");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().disableClosureCleaner();

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        SedonaContext.create(env, tableEnv);
        logger.info("Sedona initialized");

        KafkaSource<SNCBData> kafkaSource = KafkaSource.<SNCBData>builder()
                .setBootstrapServers("kafka:29092")
                .setGroupId("flink_consumer_sedona_sncb_q3")
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

        DataStream<Row> stream = tableEnv
                .toDataStream(tableEnv.sqlQuery("SELECT device_id, lon, lat, ts_ms FROM sncb"))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner(new RowTimestampAssigner())
                                .withIdleness(Duration.ofMinutes(1))
                );

        stream
                .keyBy(new DeviceIdKeySelector())
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.milliseconds(10)))
                .process(new TrajectoryCreationWindowFunction())
                .print();

        logger.info("Job starting...");
        env.execute("SedonaQuery3-SNCB, Trajectory Creation (no MEOS)");
    }
}