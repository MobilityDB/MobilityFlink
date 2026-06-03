package sedona.aisdata;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
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

/**
 * SedonaQuery6_Main (AIS): Positional Divergence for a Device
 *
 * MEOS (Query6_Main.java):
 *   - Line 2: self-join on mmsi via two Kafka sources with different consumer group IDs
 *   - Line 3: TumblingEventTimeWindows(10s)
 *   - Line 4: nearest_approach_distance -> geog_distance(geoLeft, geoRight)
 *   - Line 5: filter lat > 0.0
 *
 * Sedona/JTS:
 *   - Line 2: identical, two Kafka sources with different consumer group IDs
 *   - Line 3: TumblingEventTimeWindows(10s)
 *   - Line 4: JTS point.distance(point) * 111000: planar approximation in meters
 *   - Line 5: filter lat > 0.0
 *
 * Why no Table API here:
 *   Q6 is a windowed join between two DataStreams, not a per-record SQL filter.
 *   The Table API operates on individual rows, not on pairs from two streams.
 *   JTS is used directly in the RichJoinFunction, which is the correct place
 *   for any computation that involves both sides of a join pair.
 */
public class SedonaQuery6_Main {

    private static final Logger logger = LoggerFactory.getLogger(SedonaQuery6_Main.class);

    // MEOS: geog_distance(tgeo_end_value(tpointLeft), tgeo_end_value(tpointRight))
    // JTS:  FACTORY.createPoint(lon, lat).distance(FACTORY.createPoint(lon2, lat2)) * 111000
    static class NearestApproachJoinFunction
            extends RichJoinFunction<AISData, AISData, String>
            implements Serializable {

        private static final Logger log =
                LoggerFactory.getLogger(NearestApproachJoinFunction.class);

        private static final DateTimeFormatter FMT =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssxxx");

        private transient GeometryFactory factory;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            factory = new GeometryFactory();
        }

        @Override
        public String join(AISData left, AISData right) throws Exception {

            if (left.getLat() <= 0.0 || right.getLat() <= 0.0) return null;

            // MEOS: geog_distance
            // JTS:  point.distance() returns planar degrees, * 111000 gives approx. meters
            Point pointLeft  = factory.createPoint(new Coordinate(left.getLon(),  left.getLat()));
            Point pointRight = factory.createPoint(new Coordinate(right.getLon(), right.getLat()));

            double mindist = pointLeft.distance(pointRight) * 111_000.0;

            String tsLeft  = millisToTs(left.getTimestamp());
            String tsRight = millisToTs(right.getTimestamp());

            String result = String.format(
                    "[MINDIST][Q6-Sedona-AIS] MMSI=%-12d"
                            + " | left(lon=%10.5f lat=%9.5f ts=%s)"
                            + " | right(lon=%10.5f lat=%9.5f ts=%s)"
                            + " | mindist=%12.3f m",
                    left.getMmsi(),
                    left.getLon(),  left.getLat(),  tsLeft,
                    right.getLon(), right.getLat(), tsRight,
                    mindist);

            log.info(result);
            return result;
        }

        private String millisToTs(long ms) {
            return Instant.ofEpochMilli(ms).atOffset(ZoneOffset.UTC).format(FMT);
        }
    }

    // =========================================================================
    // MAIN
    // =========================================================================
    public static void main(String[] args) throws Exception {
        logger.info("=== SedonaQuery6_Main (AIS), starting (no MEOS) ===");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        WatermarkStrategy<AISData> watermarkStrategy =
                WatermarkStrategy.<AISData>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner((event, ts) -> event.getTimestamp())
                        .withIdleness(Duration.ofMinutes(1));

        // GPS: left side of the join
        KafkaSource<AISData> kafkaSourceLeft = KafkaSource.<AISData>builder()
                .setBootstrapServers("kafka:29092")
                .setGroupId("flink_consumer_sedona_ais_q6_left")
                .setTopics("aisdata")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new AISDataDeserializationSchema())
                .build();

        // GPS2: right side of the join
        // Separate consumer group so both sources read all messages independently.
        KafkaSource<AISData> kafkaSourceRight = KafkaSource.<AISData>builder()
                .setBootstrapServers("kafka:29092")
                .setGroupId("flink_consumer_sedona_ais_q6_right")
                .setTopics("aisdata")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new AISDataDeserializationSchema())
                .build();

        DataStream<AISData> gps  = env.fromSource(kafkaSourceLeft,  watermarkStrategy, "Kafka GPS");
        DataStream<AISData> gps2 = env.fromSource(kafkaSourceRight, watermarkStrategy, "Kafka GPS2");

        // join(gps2).where(mmsi).equalTo(mmsi)
        // window(Tumbling 10s)
        // apply(NearestApproachJoinFunction)
        gps.join(gps2)
                .where(AISData::getMmsi)
                .equalTo(AISData::getMmsi)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new NearestApproachJoinFunction())
                .print();

        logger.info("Job running...");
        env.execute("SedonaQuery6-AIS, Positional Divergence for a Device (no MEOS)");
    }
}