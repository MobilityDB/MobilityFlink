package sncbdata;

import jnr.ffi.Pointer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import functions.functions;
import functions.error_handler_fn;
import functions.MeosErrorHandler;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class Query6_Main {
    private static final Logger logger = LoggerFactory.getLogger(Query6_Main.class);

    public static void main(String[] args) throws Exception {
        logger.info("Java library path: {}", System.getProperty("java.library.path"));

        try {
            logger.info("Initializing MEOS");
            functions.meos_initialize_timezone("UTC");
            functions.meos_initialize_error_handler(new MeosErrorHandler());

            final StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);

            KafkaSource<SNCBData> kafkaSourceLeft =
                    KafkaSource
                            .<SNCBData>builder()
                            .setBootstrapServers("kafka:29092")
                            .setGroupId("flink_consumer_q6_left")
                            .setTopics("sncbdata")
                            .setStartingOffsets(OffsetsInitializer.earliest())
                            .setValueOnlyDeserializer(new SNCBDataDeserializationSchema())
                            .build();

            KafkaSource<SNCBData> kafkaSourceRight =
                    KafkaSource
                            .<SNCBData>builder()
                            .setBootstrapServers("kafka:29092")
                            .setGroupId("flink_consumer_q6_right")
                            .setTopics("sncbdata")
                            .setStartingOffsets(OffsetsInitializer.earliest())
                            .setValueOnlyDeserializer(new SNCBDataDeserializationSchema())
                            .build();

            WatermarkStrategy<SNCBData> watermarkStrategy =
                    WatermarkStrategy
                            .<SNCBData>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                            .withTimestampAssigner((event, recordTs) -> event.getTimestamp())
                            .withIdleness(Duration.ofMinutes(1));

            DataStream<SNCBData> leftSource =
                    env
                            .fromSource(kafkaSourceLeft, watermarkStrategy, "Kafka Left Source");

            DataStream<SNCBData> rightSource =
                    env
                            .fromSource(kafkaSourceRight, watermarkStrategy, "Kafka Right Source");

            leftSource
                    .join(rightSource)
                    .where(SNCBData::getDeviceId)
                    .equalTo(SNCBData::getDeviceId)
                    .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
                    .apply(new NearestApproachJoinFunction())
                    .print();

            env.execute();

        } catch (Exception e) {
            logger.error("Error during execution: {}", e.getMessage(), e);
            throw e;
        } finally {
            try {
                logger.info("Finalizing MEOS");
                functions.meos_finalize();
            } catch (Exception e) {
                logger.error("Error during MEOS finalization: {}", e.getMessage(), e);
            }
        }
    }

    public static class NearestApproachJoinFunction
        extends RichJoinFunction<SNCBData, SNCBData, String> {

            private static final Logger log = LoggerFactory.getLogger(NearestApproachJoinFunction.class);

            private static final DateTimeFormatter TIMESTAMP_FMT =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssxxx");

            // Initialized once per worker in open().
            private transient error_handler_fn errorHandler;

        @Override
        public void open(OpenContext parameters) throws Exception {
            super.open(parameters);
            errorHandler = new MeosErrorHandler();
            functions.meos_initialize_timezone("UTC");
            functions.meos_initialize_error_handler(errorHandler);
        }

        @Override
        public String join(SNCBData left, SNCBData right) throws Exception {
            String tsLeft = millisToTimestamp(left.getTimestamp());
            String tsRight = millisToTimestamp(right.getTimestamp());

            String pointLeft = String.format("Point(%f %f)@%s", left.getLon(), left.getLat(), tsLeft);
            String pointRight = String.format("Point(%f %f)@%s", right.getLon(), right.getLat(), tsRight);

            Pointer tpointLeft = functions.tgeogpoint_in(pointLeft);
            Pointer tpointRight = functions.tgeogpoint_in(pointRight);

            if (tpointLeft == null || tpointRight == null) {
                log.error("tgeogpoint_in returned null for DeviceID={}", left.getDeviceId());
                return null;
            }

            // Paper Line 4: nearest approach distance approximated via geog_distance.
            // nad_tgeo_tgeo requires temporal overlap: for TInstants at different timestamps
            // it returns Double.MAX_VALUE, so we use geog_distance on extracted geography points.
            Pointer geoLeft  = functions.tgeo_end_value(tpointLeft);
            Pointer geoRight = functions.tgeo_end_value(tpointRight);
            if (geoLeft == null || geoRight == null) {
                log.error("temporal_end_value returned null for DeviceID={}", left.getDeviceId());
                return null;
            }

            double mindist = functions.geog_distance(geoLeft, geoRight);

            // Paper Line 5: lat > 0.0.
            if (left.getLat() <= 0.0 || right.getLat() <= 0.0) return null;

            String result = String.format(
                    "[MINDIST][Q6] DeviceID=%-6d"
                            + " | left(lon=%10.5f lat=%9.5f ts=%s)"
                            + " | right(lon=%10.5f lat=%9.5f ts=%s)"
                            + " | mindist=%12.3f m",
                    left.getDeviceId(),
                    left.getLon(), left.getLat(), tsLeft,
                    right.getLon(), right.getLat(), tsRight,
                    mindist);

            log.info(result);
            return result;
        }

        private String millisToTimestamp(long millis) {
            Instant instant = Instant.ofEpochMilli(millis);
            OffsetDateTime dt = instant.atOffset(ZoneOffset.UTC);
            return dt.format(TIMESTAMP_FMT);
        }
    }
}
