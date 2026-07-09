package aisdata;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jnr.ffi.Pointer;
import functions.functions;
import functions.error_handler;

/**
 * Query 6 - Positional Divergence for a Device
 *
 * <p>This query is a Java/Flink adaptation of the MobilityNebula Query 6 described in the paper
 * <i>"MobilityNebula: A System for Processing Mobility Data Streams with MobilityDB"</i>.
 *
 * <p>The paper describes Query 6 as follows (Section 4.1):
 * <blockquote>
 *   "Query 6 computes a per-device positional divergence measure within each window. It
 *   performs a join on device_id (Line 2) and evaluates the join in ten-second tumbling windows
 *   (Line 3). Here, GPS2 denotes a second logical view of the GPS stream. For each pair, it
 *   computes the nearest approach distance between the two points (Line 4), retains only pairs
 *   where the left-side latitude is positive (lat > 0.0, Line 5), and emits the resulting
 *   mindist values."
 * </blockquote>
 *
 * <p>The original MobilityNebula pseudocode is:
 * <pre>
 *   Query::from(GPS)
 *     .joinWith(GPS2, device_id == device_id2)                             // Line 2 - self-join
 *     .window(TumblingWindow::of(EventTime(ts), Seconds(10)))              // Line 3
 *     .apply(nearest_approach_distance(lon, lat, ts, lon2, lat2, ts2))     // Line 4
 *     .filter(lat > 0.0)                                                   // Line 5
 * </pre>
 *
 * <p><b>Mapping to this implementation:</b>
 * <ul>
 *   <li><b>Line 2 (self-join on device_id)</b>: GPS2 is a second logical view of the same
 *       Kafka topic, consumed by a separate {@link KafkaSource} with a different consumer
 *       group ID ({@code flink_consumer_q6_left} vs {@code flink_consumer_q6_right}).
 *       Flink's windowed {@code join().where(mmsi).equalTo(mmsi)} groups pairs of events
 *       from the two views by MMSI within each window, equivalent to the
 *       {@code device_id == device_id2} join predicate.</li>
 *   <li><b>Line 3 (10-second tumbling window)</b>: {@code TumblingEventTimeWindows.of(Duration.ofSeconds(10))},
 *       same as Query 1.</li>
 *   <li><b>Line 4 (nearest_approach_distance)</b>: For each pair {@code (left, right)},
 *       two {@code tgeogpoint} instants are constructed and passed to {@code nad_tgeo_tgeo},
 *       the MEOS function implementing nearest approach distance between two temporal
 *       geography points. The result is the minimum distance (in metres) between the two
 *       trajectories at any shared instant.</li>
 *   <li><b>Line 5 (lat > 0.0)</b>: Only pairs where the left-side latitude is strictly
 *       positive are emitted. All AIS data in this dataset (Danish waters) has positive
 *       latitudes (~55–58°N), so all pairs will pass</li>
 * </ul>
 *
 * <p><b>Self-join semantics:</b>
 * <br>Joining GPS with GPS2 (same stream, same device) within a tumbling window produces
 * all ordered pairs {@code (e_i, e_j)} where both events belong to the same MMSI and the
 * same 10-second window. This includes:
 * <ul>
 *   <li>Pairs where {@code e_i == e_j} (same event matched with itself) → NAD = 0.</li>
 *   <li>Pairs where {@code e_i != e_j} (different instants) → NAD = geodetic distance
 *       between the two positions.</li>
 * </ul>
 * The self-pairs (NAD = 0) are expected and represent the "no divergence" baseline. The
 * non-self pairs capture positional divergence between consecutive measurements within
 * the window, which serves as a motion indicator.
 *
 * <p><b>HOW TO RUN</b>
 * <br>In the Dockerfile, change the entrypoint from {@code aisdata.Main} to
 * {@code aisdata.Query6_Main}, then run:
 * <pre>
 *   mvn clean package &amp;&amp; docker compose up --build
 * </pre>
 *
 * <p><b>TROUBLESHOOTING</b>
 * <ul>
 *   <li>MobilityDB {@code stable-1.3} or later could be required depending on your needs:
 *       {@code https://github.com/MobilityDB/MobilityDB.git -b stable-1.3}</li>
 *   <li>Make sure you're using a JMEOS version compatible with your MobilityDB version:
 *       {@code --branch fix-tests-using-docker https://github.com/MobilityDB/JMEOS}</li>
 * </ul>
 */
public class Query6_Main {

    private static final Logger logger = LoggerFactory.getLogger(Query6_Main.class);

    public static void main(String[] args) throws Exception {

        logger.info("Java library path: {}", System.getProperty("java.library.path"));

        try {
            logger.info("Initializing MEOS library");
            functions.meos_initialize_timezone("UTC");
            functions.meos_initialize_error_handler(new error_handler());

            final StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);

            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName());
            properties.setProperty("auto.offset.reset", "earliest");

            // GPS: left side of the join (paper: Query::from(GPS))
            KafkaSource<AISData> kafkaSourceLeft = KafkaSource.<AISData>builder()
                    .setBootstrapServers("kafka:29092")
                    .setGroupId("flink_consumer_q6_left")
                    .setTopics("aisdata")
                    .setStartingOffsets(OffsetsInitializer.earliest())
                    .setValueOnlyDeserializer(new AISDataDeserializationSchema())
                    .build();

            // GPS2: right side of the join (paper: "a second logical view of the GPS stream")
            // Separate consumer group so both sources read all messages independently.
            KafkaSource<AISData> kafkaSourceRight = KafkaSource.<AISData>builder()
                    .setBootstrapServers("kafka:29092")
                    .setGroupId("flink_consumer_q6_right")
                    .setTopics("aisdata")
                    .setStartingOffsets(OffsetsInitializer.earliest())
                    .setValueOnlyDeserializer(new AISDataDeserializationSchema())
                    .build();

            // Both streams use the same watermark strategy: event-time with 10s tolerance.
            WatermarkStrategy<AISData> watermarkStrategy =
                    WatermarkStrategy.<AISData>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                            .withTimestampAssigner((event, recordTs) -> event.getTimestamp())
                            .withIdleness(Duration.ofMinutes(1));

            DataStream<AISData> gps  = env.fromSource(kafkaSourceLeft,  watermarkStrategy, "Kafka GPS");
            DataStream<AISData> gps2 = env.fromSource(kafkaSourceRight, watermarkStrategy, "Kafka GPS2");

            // Pipeline implementing the MobilityNebula Query 6 pseudocode (paper Section 4.1):
            //
            //   join(gps2).where(mmsi).equalTo(mmsi)  → paper Line 2: self-join on device_id.
            //                                            Flink pairs every event from GPS with
            //                                            every event from GPS2 that shares the
            //                                            same MMSI within the window.
            //   window(Tumbling 10s)                  → paper Line 3.
            //   apply(NearestApproachJoinFunction)     → paper Lines 4 & 5:
            //                                            nad_tgeo_tgeo + lat > 0.0 filter.
            //   print()                               → emits mindist values.
            gps.join(gps2)
                    .where(AISData::getMmsi)
                    .equalTo(AISData::getMmsi)
                    .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
                    .apply(new NearestApproachJoinFunction())
                    .print();

            env.execute("Query 6 - Positional Divergence for a Device");
            logger.info("Done");

        } catch (Exception e) {
            logger.error("Error during execution: {}", e.getMessage(), e);
            throw e;
        } finally {
            try {
                logger.info("Finalizing MEOS library");
                functions.meos_finalize();
            } catch (Exception e) {
                logger.error("Error during MEOS finalization: {}", e.getMessage(), e);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Join function
    // -----------------------------------------------------------------------

    /**
     * Flink {@link JoinFunction} implementing paper Lines 4 and 5 for each pair
     * {@code (left, right)} produced by the windowed self-join.
     *
     * <p>Called by Flink once per matched pair within a 10-second tumbling window. Both
     * {@code left} and {@code right} have the same MMSI (enforced by the join predicate).
     *
     * <p>MEOS is initialised inline for each call because {@link JoinFunction} does not
     * expose a Flink {@code open()} lifecycle method.
     */
    public static class NearestApproachJoinFunction extends RichJoinFunction<AISData, AISData, String> {

        private static final Logger log =
                LoggerFactory.getLogger(NearestApproachJoinFunction.class);

        private static final DateTimeFormatter TIMESTAMP_FMT =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssxxx");

        private transient error_handler errorHandler;

        @Override
        public void open(OpenContext parameters) throws Exception {
            super.open(parameters);
            errorHandler = new error_handler();
            functions.meos_initialize_timezone("UTC");
            functions.meos_initialize_error_handler(errorHandler);
        }

        @Override
        public String join(AISData left, AISData right) throws Exception {

            String tsLeft  = millisToTimestamp(left.getTimestamp());
            String tsRight = millisToTimestamp(right.getTimestamp());

            // Build tgeogpoint instants for both sides of the pair.
            Pointer tpointLeft = functions.tgeogpoint_in(
                    String.format("POINT(%f %f)@%s", left.getLon(),  left.getLat(),  tsLeft));
            Pointer tpointRight = functions.tgeogpoint_in(
                    String.format("POINT(%f %f)@%s", right.getLon(), right.getLat(), tsRight));

            if (tpointLeft == null || tpointRight == null) {
                log.error("tgeogpoint_in returned null for pair MMSI={}", left.getMmsi());
                return null;
            }

            // Paper Line 4: nearest_approach_distance(lon, lat, ts, lon2, lat2, ts2)
            //
            // The paper's nearest_approach_distance maps to nad_tgeo_tgeo in MEOS, which
            // computes the minimum distance between two temporal trajectories over their
            // shared time interval:
            //
            //   nad(traj1, traj2) = min over t in [t_overlap] of distance(traj1(t), traj2(t))
            //
            // However, nad_tgeo_tgeo requires temporal overlap between the two inputs.
            // For two TInstants at DIFFERENT timestamps there is no overlap, so MEOS
            // returns Double.MAX_VALUE (+infinity) which is unusable for divergence measurement.
            // For two TInstants at the SAME timestamp it returns the correct distance,
            // but in our self-join most pairs have different timestamps.
            //
            // Alternative used here:
            // We use geog_distance on the raw geography points extracted via
            // temporal_end_value(), which gives the direct geodetic distance (metres on
            // WGS-84) between the two positions regardless of their timestamps. This is
            // equivalent to nad_tgeo_tgeo only when ts_left == ts_right. For ts_left !=
            // ts_right it is a spatial-only distance.
            Pointer geoLeft  = functions.tgeo_end_value(tpointLeft);
            Pointer geoRight = functions.tgeo_end_value(tpointRight);

            if (geoLeft == null || geoRight == null) {
                log.error("temporal_end_value returned null for MMSI={}", left.getMmsi());
                return null;
            }

            double mindist = functions.geog_distance(geoLeft, geoRight);

            // Paper Line 5: filter(lat > 0.0): keep only pairs where left-side lat is positive.
            // All AIS positions in this dataset are in the Northern Hemisphere (~55–58°N),
            // so all pairs pass this filter.
            if (left.getLat() <= 0.0) return null;

            String result = String.format(
                    "[MINDIST][Q6] MMSI=%-12d"
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

        private String millisToTimestamp(long millis) {
            Instant instant = Instant.ofEpochMilli(millis);
            OffsetDateTime dt = instant.atOffset(ZoneOffset.UTC);
            return dt.format(TIMESTAMP_FMT);
        }
    }
}