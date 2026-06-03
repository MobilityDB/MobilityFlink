package aisdata;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jnr.ffi.Pointer;
import functions.functions;
import functions.error_handler;
import functions.error_handler_fn;

/**
 * Query 3 - Trajectory Creation
 *
 * <p>This query is a Java/Flink adaptation of the MobilityNebula Query 3 described in the paper
 * <i>"MobilityNebula: A System for Processing Mobility Data Streams with MobilityDB"</i>.
 *
 * <p>The paper describes Query 3 as follows (Section 4.1):
 * <blockquote>
 *   "Query 3 performs trajectory creation. It first applies a ten-second sliding window with a
 *   ten-millisecond step over the event timestamp (Line 2). Within each window, the query
 *   computes the trajectory by using attributes longitude, latitude, and timestamp (Line 3),
 *   and outputs the train position as a trajectory."
 * </blockquote>
 *
 * <p>The original MobilityNebula pseudocode is:
 * <pre>
 *   Query::from(GPS)
 *     .window(SlidingWindow::of(EventTime(ts), Seconds(10), Milliseconds(10))) // Line 2
 *     .apply(temporal_sequence(lon, lat, ts));                                  // Line 3
 * </pre>
 *
 * <p><b>Mapping to this implementation:</b>
 * <ul>
 *   <li><b>Line 2 (sliding window 10s / 10ms)</b>: Implemented with
 *       {@code SlidingEventTimeWindows.of(Time.seconds(10), Time.milliseconds(10))},
 *       identical to Query 2. Each window collects up to 10 seconds of GPS points for one
 *       vessel, and a new window opens every 10 milliseconds, producing overlapping snapshots
 *       of the vessel's recent trajectory.</li>
 *   <li><b>Line 3 (temporal_sequence(lon, lat, ts))</b>: The {@code temporal_sequence} operator
 *       assembles individual GPS instants into a MobilityDB temporal sequence. In this
 *       implementation:
 *       <ol>
 *         <li>Each event is formatted as a {@code tgeogpoint} instant:
 *             {@code "POINT(lon lat)@timestamp"}.</li>
 *         <li>All instants are sorted by timestamp and concatenated into a sequence literal:
 *             {@code "{POINT(lon1 lat1)@ts1, POINT(lon2 lat2)@ts2, ...}"}.</li>
 *         <li>The sequence is parsed by {@code tgeogpoint_in} to create a native MEOS
 *             {@code tgeogpoint} sequence pointer.</li>
 *         <li>The sequence is serialised back to EWKT via {@code tspatial_as_ewkt} for human-readable output.</li>
 *       </ol>
 *   </li>
 * </ul>
 *
 * <p><b>HOW TO RUN</b>
 * <br>In the Dockerfile, change the entrypoint from {@code aisdata.Main} to
 * {@code aisdata.Query3_Main}, then run:
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
public class Query3_Main {

    private static final Logger logger = LoggerFactory.getLogger(Query3_Main.class);

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
            properties.setProperty("bootstrap.servers", "kafka:29092");
            properties.setProperty("group.id", "flink_consumer_q3");
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName());
            properties.setProperty("auto.offset.reset", "earliest");

            KafkaSource<AISData> kafkaSource = KafkaSource.<AISData>builder()
                    .setBootstrapServers("kafka:29092")
                    .setGroupId("flink_consumer_q3")
                    .setTopics("aisdata")
                    .setStartingOffsets(OffsetsInitializer.earliest())
                    .setValueOnlyDeserializer(new AISDataDeserializationSchema())
                    .build();

            // Same watermark strategy as Queries 1 and 2 (see Query1_Main for detailed comments).
            DataStream<AISData> source = env
                    .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
                    .assignTimestampsAndWatermarks(
                            WatermarkStrategy.<AISData>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                    .withTimestampAssigner(
                                            (event, recordTs) -> event.getTimestamp())
                                    .withIdleness(Duration.ofMinutes(1)));

            // Pipeline implementing the MobilityNebula Query 3 pseudocode (paper Section 4.1):
            //
            //   keyBy(getMmsi)            → partitions the stream per vessel, so each window
            //                               holds GPS points for exactly one MMSI.
            //   window(Sliding 10s/10ms)  → paper Line 2: overlapping 10-second windows
            //                               advancing every 10 milliseconds, producing a
            //                               continuous stream of recent trajectory snapshots.
            //   process(WindowFunction)   → paper Line 3: temporal_sequence(lon, lat, ts)
            //                               assembles the points into a tgeogpoint sequence.
            //   print()                   → outputs each trajectory as a human-readable EWKT string.
            source
                    .keyBy(AISData::getMmsi)
                    .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.milliseconds(10)))
                    .process(new TrajectoryCreationWindowFunction())
                    .print();

            env.execute("Query 3 - Trajectory Creation");
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
    // Window function
    // -----------------------------------------------------------------------

    /**
     * Flink {@link ProcessWindowFunction} that implements paper Line 3
     * ({@code temporal_sequence(lon, lat, ts)}) for a single (MMSI, sliding window) pair.
     *
     * <p>Assembles the GPS instants in the window into a MobilityDB {@code tgeogpoint} sequence
     * and outputs it as a human-readable EWKT string. The sequence represents the vessel's trajectory over the
     * 10-second window, sampled at the AIS reporting rate (~1 message/second).
     */
    public static class TrajectoryCreationWindowFunction
            extends ProcessWindowFunction<AISData, String, Integer, TimeWindow> {

        private static final Logger log =
                LoggerFactory.getLogger(TrajectoryCreationWindowFunction.class);

        private transient error_handler_fn errorHandler;

        private static final DateTimeFormatter TIMESTAMP_FMT =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssxxx");

        /** Initialises the MEOS library for this operator instance. */
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            errorHandler = new error_handler();
            functions.meos_initialize_timezone("UTC");
            functions.meos_initialize_error_handler(errorHandler);
            log.info("MEOS initialized in TrajectoryCreationWindowFunction.open()");
        }

        /**
         * Assembles all GPS events in the window into a {@code tgeogpoint} sequence:
         * the implementation of paper Line 3: {@code temporal_sequence(lon, lat, ts)}.
         *
         * <p>Steps:
         * <ol>
         *   <li>Collect all events and sort by timestamp (MEOS requires strictly increasing
         *       timestamps in a sequence).</li>
         *   <li>Format each event as a {@code tgeogpoint} instant WKT:
         *       {@code "POINT(lon lat)@timestamp"}.</li>
         *   <li>Concatenate into a sequence literal:
         *       {@code "{POINT(lon1 lat1)@ts1, POINT(lon2 lat2)@ts2, ...}"}.</li>
         *   <li>Parse the sequence with {@code tgeogpoint_in} to obtain a native MEOS pointer.</li>
         *   <li>Serialise back to EWKT via {@code tspatial_as_ewkt} and emit.</li>
         * </ol>
         *
         * @param mmsi      vessel identifier (the key used by keyBy)
         * @param context   window metadata (start/end timestamps)
         * @param elements  all AISData events in this (MMSI, window) pair
         * @param out       collector for the trajectory EWKT string
         */
        @Override
        public void process(
                Integer mmsi,
                Context context,
                Iterable<AISData> elements,
                Collector<String> out) {

            String windowStart = millisToTimestamp(context.window().getStart());
            String windowEnd   = millisToTimestamp(context.window().getEnd());

            // Step 1: collect and sort by timestamp.
            // MEOS tgeogpoint_in requires instants in strictly increasing temporal order;
            // Flink does not guarantee arrival order within a window.
            List<AISData> sorted = new ArrayList<>();
            for (AISData e : elements) sorted.add(e);
            sorted.sort((a, b) -> Long.compare(a.getTimestamp(), b.getTimestamp()));

            if (sorted.isEmpty()) return;

            // Step 2 & 3: build the sequence literal: {POINT(lon lat)@ts, ...}
            // This is the WKT representation of a tgeogpoint TSequence
            StringBuilder seq = new StringBuilder("{");
            for (int i = 0; i < sorted.size(); i++) {
                AISData event = sorted.get(i);
                String ts = millisToTimestamp(event.getTimestamp());
                if (i > 0) seq.append(",");
                seq.append(String.format("POINT(%f %f)@%s", event.getLon(), event.getLat(), ts));
            }
            seq.append("}");

            // Step 4: parse the sequence into a native MEOS tgeogpoint pointer.
            // tgeogpoint_in accepts both single instants ("POINT(lon lat)@ts") and sequences
            // ("{POINT(...)@ts,...}"). Here we always pass a sequence.
            Pointer trajectory = functions.tgeogpoint_in(seq.toString());
            if (trajectory == null) {
                log.error("tgeogpoint_in returned null for sequence: {}", seq);
                return;
            }

            // Step 5: serialise the MEOS pointer back to a human-readable WKT string.
            // tspatial_as_ewkt(pointer, maxdd) converts any temporal spatial type to EWKT
            // (WKT with SRID prefix), producing human-readable "POINT(lon lat)@ts" output.
            // maxdd=6 gives 6 decimal places.
            String trajectoryWkt = functions.tspatial_as_ewkt(trajectory, 6);

            /*
                // We have this trajectory
                Pointer trajectory = functions.tgeogpoint_in(seq.toString());

                // We could ask where was the vessel at a certain timestamp ? MEOS can interpolate the position based on
                // the trajectory
                Pointer tstz = functions.pg_timestamptz_in("2021-01-08 00:04:33+00", -1);
                Pointer interpolated = functions.temporal_at_timestamptz(trajectory, tstz);

                // interpolated is a TInstant which was computed by Linear Interpolation by default
                String result = functions.tspatial_as_ewkt(interpolated, 6);
                // → SRID=4326;POINT(x.xxxxx xx.xxxxx)@2021-01-08 00:04:33+00
            */

            String output = String.format(
                    "[TRAJ][Q3] MMSI=%-12d | points=%3d | window [%s - %s]%n           trajectory: %s",
                    mmsi,
                    sorted.size(),
                    windowStart, windowEnd,
                    trajectoryWkt);

            log.info(output);
            out.collect(output);
        }

        private String millisToTimestamp(long millis) {
            Instant instant = Instant.ofEpochMilli(millis);
            OffsetDateTime dt = instant.atOffset(ZoneOffset.UTC);
            return dt.format(TIMESTAMP_FMT);
        }
    }
}