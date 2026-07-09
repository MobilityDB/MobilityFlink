package sncbdata;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import functions.GeneratedFunctions;
import jnr.ffi.Memory;
import jnr.ffi.Pointer;
import jnr.ffi.Runtime;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import functions.GeneratedFunctions;
import functions.error_handler;
import functions.error_handler_fn;
import types.temporal.TInterpolation;

/**
 * Query 3 - Trajectory Creation (SNCB dataset)
 *
 * <p>Reconstructs the trajectory of each train from its GPS positions within a
 * sliding window. Two implementations are provided:
 *
 * <ul>
 *   <li><b>V1 (WKT)</b>: {@link TrajectoryCreationV1_WKT}: builds the whole sequence
 *       as a WKT string {@code {POINT(lon lat)@ts,...}} and calls {@code tgeogpoint_in()}
 *       once.</li>
 *   <li><b>V2 (Expand)</b>: {@link TrajectoryCreationV2_Expand}: builds the sequence
 *       incrementally using {@code temporal_append_tinstant()}.</li>
 * </ul>
 *
 * <p>To switch implementation, change the {@code .process(...)} call in {@link #main}.
 *
 * <p>Original MobilityNebula pseudocode:
 * <pre>
 *   Query::from(GPS)
 *     .window(SlidingWindow::of(EventTime(ts), Seconds(10), Milliseconds(10))) // Line 2
 *     .apply(temporal_sequence(lon, lat, ts));                                  // Line 3
 * </pre>
 *
 * <p><b>HOW TO RUN:</b> change Dockerfile entrypoint to {@code sncbdata.Query3_Main},
 * then {@code mvn clean package && docker compose up --build}.
 */
public class Query3_Main {

    private static final Logger logger = LoggerFactory.getLogger(Query3_Main.class);

    public static void main(String[] args) throws Exception {

        logger.info("Java library path: {}", System.getProperty("java.library.path"));

        try {
            GeneratedFunctions.meos_initialize_timezone("UTC");
            GeneratedFunctions.meos_initialize_error_handler(new error_handler());

            final StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);

            KafkaSource<SNCBData> kafkaSource = KafkaSource.<SNCBData>builder()
                    .setBootstrapServers("kafka:29092")
                    .setGroupId("flink_consumer_q3")
                    .setTopics("sncbdata")
                    .setStartingOffsets(OffsetsInitializer.earliest())
                    .setValueOnlyDeserializer(new SNCBDataDeserializationSchema())
                    .build();

            WatermarkStrategy<SNCBData> watermarkStrategy =
                    WatermarkStrategy.<SNCBData>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                            .withTimestampAssigner((event, recordTs) -> event.getTimestamp())
                            .withIdleness(Duration.ofMinutes(1));

            DataStream<SNCBData> source = env.fromSource(kafkaSource, watermarkStrategy, "Kafka Source");

            source
                    .keyBy(SNCBData::getDeviceId)
                    .window(SlidingEventTimeWindows.of(Duration.ofSeconds(10), Duration.ofMillis(10)))
                    // Switch here to compare implementations:
                    .process(new TrajectoryCreationV1_WKT())
                    //.process(new TrajectoryCreationV2_Expand())
                    .print();

            env.execute("Query 3 - Trajectory Creation (SNCB)");

        } catch (Exception e) {
            logger.error("Error during execution: {}", e.getMessage(), e);
            throw e;
        } finally {
            try { GeneratedFunctions.meos_finalize(); }
            catch (Exception e) { logger.error("Error during MEOS finalization: {}", e.getMessage(), e); }
        }
    }

    // =========================================================================
    // V1: WKT StringBuilder + tgeogpoint_in
    // =========================================================================

    /**
     * Builds the entire sequence as a WKT literal {@code {POINT(lon lat)@ts,...}}
     * and parses it in one call to {@code tgeogpoint_in()}.
     */
    public static class TrajectoryCreationV1_WKT
            extends ProcessWindowFunction<SNCBData, String, Integer, TimeWindow> {

        private static final Logger log = LoggerFactory.getLogger(TrajectoryCreationV1_WKT.class);
        private static final DateTimeFormatter TIMESTAMP_FMT =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssxxx");
        private transient error_handler_fn errorHandler;

        @Override
        public void open(OpenContext parameters) throws Exception {
            super.open(parameters);
            errorHandler = new error_handler();
            GeneratedFunctions.meos_initialize_timezone("UTC");
            GeneratedFunctions.meos_initialize_error_handler(errorHandler);
        }

        @Override
        public void process(Integer deviceId, Context context,
                            Iterable<SNCBData> elements, Collector<String> out) {

            String windowStart = millisToTs(context.window().getStart());
            String windowEnd   = millisToTs(context.window().getEnd());

            List<SNCBData> sorted = new ArrayList<>();
            for (SNCBData e : elements) sorted.add(e);
            sorted.sort((a, b) -> Long.compare(a.getTimestamp(), b.getTimestamp()));
            if (sorted.isEmpty()) return;

            StringBuilder seq = new StringBuilder("{");
            for (int i = 0; i < sorted.size(); i++) {
                SNCBData event = sorted.get(i);
                if (i > 0) seq.append(",");
                seq.append(String.format("POINT(%f %f)@%s",
                        event.getLon(), event.getLat(), millisToTs(event.getTimestamp())));
            }
            seq.append("}");

            Pointer trajectory = GeneratedFunctions.tgeogpoint_in(seq.toString());
            if (trajectory == null) {
                log.error("[V1] tgeogpoint_in returned null for sequence: {}", seq);
                return;
            }
            emit(out, log, "V1", deviceId, sorted.size(), windowStart, windowEnd, trajectory);
        }

        private String millisToTs(long millis) {
            return Instant.ofEpochMilli(millis).atOffset(ZoneOffset.UTC).format(TIMESTAMP_FMT);
        }
    }

    // =========================================================================
    // V2: Expand: tgeogpoint_in (instant) → temporal_append_tinstant
    // =========================================================================

    /**
     * Builds the sequence incrementally: the first instant seeds the sequence via
     * {@code tsequence_make}, and subsequent instants are appended with
     * {@code temporal_append_tinstant()}. MEOS handles capacity doubling internally.
     */
    public static class TrajectoryCreationV2_Expand
            extends ProcessWindowFunction<SNCBData, String, Integer, TimeWindow> {

        private static final Logger log = LoggerFactory.getLogger(TrajectoryCreationV2_Expand.class);
        private static final DateTimeFormatter TIMESTAMP_FMT =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssxxx");
        private transient error_handler_fn errorHandler;

        @Override
        public void open(OpenContext parameters) throws Exception {
            super.open(parameters);
            errorHandler = new error_handler();
            GeneratedFunctions.meos_initialize_timezone("UTC");
            GeneratedFunctions.meos_initialize_error_handler(errorHandler);
        }

        @Override
        public void process(Integer deviceId, Context context,
                            Iterable<SNCBData> elements, Collector<String> out) {

            String windowStart = millisToTs(context.window().getStart());
            String windowEnd   = millisToTs(context.window().getEnd());

            List<SNCBData> sorted = new ArrayList<>();
            for (SNCBData e : elements) sorted.add(e);
            sorted.sort((a, b) -> Long.compare(a.getTimestamp(), b.getTimestamp()));
            if (sorted.isEmpty()) return;

            Runtime runtime = Runtime.getSystemRuntime();
            Pointer trajectory = null;
            int count = 0;

            for (SNCBData event : sorted) {
                String wkt   = String.format("POINT(%f %f)@%s",
                        event.getLon(), event.getLat(), millisToTs(event.getTimestamp()));
                Pointer inst = GeneratedFunctions.tgeogpoint_in(wkt);
                if (inst == null) {
                    log.error("[V2] tgeogpoint_in returned null for DeviceID={} wkt={}", deviceId, wkt);
                    continue;
                }

                if (trajectory == null) {
                    // Seed the expandable sequence with the first instant.
                    Pointer seedArray = Memory.allocate(runtime, Long.BYTES);
                    seedArray.putPointer(0, inst);
                    trajectory = GeneratedFunctions.tsequence_make(
                            seedArray, 1, true, true, TInterpolation.LINEAR.getValue(), true);
                    if (trajectory == null) {
                        log.error("[V2] tsequence_make (seed) returned null for DeviceID={}", deviceId);
                        return;
                    }
                } else {
                    // Append: MEOS expands capacity as needed.
                    Pointer expanded = GeneratedFunctions.temporal_append_tinstant(
                            trajectory, inst, TInterpolation.LINEAR.getValue(), 0.0, null, true);
                    if (expanded == null) {
                        log.error("[V2] temporal_append_tinstant returned null for DeviceID={} wkt={}", deviceId, wkt);
                        continue;
                    }
                    trajectory = expanded;
                }
                count++;
            }

            if (trajectory == null || count == 0) return;
            emit(out, log, "V2", deviceId, count, windowStart, windowEnd, trajectory);
        }

        private String millisToTs(long millis) {
            return Instant.ofEpochMilli(millis).atOffset(ZoneOffset.UTC).format(TIMESTAMP_FMT);
        }
    }

    // =========================================================================
    // Shared output helper
    // =========================================================================

    private static void emit(Collector<String> out, Logger log, String version,
                             int deviceId, int points,
                             String windowStart, String windowEnd,
                             Pointer trajectory) {
        String trajectoryWkt = GeneratedFunctions.tspatial_as_ewkt(trajectory, 6);
        String output = String.format(
                "[TRAJ][Q3][%s] DeviceID=%-6d | points=%3d | window [%s - %s]%n"
                        + "              trajectory: %s",
                version, deviceId, points, windowStart, windowEnd, trajectoryWkt);
        log.info(output);
        out.collect(output);
    }
}