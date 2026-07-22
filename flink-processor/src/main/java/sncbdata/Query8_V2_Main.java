package sncbdata;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

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
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import functions.GeneratedFunctions;
import functions.error_handler;
import functions.error_handler_fn;
import types.temporal.TInterpolation;

/**
 * Query 8 V2 - Trajectory Denoising with MEOS native EKF (SNCB dataset)
 *
 * <p>Uses the Extended Kalman Filter implemented natively in MEOS via
 * {@code temporal_ext_kalman_filter()} (PR #749: marianaGarcez/MobilityDB fork).
 * This replaces the Apache Commons Math Kalman Filter used in V1.
 *
 * <p>Original MobilityNebula pseudocode:
 * <pre>
 *   Query::from(GPS)
 *     .window(TumblingWindow::of(EventTime(ts), Seconds(10)))          // Line 2
 *     .apply(temporal_ext_kalman_filter(                               // Line 3
 *         temporal_sequence(lon, lat, ts),
 *         gate=3.0, q=0.01, variance=1.0, dropOutliers=false));
 * </pre>
 *
 * <h2>Key differences from Query8 V1</h2>
 * <ul>
 *   <li><b>Noise parameter units:</b> MEOS EKF operates in degree² units
 *       (coordinates stay in lon/lat), whereas V1 operated in metre² (ENU space).
 *       The paper values (q=0.01, variance=1.0) are in metre² and cannot be used
 *       directly here. The defaults from the C reference example are used instead:
 *       gate=8.0, q=5e-10 deg²/s⁴, r=4e-6 deg².</li>
 * </ul>
 *
 * <h2>Build requirement</h2>
 * <p>Requires {@code libmeos.so} compiled from the {@code marianaGarcez/MobilityDB}
 * fork (branch {@code master}) and {@code GeneratedFunctions.java} patched to expose
 * {@code temporal_ext_kalman_filter}, both handled by {@code Dockerfile.q8v2}.
 *
 * <p><b>HOW TO RUN:</b>
 * {@code docker compose -f docker-compose-q8v2.yml up --build}
 */
public class Query8_V2_Main {

    private static final Logger logger = LoggerFactory.getLogger(Query8_V2_Main.class);

    // EKF parameters: degree² units (C reference example defaults: https://github.com/marianaGarcez/MobilityDB/blob/e3319c1e6fc9157d19cb0580e097224cd42a8214/meos/examples/ais_ekf_clean.c)
    private static final double GATE = 3.0;

    private static final double Q = 5e-10;

    private static final double R = 4e-6;

    private static final boolean DROP_OUTLIERS = false;

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
                    .setGroupId("flink_consumer_q8v2")
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
                    .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
                    .process(new MeosEkfWindowFunction(GATE, Q, R, DROP_OUTLIERS))
                    .print();

            env.execute("Query 8 V2 - Trajectory Denoising with MEOS native EKF (SNCB)");

        } catch (Exception e) {
            logger.error("Error during execution: {}", e.getMessage(), e);
            throw e;
        } finally {
            try { GeneratedFunctions.meos_finalize(); }
            catch (Exception e) { logger.error("Error during MEOS finalization: {}", e.getMessage(), e); }
        }
    }

    // =========================================================================
    // Window function
    // =========================================================================

    public static class MeosEkfWindowFunction
            extends ProcessWindowFunction<SNCBData, String, Integer, TimeWindow> {

        private static final Logger log = LoggerFactory.getLogger(MeosEkfWindowFunction.class);

        private final double gate;
        private final double q;
        private final double r;
        private final boolean dropOutliers;

        private transient error_handler_fn errorHandler;

        public MeosEkfWindowFunction(double gate, double q, double r, boolean dropOutliers) {
            this.gate = gate;
            this.q = q;
            this.r = r;
            this.dropOutliers = dropOutliers;
        }

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

            // 1. Collect and sort
            List<SNCBData> sorted = new ArrayList<>();
            for (SNCBData e : elements) sorted.add(e);
            sorted.sort((a, b) -> Long.compare(a.getTimestamp(), b.getTimestamp()));
            if (sorted.isEmpty()) return;
            // EKF requires at least 2 points to be meaningful.
            // With 1 point, we skip the filter and emit the raw single-point sequence directly.
            if (sorted.size() < 2) {
                SNCBData event = sorted.get(0);
                String wkt   = String.format("POINT(%f %f)@%s",
                        event.getLon(), event.getLat(), millisToTs(event.getTimestamp()));
                Pointer inst = GeneratedFunctions.tgeompoint_in(wkt);
                if (inst == null) return;
                Runtime runtime = Runtime.getSystemRuntime();
                Pointer seedArray = Memory.allocate(runtime, Long.BYTES);
                seedArray.putPointer(0, inst);
                Pointer singleSeq = GeneratedFunctions.tsequence_make(
                        seedArray, 1, true, true, TInterpolation.LINEAR.getValue(), true);
                if (singleSeq == null) return;
                String ewkt = GeneratedFunctions.tspatial_as_ewkt(singleSeq, 6);
                String result = String.format(
                        "[EKF-V2][Q8] DeviceID=%-6d | points=1 | EKF skipped (single point)"
                                + " | window [%s - %s]%n"
                                + "             raw=cleaned: %s",
                        deviceId, windowStart, windowEnd, ewkt);
                log.info(result);
                out.collect(result);
                return;
            }

            // 2. Build tgeompoint instants via tgeompoint_in (WKT string).
            Runtime runtime = Runtime.getSystemRuntime();
            List<Pointer> instants = new ArrayList<>(sorted.size());

            for (SNCBData event : sorted) {
                String wkt   = String.format("POINT(%f %f)@%s",
                        event.getLon(), event.getLat(), millisToTs(event.getTimestamp()));
                Pointer inst = GeneratedFunctions.tgeompoint_in(wkt);
                if (inst == null) {
                    log.error("[EKF-V2] tgeompoint_in returned null for DeviceID={} wkt={}", deviceId, wkt);
                    continue;
                }
                instants.add(inst);
            }
            if (instants.size() < 2) return;

            // 3. Assemble the raw tgeompoint sequence.
            Pointer ptrArray = Memory.allocate(runtime, Math.toIntExact((long) instants.size() * Long.BYTES));
            for (int i = 0; i < instants.size(); i++) {
                ptrArray.putPointer((long) i * Long.BYTES, instants.get(i));
            }
            Pointer rawSeq = GeneratedFunctions.tsequence_make(
                    ptrArray, instants.size(), true, true, TInterpolation.LINEAR.getValue(), true);
            if (rawSeq == null) {
                log.error("[EKF-V2] tsequence_make returned null for DeviceID={}", deviceId);
                return;
            }

            // 4. Apply MEOS native Extended Kalman Filter.
            //    temporal_ext_kalman_filter(temp, gate, q, r, drop_outliers)
            //    - gate : Mahalanobis threshold
            //    - q    : process noise spectral density
            //    - r    : measurement noise variance
            //    Returns a new Temporal* (cleaned tgeompoint sequence/set).
            Pointer cleanedSeq = GeneratedFunctions.temporal_ext_kalman_filter(rawSeq, gate, q, r, dropOutliers);
            if (cleanedSeq == null) {
                log.warn("[EKF-V2] temporal_ext_kalman_filter returned null for DeviceID={} "
                        + "(window may be too small). Falling back to raw.", deviceId);
                cleanedSeq = rawSeq;
            }

            // 5. Format output: raw and cleaned EWKT side-by-side.
            String rawEwkt     = GeneratedFunctions.tspatial_as_ewkt(rawSeq, 6);
            String cleanedEwkt = GeneratedFunctions.tspatial_as_ewkt(cleanedSeq, 6);

            String result = String.format(
                    "[EKF-V2][Q8] DeviceID=%-6d | points=%2d | gate=%.1f q=%.2e r=%.2e drop=%b"
                            + " | window [%s - %s]%n"
                            + "             raw:     %s%n"
                            + "             cleaned: %s",
                    deviceId, instants.size(), gate, q, r, dropOutliers,
                    windowStart, windowEnd,
                    rawEwkt, cleanedEwkt);

            log.info(result);
            out.collect(result);
        }

        private static String millisToTs(long millis) {
            return Instant.ofEpochMilli(millis).atOffset(ZoneOffset.UTC)
                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssxxx"));
        }
    }
}