package aisdata;

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

import functions.functions;
import functions.GeneratedFunctions;
import functions.error_handler;
import functions.error_handler_fn;
import types.temporal.TInterpolation;

/**
 * Query 8 V2 - Trajectory Denoising with MEOS native EKF (AIS dataset)
 *
 * <p>Uses the Extended Kalman Filter implemented natively in MEOS via
 * {@code temporal_ext_kalman_filter()} (PR #749: marianaGarcez/MobilityDB fork).
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
 * <p><b>HOW TO RUN:</b> change Dockerfile entrypoint to {@code aisdata.Query8_V2_Main},
 * then {@code docker compose up --build}.
 */
public class Query8_V2_Main {

    private static final Logger logger = LoggerFactory.getLogger(Query8_V2_Main.class);

    // EKF parameters: degree² units (C reference example defaults: https://github.com/marianaGarcez/MobilityDB/blob/e3319c1e6fc9157d19cb0580e097224cd42a8214/meos/examples/ais_ekf_clean.c)
    private static final double  GATE          = 8.0;
    private static final double  Q             = 5e-10;
    private static final double  R             = 4e-6;
    private static final boolean DROP_OUTLIERS = false;

    public static void main(String[] args) throws Exception {

        logger.info("Java library path: {}", System.getProperty("java.library.path"));

        try {
            functions.meos_initialize_timezone("UTC");
            functions.meos_initialize_error_handler(new error_handler());

            final StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);

            KafkaSource<AISData> kafkaSource = KafkaSource.<AISData>builder()
                    .setBootstrapServers("kafka:29092")
                    .setGroupId("flink_consumer_q8v2_ais")
                    .setTopics("aisdata")
                    .setStartingOffsets(OffsetsInitializer.earliest())
                    .setValueOnlyDeserializer(new AISDataDeserializationSchema())
                    .build();

            WatermarkStrategy<AISData> watermarkStrategy =
                    WatermarkStrategy.<AISData>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                            .withTimestampAssigner((event, recordTs) -> event.getTimestamp())
                            .withIdleness(Duration.ofMinutes(1));

            DataStream<AISData> source = env.fromSource(kafkaSource, watermarkStrategy, "Kafka Source");

            source
                    .keyBy(AISData::getMmsi)
                    .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
                    .process(new MeosEkfWindowFunction(GATE, Q, R, DROP_OUTLIERS))
                    .print();

            env.execute("Query 8 V2 - Trajectory Denoising with MEOS native EKF (AIS)");

        } catch (Exception e) {
            logger.error("Error during execution: {}", e.getMessage(), e);
            throw e;
        } finally {
            try { functions.meos_finalize(); }
            catch (Exception e) { logger.error("Error during MEOS finalization: {}", e.getMessage(), e); }
        }
    }

    public static class MeosEkfWindowFunction
            extends ProcessWindowFunction<AISData, String, Integer, TimeWindow> {

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
            functions.meos_initialize_timezone("UTC");
            functions.meos_initialize_error_handler(errorHandler);
        }

        @Override
        public void process(Integer mmsi, Context context,
                            Iterable<AISData> elements, Collector<String> out) {

            String windowStart = millisToTs(context.window().getStart());
            String windowEnd   = millisToTs(context.window().getEnd());

            // 1. Collect and sort
            List<AISData> sorted = new ArrayList<>();
            for (AISData e : elements) sorted.add(e);
            sorted.sort((a, b) -> Long.compare(a.getTimestamp(), b.getTimestamp()));
            if (sorted.isEmpty()) return;

            Runtime runtime = Runtime.getSystemRuntime();

            // 2. Single-point case: EKF not applicable, emit raw point directly.
            if (sorted.size() < 2) {
                AISData event = sorted.get(0);
                String wkt   = String.format("POINT(%f %f)@%s",
                        event.getLon(), event.getLat(), millisToTs(event.getTimestamp()));
                Pointer inst = functions.tgeompoint_in(wkt);
                if (inst == null) return;
                Pointer seedArray = Memory.allocate(runtime, Long.BYTES);
                seedArray.putPointer(0, inst);
                Pointer singleSeq = functions.tsequence_make(
                        seedArray, 1, true, true, TInterpolation.LINEAR.getValue(), true);
                if (singleSeq == null) return;
                String result = String.format(
                        "[EKF-V2][Q8] MMSI=%-12d | points=1 | EKF skipped (single point)"
                                + " | window [%s - %s]%n"
                                + "             raw=cleaned: %s",
                        mmsi, windowStart, windowEnd,
                        functions.tspatial_as_ewkt(singleSeq, 6));
                log.info(result);
                out.collect(result);
                return;
            }

            // 3. Build tgeompoint instants via tgeompoint_in (WKT string).
            List<Pointer> instants = new ArrayList<>(sorted.size());
            for (AISData event : sorted) {
                String ts   = millisToTs(event.getTimestamp());
                String wkt  = String.format("POINT(%f %f)@%s", event.getLon(), event.getLat(), ts);
                Pointer inst = functions.tgeompoint_in(wkt);
                if (inst == null) {
                    log.error("[EKF-V2] tgeompoint_in returned null for MMSI={} wkt={}", mmsi, wkt);
                    continue;
                }
                instants.add(inst);
            }
            if (instants.size() < 2) return;

            // 4. Assemble raw tgeompoint sequence.
            Pointer ptrArray = Memory.allocate(runtime, Math.toIntExact((long) instants.size() * Long.BYTES));
            for (int i = 0; i < instants.size(); i++) {
                ptrArray.putPointer((long) i * Long.BYTES, instants.get(i));
            }
            Pointer rawSeq = functions.tsequence_make(
                    ptrArray, instants.size(), true, true, TInterpolation.LINEAR.getValue(), true);
            if (rawSeq == null) {
                log.error("[EKF-V2] tsequence_make returned null for MMSI={}", mmsi);
                return;
            }

            // 5. Apply MEOS native Extended Kalman Filter.
            Pointer cleanedSeq = GeneratedFunctions.temporal_ext_kalman_filter(rawSeq, gate, q, r, dropOutliers);
            if (cleanedSeq == null) {
                log.warn("[EKF-V2] temporal_ext_kalman_filter returned null for MMSI={} "
                        + "(window too small?). Falling back to raw.", mmsi);
                cleanedSeq = rawSeq;
            }

            String result = String.format(
                    "[EKF-V2][Q8] MMSI=%-12d | points=%2d | gate=%.1f q=%.2e r=%.2e drop=%b"
                            + " | window [%s - %s]%n"
                            + "             raw:     %s%n"
                            + "             cleaned: %s",
                    mmsi, instants.size(), gate, q, r, dropOutliers,
                    windowStart, windowEnd,
                    functions.tspatial_as_ewkt(rawSeq, 6),
                    functions.tspatial_as_ewkt(cleanedSeq, 6));

            log.info(result);
            out.collect(result);
        }

        private static String millisToTs(long millis) {
            return Instant.ofEpochMilli(millis).atOffset(ZoneOffset.UTC)
                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssxxx"));
        }
    }
}