package sncbdata;

import com.fasterxml.jackson.databind.deser.std.StringDeserializer;
import jnr.ffi.Pointer;
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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import functions.functions;
import functions.MeosErrorHandler;
import functions.error_handler_fn;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Query 2 - Brake System Monitoring (SNCB dataset)
 *
 * <p>Monitors variance of PCFA (automatic brake pipe pressure) and PCFF
 * (fictitious brake cylinder pressure) within a sliding window to detect
 * potential brake anomalies. Uses real pressure values from {@link SNCBData}.
 *
 * <p>Original MobilityNebula pseudocode:
 * <pre>
 *   Query::from(GPS)
 *     .filter(eintersects_tgeo_geo(lon, lat, ts, INPolygons) == 0) // Line 2
 *     .window(SlidingWindow::of(EventTime(ts), Seconds(10), Milliseconds(10))) // Line 3
 *     .apply(variation(PCFA_mbar), variation(PCFF_mbar))                       // Line 4
 *     .filter(varFA > 0.6 && varFF <= 0.5);                                    // Line 5
 * </pre>
 *
 * <p><b>HOW TO RUN:</b> change Dockerfile entrypoint to {@code sncbdata.Query2_Main},
 * then {@code mvn clean package && docker compose up --build}.
 */
public class Query2_Main {
    private static final Logger logger = LoggerFactory.getLogger(Query2_Main.class);

    private static final double VAR_FA_THRESHOLD = 0.6;
    private static final double VAR_FF_THRESHOLD = 0.5;

    /**
     * Maintenance area exclusion zones (INPolygons).
     * Placed in eastern Belgium away from the active train corridors in the dataset.
     */
    private static final String[] MAINTENANCE_AREAS_WKT = {
            "POLYGON((5.5500 50.6000, 5.5500 50.7000, 5.6500 50.7000, 5.6500 50.6000, 5.5500 50.6000))",
            "POLYGON((5.8000 49.7000, 5.8000 49.8000, 5.9000 49.8000, 5.9000 49.7000, 5.8000 49.7000))"
    };

    public static void main(String[] args) throws Exception {
        logger.info("Java library path: {}", System.getProperty("java.library.path"));

        try {
            logger.info("Initializing MEOS library");
            functions.meos_initialize_timezone("UTC");
            functions.meos_initialize_error_handler(new MeosErrorHandler());

            final StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);

            Properties properties = new Properties();
            properties.setProperty("bootstrap.servers", "kafka:29092");
            properties.setProperty("group.id", "flink_consumer_q2");
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName());
            properties.setProperty("auto.offset.reset", "earliest");

            KafkaSource<SNCBData> kafkaSource =
                    KafkaSource.<SNCBData>builder()
                            .setBootstrapServers("kafka:29092")
                            .setGroupId("flink_consumer_q2")
                            .setTopics("sncbdata")
                            .setStartingOffsets(OffsetsInitializer.earliest())
                            .setValueOnlyDeserializer(new SNCBDataDeserializationSchema())
                            .build();

            WatermarkStrategy<SNCBData> watermarkStrategy =
                    WatermarkStrategy.<SNCBData>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                            .withTimestampAssigner((event, recordTs) -> event.getTimestamp())
                            .withIdleness(Duration.ofMinutes(1));

            DataStream<SNCBData> source = env.fromSource(kafkaSource, watermarkStrategy, "Kafka Source");

            // Window: 10s / 10ms
            source
                    .keyBy(SNCBData::getDeviceId)
                    .window(SlidingEventTimeWindows.of(Duration.ofSeconds(10), Duration.ofMillis(10)))
                    .process(new BrakeMonitoringWindowFunction(
                            MAINTENANCE_AREAS_WKT, VAR_FA_THRESHOLD, VAR_FF_THRESHOLD))
                    .print();

            env.execute("Query 2 - Brake System Monitoring");
        } catch (Exception e) {
            logger.error("Error during execution: {}", e.getMessage(), e);
            throw e;
        } finally {
            try {
                functions.meos_finalize();
            } catch (Exception e) {
                logger.error("Error during MEOS finalization: {}", e.getMessage(), e);
            }
        }
    }

    public static class BrakeMonitoringWindowFunction
        extends ProcessWindowFunction<SNCBData, String, Integer, TimeWindow> {

        private static final Logger log = LoggerFactory.getLogger(Query2_Main.class);

        private final String[] maintenanceAreasWkt;
        private final double varFaThreshold;
        private final double varFfThreshold;

        private transient Pointer[] maintenanceZones;
        private transient error_handler_fn errorHandler;

        private static final DateTimeFormatter TIMESTAMP_FMT =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssxxx");

        public BrakeMonitoringWindowFunction(String[] maintenanceAreasWkt, double varFaThreshold, double varFfThreshold) {
            this.maintenanceAreasWkt = maintenanceAreasWkt;
            this.varFaThreshold = varFaThreshold;
            this.varFfThreshold = varFfThreshold;
        }

        @Override
        public void open(OpenContext parameters) throws Exception {
            super.open(parameters);
            errorHandler = new MeosErrorHandler();
            functions.meos_initialize_timezone("UTC");
            functions.meos_initialize_error_handler(errorHandler);
            // Parse maintenance area polygons once per worker
            maintenanceZones = new Pointer[maintenanceAreasWkt.length];
            for (int i = 0; i < maintenanceAreasWkt.length; i++) {
                maintenanceZones[i] = functions.geog_in(maintenanceAreasWkt[i], -1);
                if (maintenanceZones[i] == null) {
                    log.error("geog_in returned null for maintenance area {}", i + 1);
                }
            }
        }

        @Override
        public void process(Integer deviceId, ProcessWindowFunction<SNCBData, String, Integer, TimeWindow>.Context context, Iterable<SNCBData> elements, Collector<String> out) throws Exception {
            String windowStart = millisToTimestamp(context.window().getStart());
            String windowEnd = millisToTimestamp(context.window().getEnd());

            List<Double> faValues = new ArrayList<>();
            List<Double> ffValues = new ArrayList<>();

            for (SNCBData elem : elements) {
                String timestamp = millisToTimestamp(elem.getTimestamp());
                String point = String.format("Point(%f %f)@%s", elem.getLon(), elem.getLat(), timestamp);

                Pointer tpoint = functions.tgeogpoint_in(point);
                if (tpoint == null) {
                    log.error("tgeogpoint_in returned null for WKT: {}", point);
                    continue;
                }

                // Line 2: exclude points inside maintenance areas
                boolean inMaintenanceArea = false;
                for (Pointer zone : maintenanceZones) {
                    if (zone == null) continue;
                    if (functions.eintersects_tgeo_geo(tpoint, zone) == 1) {
                        inMaintenanceArea = true;
                        log.debug("MMSI={} skipped: point intersects maintenance area at ts={}",
                                deviceId, timestamp);
                        break;
                    }
                }
                if (inMaintenanceArea) continue;

                faValues.add(elem.getPcfaMbar());
                ffValues.add(elem.getPcffMbar());
            }

            if (ffValues.isEmpty() || faValues.isEmpty()) {
                return;
            }

            double varFA = variance(faValues);
            double varFF = variance(ffValues);

            if (varFA > varFaThreshold && varFF <= varFfThreshold) {
                String alert = String.format(
                        "[ALERT][Q2] DeviceID=%-6d | varFA=%6.4f bar² (>%.1f)"
                                + " | varFF=%6.4f bar² (<=%.1f) | events=%d | window [%s - %s]",
                        deviceId, varFA, varFaThreshold, varFF, varFfThreshold,
                        faValues.size(), windowStart, windowEnd);
                log.warn(alert);
                out.collect(alert);
            }
        }

        /**
         * Computes the statistical variance of a list of values.
         *
         * <p>This is the Java implementation of the {@code variation} operator from the paper
         * (Line 4). MEOS does not expose a {@code variation} function in its Java bindings, so
         * the computation is performed directly here.
         *
         * Formula : Sigma([(val - avg)^2] for each val) / number_of_val
         *
         * @param values sample values within one window (e.g. FA or FF readings)
         * @return variance in the same unit² as the input (bar² for the SNCB dataset), or
         *         {@code 0.0} if the list has fewer than two elements
         */
        static double variance(List<Double> values) {
            if (values.size() < 2) return 0.0;
            double mean = values.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
            double sumSq = values.stream().mapToDouble(v -> (v - mean) * (v - mean)).sum();
            return sumSq / values.size(); // population variance
        }

        private String millisToTimestamp(long millis) {
            Instant instant = Instant.ofEpochMilli(millis);
            OffsetDateTime dt = instant.atOffset(ZoneOffset.UTC);
            return dt.format(TIMESTAMP_FMT);
        }
    }
}
