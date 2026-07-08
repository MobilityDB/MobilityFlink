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
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
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
import java.util.Properties;

/**
 * Query 1 - High-Risk Zone Proximity Monitoring (SNCB dataset)
 *
 * <p>Detects trains within {@link #ALERT_DISTANCE_METERS} of predefined high-risk
 * zones along the Belgian railway network.
 *
 * <p>Original MobilityNebula pseudocode:
 * <pre>
 *   Query::from(GPS)
 *     .filter(edwithin_tgeo_geo(lon, lat, ts, INPolygons, 20) == 1) // Line 2
 *     .window(TumblingWindow::of(EventTime(ts), Seconds(10)))        // Line 3
 *     .sink(PrintSinkDescriptor::create());                          // Line 4
 * </pre>
 *
 * <p><b>HOW TO RUN:</b> change Dockerfile entrypoint to {@code sncbdata.Query1_Main},
 * then {@code mvn clean package && docker compose up --build}.
 */
public class Query1_Main {
    private static final Logger logger = LoggerFactory.getLogger(Query1_Main.class);

    /** Distance threshold in metres: paper specifies 20 m for SNCB trains. */
    private static final double ALERT_DISTANCE_METERS = 20.0;

    /**
     * High-risk zones along the Belgian railway network (INPolygons).
     *
     * Zone 1 - Brussels-South area
     * Zone 2 - Brussels-North / Schaerbeek area
     * Zone 3 - Ghent area
     * Zone 4 - Antwerp area
     */
    private static final String[] HIGH_RISK_ZONES_WKT = {
            "POLYGON((4.3550 50.6350, 4.3550 50.6550, 4.3750 50.6550, 4.3750 50.6350, 4.3550 50.6350))",
            "POLYGON((4.3500 50.8600, 4.3500 50.8800, 4.3700 50.8800, 4.3700 50.8600, 4.3500 50.8600))",
            "POLYGON((3.7100 51.0200, 3.7100 51.0400, 3.7300 51.0400, 3.7300 51.0200, 3.7100 51.0200))",
            "POLYGON((4.4100 51.2900, 4.4100 51.3100, 4.4300 51.3100, 4.4300 51.2900, 4.4100 51.2900))"
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
            properties.setProperty("group.id", "flink_consumer_q1");
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName());
            properties.setProperty("auto.offset.reset", "earliest");

            KafkaSource<SNCBData> kafkaSource = KafkaSource.<SNCBData>builder()
                    .setBootstrapServers("kafka:29092")
                    .setGroupId("flink_consumer_q1")
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
                    .process(new HighRiskZoneWindowFunction(HIGH_RISK_ZONES_WKT, ALERT_DISTANCE_METERS))
                    .print();

            env.execute("Query 1 - High-Risk Zone Proximity Monitoring");
            logger.info("Done");
        } catch (Exception e) {
            logger.error("Error during execution: {}", e.getMessage(), e);
            throw e;
        } finally {
            try {
                logger.info("Finalizing MEOS library");
                functions.meos_finalize();
            } catch (Exception e) {
                logger.error("Error during MEOS finalization: {}", e.getMessage());
            }
        }
    }

    public static class HighRiskZoneWindowFunction
            extends ProcessWindowFunction<SNCBData, String, Integer, TimeWindow> {

        private static final Logger log =
                LoggerFactory.getLogger(HighRiskZoneWindowFunction.class);

        private final String[] zoneWkt;
        private final double distanceMeters;

        private transient Pointer[] hazardZones;
        private transient error_handler_fn error_handler;

        private static final DateTimeFormatter TIMESTAMP_FMT =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssxxx");

        public HighRiskZoneWindowFunction(String[] zoneWkt, double distanceMeters) {
            this.zoneWkt = zoneWkt;
            this.distanceMeters = distanceMeters;
        }

        @Override
        public void open(OpenContext parameters) throws Exception {
            super.open(parameters);
            error_handler = new MeosErrorHandler();
            functions.meos_initialize_timezone("UTC");
            functions.meos_initialize_error_handler(error_handler);
            // Parse INPolygons once per worker: not per window call.
            this.hazardZones = new Pointer[zoneWkt.length];
            for (int i = 0; i < zoneWkt.length; i++) {
                hazardZones[i] = functions.geog_in(zoneWkt[i], -1);
                if (hazardZones[i] == null) {
                    logger.error("geog_in returned null for ZONE {}", i + 1);
                }
            }
        }

        @Override
        public void process(Integer deviceId, Context context, Iterable<SNCBData> elements, Collector<String> collector) throws Exception {
            String windowStart = millisToTimestamp(context.window().getStart());
            String windowEnd   = millisToTimestamp(context.window().getEnd());

            for (SNCBData event : elements) {
                String ts = millisToTimestamp(event.getTimestamp());
                String tpointWkt = String.format("POINT(%f %f)@%s", event.getLon(), event.getLat(), ts);

                Pointer tpoint = functions.tgeogpoint_in(tpointWkt);
                if (tpoint == null) { log.error("tgeogpoint_in returned null for WKT: {}", tpointWkt); continue; }

                for (int i = 0; i < hazardZones.length; i++) {
                    if (hazardZones[i] == null) continue;
                    // Paper Line 2: edwithin_tgeo_geo returns 1 if tpoint is within
                    // distanceMeters of the zone polygon at any instant.
                    if (functions.edwithin_tgeo_geo(tpoint, hazardZones[i], distanceMeters) == 1) {
                        String alert = String.format(
                                "[ALERT][Q1] DeviceID=%-6d | lon=%10.5f lat=%9.5f"
                                        + " | ts=%s | within %.0f m of ZONE %d | window [%s - %s]",
                                deviceId, event.getLon(), event.getLat(),
                                ts, distanceMeters, i + 1, windowStart, windowEnd);
                        log.warn(alert);
                        collector.collect(alert);
                    }
                }
            }
        }

        private String millisToTimestamp(long millis) {
            Instant instant = Instant.ofEpochMilli(millis);
            OffsetDateTime dt = instant.atOffset(ZoneOffset.UTC);
            return dt.format(TIMESTAMP_FMT);
        }
    }
}
