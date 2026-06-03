package aisdata;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
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
 * Query 1 - High-Risk Zone Proximity Monitoring
 *
 * <p>This query is a Java/Flink adaptation of the MobilityNebula Query 1 described in the paper
 * <i>"MobilityNebula: A System for Processing Mobility Data Streams with MobilityDB"</i>.
 *
 * <p>The paper describes Query 1 as follows (Section 4.1):
 * <blockquote>
 *   "Query 1 performs high-risk zone proximity monitoring. It identifies trains in proximity
 *   to dangerous zones by invoking edwithin_tgeo_geo on the SNCB stream against predefined
 *   high-risk area polygons (INPolygons) (Line 2). Any incoming temporal point within the
 *   specified distance of 20 meters in a ten-second tumbling window (Line 3) is immediately
 *   printed and generates real-time alerts for trains near hazardous locations."
 * </blockquote>
 *
 * <p>The original MobilityNebula pseudocode is:
 * <pre>
 *   Query::from(GPS)                                                 // Line 1 - input stream
 *     .filter(edwithin_tgeo_geo(lon, lat, ts, INPolygons, 20) == 1) // Line 2 - proximity filter
 *     .window(TumblingWindow::of(EventTime(ts), Seconds(10)))        // Line 3 - 10-second window
 *     .sink(PrintSinkDescriptor::create());                          // Line 4 - print alerts
 * </pre>
 *
 * <p><b>Mapping to this implementation:</b>
 * <ul>
 *   <li><b>Line 1 (GPS stream)</b>: Kafka source consuming the {@code aisdata} topic,
 *       keyed by MMSI (vessel identifier). In the paper this is the SNCB train GPS stream;
 *       here we use AIS vessel data from Danish waters (January 2021) as a substitute dataset.</li>
 *   <li><b>Line 2 (edwithin_tgeo_geo)</b>: Called inside {@link HighRiskZoneWindowFunction#process}
 *       for each event against each predefined hazard zone polygon (INPolygons). The distance
 *       threshold is set to 500 m for the AIS dataset (the paper uses 20 m for SNCB trains,
 *       see {@link #ALERT_DISTANCE_METERS}).</li>
 *   <li><b>Line 3 (10-second tumbling window)</b>: Implemented with
 *       {@code TumblingEventTimeWindows.of(Time.seconds(10))}, using the AIS message timestamp
 *       as event time.</li>
 *   <li><b>Line 4 (print sink)</b>: Alerts are emitted via {@code .print()} and also logged
 *       at WARN level by {@link HighRiskZoneWindowFunction}.</li>
 * </ul>
 *
 * <p><b>Geography type choice (tgeogpoint_in + geog_in):</b>
 * <br>Five alternative approaches were attempted for the zone polygons before reaching the
 * current solution:
 * <ol>
 *   <li>{@code geom_in("POLYGON(...)", -1)}: Creates a SRID=0 (planar) geometry.
 *       {@code edwithin_tgeo_geo} then computes distance in degrees instead of metres -
 *       500 degrees covers the entire globe, so every vessel matches every zone. <b>WRONG.</b></li>
 *   <li>{@code geom_in("SRID=4326;POLYGON(...)", -1)}: {@code geom_in} does not support the
 *       EWKT prefix format; it returns {@code null}, which causes a SIGSEGV in native code.
 *       <b>CRASH.</b></li>
 *   <li>{@code geom_from_hexewkb(hex)}: Zone is SRID=4326 geometry, but {@code tgeompoint_in}
 *       creates a SRID=0 point. MEOS crashes on the SRID mismatch inside
 *       {@code edwithin_tgeo_geo}. <b>CRASH.</b></li>
 *   <li>{@code tgeogpoint_in} + {@code geog_from_hexewkb(hex)}: Both types are SRID=4326
 *       geography, distances computed in metres. Works, but requires pre-encoding polygons
 *       as EWKB hex strings, which are verbose and error-prone. <b>CORRECT but complex.</b></li>
 *   <li>{@code tgeogpoint_in} + {@code geog_in("POLYGON(...)", -1)} ← <b>current approach</b>:
 *       {@code geog_in} is the geography counterpart of {@code geom_in}. Both the vessel point and the zone polygon are
 *       SRID=4326 geography types. <b>CORRECT and simplest.</b></li>
 * </ol>
 *
 * <p><b>HOW TO RUN</b>
 * <br>In the Dockerfile, change the entrypoint from {@code aisdata.Main} to
 * {@code aisdata.Query1_Main}, then run:
 * <pre>
 *   mvn clean package &amp;&amp; docker compose up --build
 * </pre>
 *
 * <p><b>TROUBLESHOOTING</b>
 * <ul>
 *   <li>MobilityDB {@code stable-1.3} or later could be required depending on your needs since
 *       {@code geom_in} is only exported by {@code libmeos.so} from stable-1.3 onwards for example:
 *       {@code https://github.com/MobilityDB/MobilityDB.git -b stable-1.3}</li>
 *   <li>Make sure you're using a JMEOS version which is compatible with the version of
 *       MobilityDB you're using:
 *       {@code --branch fix-tests-using-docker https://github.com/MobilityDB/JMEOS}</li>
 * </ul>
 */
public class Query1_Main {

    private static final Logger logger = LoggerFactory.getLogger(Query1_Main.class);

    /**
     * Distance threshold in metres used in the {@code edwithin_tgeo_geo} call (paper Line 2).
     *
     * <p>The paper specifies <b>20 metres</b> for the SNCB train dataset:
     * <i>"Any incoming temporal point within the specified distance of 20 meters"</i>.
     *
     * <p>This value is set to <b>500 metres</b> here because the AIS vessel dataset covers
     * open sea areas where exact polygon placement is less precise.
     */
    private static final double ALERT_DISTANCE_METERS = 500.0;

    /**
     * Predefined high-risk area polygons --> the <b>INPolygons</b> referenced in paper Line 2:
     * <i>"invoking edwithin_tgeo_geo on the SNCB stream against predefined high-risk area
     * polygons (INPolygons)"</i>.
     *
     * <p>Plain WKT strings parsed with {@code geog_in(wkt, -1)}, which creates a
     * <b>geography type (SRID=4326)</b>. This is the simplest viable approach:
     * {@code geog_in} is the geography counterpart of {@code geom_in}.
     *
     * <p>In the paper, INPolygons represent dangerous zones alongside SNCB railway lines.
     * Here they are placed over the known cluster positions of vessels in the AIS test dataset
     * (Danish waters, January 2021) to produce alerts with the substitute data.
     * Each polygon is a rectangular bounding box (5 coordinates, first = last to close the ring)
     * built around a representative position observed in {@code ais_instants.csv}.
     *
     * <p>Zones:
     *   ZONE 1 - Kattegat TSS        (MMSI 265513270, lon~12.27, lat~57.06)
     *   ZONE 2 - Limfjord / Aalborg  (MMSI 219001559, lon~9.98,  lat~57.59)
     *   ZONE 3 - Great Belt narrows  (MMSI 219027804, lon~12.06, lat~55.94)
     *   ZONE 4 - North Sea / Esbjerg (MMSI 566948000, lon~4.49-4.62, lat~55.57-55.64)
     */
    private static final String[] HIGH_RISK_ZONES_WKT = {

            // ZONE 1
            "POLYGON((12.2524 57.0390, 12.2524 57.0790, 12.2924 57.0790, 12.2924 57.0390, 12.2524 57.0390))",

            // ZONE 2
            "POLYGON((9.9555 57.5720, 9.9555 57.6120, 9.9955 57.6120, 9.9955 57.5720, 9.9555 57.5720))",

            // ZONE 3
            "POLYGON((11.9900 55.9200, 11.9900 55.9600, 12.0800 55.9600, 12.0800 55.9200, 11.9900 55.9200))",

            // ZONE 4
            "POLYGON((4.4800 55.5500, 4.4800 55.6600, 4.6400 55.6600, 4.6400 55.5500, 4.4800 55.5500))"
    };

    // -----------------------------------------------------------------------
    // Entry point
    // -----------------------------------------------------------------------

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
            properties.setProperty("group.id", "flink_consumer_q1");
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName());
            properties.setProperty("auto.offset.reset", "earliest");

            KafkaSource<AISData> kafkaSource = KafkaSource.<AISData>builder()
                    .setBootstrapServers("kafka:29092")
                    .setGroupId("flink_consumer_q1")
                    .setTopics("aisdata")
                    .setStartingOffsets(OffsetsInitializer.earliest())
                    .setValueOnlyDeserializer(new AISDataDeserializationSchema())
                    .build();

            // Event-time source with bounded out-of-orderness watermark strategy.
            // - withTimestampAssigner: tells Flink which field of AISData carries the event
            //   timestamp, enabling TumblingEventTimeWindows to assign each event to the
            //   correct 10-second window based on when the GPS coordinates were recorded, not when
            //   Flink received the Kafka message which would be TumblingProcessingTimeWindows.
            // - forBoundedOutOfOrderness(10s): Flink waits up to 10 seconds for late-arriving
            //   events before closing a window, tolerating reordering in the Kafka stream.
            // - withIdleness(1min): marks the source as idle after 1 minute without new events
            //   so that watermarks can still advance in other partitions instead of indefinitely waiting.
            DataStream<AISData> source = env
                    .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
                    .assignTimestampsAndWatermarks(
                            WatermarkStrategy.<AISData>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                    .withTimestampAssigner(
                                            (event, recordTs) -> event.getTimestamp())
                                    .withIdleness(Duration.ofMinutes(1)));

            // Pipeline implementing the MobilityNebula Query 1 pseudocode:
            //
            //   keyBy(getMmsi)          → paper Line 1: partitions the stream per vessel so that
            //                             each window contains events from a single MMSI only.
            //   window(Tumbling 10s)    → paper Line 3: groups events into non-overlapping
            //                             10-second event-time windows.
            //   process(WindowFunction) → paper Line 2: calls edwithin_tgeo_geo for each event
            //                             against each INPolygon; emits alerts on match.
            //   print()                 → paper Line 4: equivalent to PrintSinkDescriptor.
            source
                    .keyBy(AISData::getMmsi)
                    .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                    .process(new HighRiskZoneWindowFunction(
                            HIGH_RISK_ZONES_WKT, ALERT_DISTANCE_METERS))
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
                logger.error("Error during MEOS finalization: {}", e.getMessage(), e);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Window function
    // -----------------------------------------------------------------------

    /**
     * Flink {@link ProcessWindowFunction} that implements paper Lines 2–4 for a single
     * (MMSI, 10-second window) pair.
     *
     * <p>Called by Flink once per closed window. Receives all {@link AISData} events that
     * fell within the window's event-time boundaries for one vessel.
     */
    public static class HighRiskZoneWindowFunction
            extends ProcessWindowFunction<AISData, String, Integer, TimeWindow> {

        private static final Logger log =
                LoggerFactory.getLogger(HighRiskZoneWindowFunction.class);

        private final String[] zoneWkt;
        private final double distanceMeters;

        private transient Pointer[] hazardZones;

        private transient error_handler_fn errorHandler;

        private static final DateTimeFormatter TIMESTAMP_FMT =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssxxx");

        public HighRiskZoneWindowFunction(String[] zoneWkt, double distanceMeters) {
            this.zoneWkt = zoneWkt;
            this.distanceMeters = distanceMeters;
        }

        /** Initialises the MEOS library and the hazard zones for this operator instance. */
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            errorHandler = new error_handler();
            functions.meos_initialize_timezone("UTC");
            functions.meos_initialize_error_handler(errorHandler);
            // Parse the INPolygons only once per worker.
            // geog_in(wkt, -1) creates a geography type (SRID=4326), so
            // edwithin_tgeo_geo computes distances geodetically in metres - consistent
            // with the tgeogpoint created below via tgeogpoint_in.
            this.hazardZones = new Pointer[zoneWkt.length];
            for (int i = 0; i < zoneWkt.length; i++) {
                hazardZones[i] = functions.geog_in(zoneWkt[i], -1);
                if (hazardZones[i] == null) {
                    log.error("geog_in returned null for ZONE {}", i + 1);
                }
            }
            log.info("MEOS initialized in HighRiskZoneWindowFunction.open(), {} hazard zones parsed", hazardZones.length);
        }

        /**
         * Evaluates paper Lines 2–4 for all events in a 10-second tumbling window.
         *
         * <p><b>Paper Line 2</b> - {@code edwithin_tgeo_geo(lon, lat, ts, INPolygons, 20) == 1}:
         * <br>For each vessel event in the window, a {@code tgeogpoint} instant is constructed
         * via {@code tgeogpoint_in} and tested against every INPolygon via
         * {@code edwithin_tgeo_geo}. A return value of {@code 1} means the vessel is within
         * {@link #distanceMeters} of the zone boundary - an alert is emitted.
         *
         * <p><b>Paper Line 3</b> - {@code TumblingWindow::of(EventTime(ts), Seconds(10))}:
         * <br>Managed by Flink upstream; this method is called once per (MMSI, window) pair
         * after the window closes.
         *
         * <p><b>Paper Line 4</b> - {@code PrintSinkDescriptor::create()}:
         * <br>Alerts are forwarded to the print sink via {@code out.collect(alert)} and also
         * logged at WARN level for visibility in the container log.
         *
         * @param mmsi     vessel identifier (the key used by keyBy)
         * @param context  window metadata (start/end timestamps)
         * @param elements all AISData events in this (MMSI, window) pair
         * @param out      collector for alert strings forwarded to the print sink
         */
        @Override
        public void process(
                Integer mmsi,
                Context context,
                Iterable<AISData> elements,
                Collector<String> out) {


            String windowStart = millisToTimestamp(context.window().getStart());
            String windowEnd   = millisToTimestamp(context.window().getEnd());

            for (AISData event : elements) {

                String ts = millisToTimestamp(event.getTimestamp());

                // Build a temporal geography point instant: "POINT(lon lat)@timestamp".
                // tgeogpoint_in (not tgeompoint_in) is critical: tgeompoint_in would create
                // a SRID=0 geometry, causing a MEOS crash on SRID mismatch with the
                // SRID=4326 geography zone above.
                String tpointWkt = String.format(
                        "POINT(%f %f)@%s", event.getLon(), event.getLat(), ts);

                Pointer tpoint = functions.tgeogpoint_in(tpointWkt);
                if (tpoint == null) {
                    log.error("tgeogpoint_in returned null for WKT: {}", tpointWkt);
                    continue;
                }

                for (int i = 0; i < hazardZones.length; i++) {
                    if (hazardZones[i] == null) continue;

                    // edwithin_tgeo_geo returns 1 if tpoint is within distanceMeters of the
                    // zone polygon at any instant — implements paper Line 2.
                    int within = functions.edwithin_tgeo_geo(
                            tpoint, hazardZones[i], distanceMeters);

                    if (within == 1) {
                        String alert = String.format(
                                "[ALERT][Q1] MMSI=%-12d | lon=%10.5f lat=%9.5f"
                                        + " | ts=%s | within %.0f m of ZONE %d"
                                        + " | window [%s - %s]",
                                mmsi,
                                event.getLon(), event.getLat(),
                                ts,
                                distanceMeters,
                                i + 1,
                                windowStart, windowEnd);

                        log.warn(alert);
                        out.collect(alert); // paper Line 4: forwarded to print sink
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