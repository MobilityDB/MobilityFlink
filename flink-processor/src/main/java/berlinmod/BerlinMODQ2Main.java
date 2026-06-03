package berlinmod;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Entry point for the BerlinMOD-Q2 scaffold on MobilityFlink.
 *
 * <p>Runs the three streaming forms of BerlinMOD-Q2 ("where is vehicle X at
 * time T?") side by side over the same Kafka input topic {@code berlinmod}:
 * <ul>
 *   <li>{@link Q2ContinuousFunction} — emit every event of vehicle X as it arrives</li>
 *   <li>{@link Q2WindowedFunction}   — last-known (lon, lat) of vehicle X per N-second tumbling window</li>
 *   <li>{@link Q2SnapshotFunction}   — vehicle X's last-known (lon, lat) at each watermark tick;
 *       the parity-oracle form (≡ batch BerlinMOD-Q2 at the same scale factor)</li>
 * </ul>
 *
 * <p>The queried vehicle id and other defaults match
 * {@code doc/berlinmod-q3-streaming-forms.md}. The companion local test driver
 * is {@link BerlinMODQ2LocalTest}.
 */
public class BerlinMODQ2Main {

    private static final Logger LOG = LoggerFactory.getLogger(BerlinMODQ2Main.class);

    // Default Q2 parameters — query vehicle 200 (Anderlecht), 10 s windows,
    // 5 s snapshot tick. Matches the synthetic-corpus defaults.
    private static final int TARGET_VEHICLE_ID = 200;
    private static final long WINDOW_SIZE_SECONDS = 10L;
    private static final long SNAPSHOT_TICK_MILLIS = 5_000L;
    private static final String KAFKA_TOPIC = "berlinmod";
    private static final String KAFKA_BOOTSTRAP = "kafka:29092";
    private static final String CONSUMER_GROUP = "flink_berlinmod_q2";

    public static void main(String[] args) throws Exception {
        LOG.info("BerlinMODQ2Main starting; X={} window={}s tick={}ms",
                TARGET_VEHICLE_ID, WINDOW_SIZE_SECONDS, SNAPSHOT_TICK_MILLIS);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP)
                .setGroupId(CONSUMER_GROUP)
                .setTopics(KAFKA_TOPIC)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> raw = env.fromSource(
                kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source (berlinmod)");

        DataStream<BerlinMODTrip> trips = raw
                .map(new DeserializeBerlinMODMapFunction())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<BerlinMODTrip>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner(new BerlinMODTimestampAssigner())
                                .withIdleness(Duration.ofMinutes(1))
                );

        // Continuous form — per-event pass-through for the queried vehicle
        DataStream<BerlinMODTrip> continuous = trips
                .process(new Q2ContinuousFunction(TARGET_VEHICLE_ID));
        continuous.print("Q2-continuous");

        // Windowed form — last-known (lon, lat) per tumbling window
        DataStream<Tuple5<Long, Long, Integer, Double, Double>> windowed = trips
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(WINDOW_SIZE_SECONDS)))
                .process(new Q2WindowedFunction(TARGET_VEHICLE_ID));
        windowed.print("Q2-windowed");

        // Snapshot form — keyed by vehicle, emits queried vehicle's last position at each tick
        DataStream<Tuple4<Long, Double, Double, Long>> snapshot = trips
                .keyBy(BerlinMODTrip::getVehicleId)
                .process(new Q2SnapshotFunction(TARGET_VEHICLE_ID, SNAPSHOT_TICK_MILLIS));
        snapshot.print("Q2-snapshot");

        env.execute("BerlinMOD-Q2 (continuous / windowed / snapshot)");
        LOG.info("BerlinMODQ2Main done");
    }

    public static class DeserializeBerlinMODMapFunction implements MapFunction<String, BerlinMODTrip> {
        @Override
        public BerlinMODTrip map(String value) throws Exception {
            return new BerlinMODDeserializationSchema().deserialize(value.getBytes());
        }
    }

    public static class BerlinMODTimestampAssigner implements SerializableTimestampAssigner<BerlinMODTrip> {
        @Override
        public long extractTimestamp(BerlinMODTrip element, long recordTimestamp) {
            return element.getTimestamp();
        }
    }
}
