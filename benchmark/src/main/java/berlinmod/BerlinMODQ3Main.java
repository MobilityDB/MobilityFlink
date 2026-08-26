/*****************************************************************************
 *
 * This MobilityDB code is provided under The PostgreSQL License.
 * Copyright (c) 2020-2026, Université libre de Bruxelles and MobilityDB
 * contributors
 *
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose, without fee, and without a written
 * agreement is hereby granted, provided that the above copyright notice and
 * this paragraph and the following two paragraphs appear in all copies.
 *
 * IN NO EVENT SHALL UNIVERSITE LIBRE DE BRUXELLES BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
 * LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
 * EVEN IF UNIVERSITE LIBRE DE BRUXELLES HAS BEEN ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 * UNIVERSITE LIBRE DE BRUXELLES SPECIFICALLY DISCLAIMS ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON
 * AN "AS IS" BASIS, AND UNIVERSITE LIBRE DE BRUXELLES HAS NO OBLIGATIONS TO
 * PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 *
 *****************************************************************************/

package berlinmod;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Entry point for the BerlinMOD-Q3 scaffold on MobilityFlink.
 *
 * <p>Runs the three streaming forms of BerlinMOD-Q3 side by side over the same
 * Kafka input topic {@code berlinmod}:
 * <ul>
 *   <li>{@link Q3ContinuousFunction} — per-event near/not-near</li>
 *   <li>{@link Q3WindowedFunction}   — distinct-count per N-second tumbling window</li>
 *   <li>{@link Q3SnapshotFunction}   — set of vehicles near P at each watermark tick
 *       (the parity-oracle form; ≡ batch BerlinMOD-Q3 at the same scale factor)</li>
 * </ul>
 *
 * <p>Reference point P, radius {@code d}, window size and snapshot tick are
 * the hardcoded defaults from the BerlinMOD-Q3 streaming-forms spec (see
 * {@code doc/berlinmod-q3-streaming-forms.md}). The Kafka producer is the
 * companion {@code kafka-producer/python-producer-berlinmod.py}.
 */
public class BerlinMODQ3Main {

    private static final Logger LOG = LoggerFactory.getLogger(BerlinMODQ3Main.class);

    // Default Q3 parameters — the canonical sample area, 5 km radius, 10 s windows,
    // 5 s snapshot tick. Matches the defaults in the spec doc.
    private static final double P_LON = 4.4322;   // near canonical vehicle 1
    private static final double P_LAT = 50.7670;
    private static final double RADIUS_METRES = 5_000.0;
    private static final long WINDOW_SIZE_SECONDS = 10L;
    private static final long SNAPSHOT_TICK_MILLIS = 5_000L;
    private static final String KAFKA_TOPIC = "berlinmod";
    private static final String KAFKA_BOOTSTRAP = "kafka:29092";
    private static final String CONSUMER_GROUP = "flink_berlinmod_q3";

    public static void main(String[] args) throws Exception {
        LOG.info("BerlinMODQ3Main starting; P=({}, {}) radius={}m window={}s tick={}ms",
                P_LON, P_LAT, RADIUS_METRES, WINDOW_SIZE_SECONDS, SNAPSHOT_TICK_MILLIS);

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

        // Continuous form — per-event near/not-near
        DataStream<Tuple3<Integer, Long, Boolean>> continuous = trips
                .process(new Q3ContinuousFunction(P_LON, P_LAT, RADIUS_METRES));
        continuous.print("Q3-continuous");

        // Windowed form — distinct count per tumbling window
        DataStream<Tuple3<Long, Long, Long>> windowed = trips
                .windowAll(TumblingEventTimeWindows.of(Duration.ofSeconds(WINDOW_SIZE_SECONDS)))
                .process(new Q3WindowedFunction(P_LON, P_LAT, RADIUS_METRES));
        windowed.print("Q3-windowed");

        // Snapshot form — keyed by vehicle, emits set of vehicles near P at each tick
        DataStream<Tuple2<Long, Integer>> snapshot = trips
                .keyBy(BerlinMODTrip::getVehicleId)
                .process(new Q3SnapshotFunction(P_LON, P_LAT, RADIUS_METRES, SNAPSHOT_TICK_MILLIS));
        snapshot.print("Q3-snapshot");

        env.execute("BerlinMOD-Q3 (continuous / windowed / snapshot)");
        LOG.info("BerlinMODQ3Main done");
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
