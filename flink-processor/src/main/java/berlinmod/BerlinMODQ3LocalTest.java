package berlinmod;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Local end-to-end test driver for the BerlinMOD-Q3 three streaming forms.
 *
 * <p>Runs the same three form functions {@link BerlinMODQ3Main} runs (continuous,
 * windowed, snapshot) but reads from a hardcoded synthetic event list via
 * {@code env.fromCollection(...)} instead of from Kafka. This lets the scaffold
 * be verified on any machine with Java + Maven, without Docker, a Kafka broker,
 * the MEOS native lib, or any JMEOS call.
 *
 * <p>Synthetic corpus: 3 vehicles, 21 events over 14 simulated seconds —
 * <ul>
 *   <li><b>Vehicle 100</b> — sits on Brussels city centre {@code P}, distance 0 m, <b>near</b></li>
 *   <li><b>Vehicle 200</b> — Anderlecht, ~4.1 km from {@code P}, <b>near</b> (within the 5 km radius)</li>
 *   <li><b>Vehicle 300</b> — Forest, ~15.4 km from {@code P}, <b>not near</b> (outside the 5 km radius)</li>
 * </ul>
 *
 * <p>Expected output shape:
 * <ul>
 *   <li><b>Q3-continuous</b>: 21 lines, {@code near=true} for vehicles 100 and 200, {@code false} for 300</li>
 *   <li><b>Q3-windowed</b>: 2 windows of size 10 s, each with {@code distinctCount=2} (vehicles 100 and 200)</li>
 *   <li><b>Q3-snapshot</b>: 3 ticks × 2 near vehicles = 6 lines (vehicles 100 and 200 at each of the three 5 s ticks)</li>
 * </ul>
 *
 * <p>Run after {@code mvn package} with:
 * <pre>
 *   java -cp target/flink-kafka2postgres-1.0-SNAPSHOT.jar berlinmod.BerlinMODQ3LocalTest
 * </pre>
 */
public class BerlinMODQ3LocalTest {

    private static final Logger LOG = LoggerFactory.getLogger(BerlinMODQ3LocalTest.class);

    private static final double P_LON = 4.3517;
    private static final double P_LAT = 50.8503;
    private static final double RADIUS_METRES = 5_000.0;
    private static final long WINDOW_SIZE_SECONDS = 10L;
    private static final long SNAPSHOT_TICK_MILLIS = 5_000L;
    private static final long T0 = 1_735_711_200_000L; // 2025-01-01 06:00:00 UTC

    public static void main(String[] args) throws Exception {
        System.setProperty("mobilityflink.meos.enabled", "false");
        LOG.info("BerlinMODQ3LocalTest starting; P=({}, {}) radius={}m window={}s tick={}ms",
                P_LON, P_LAT, RADIUS_METRES, WINDOW_SIZE_SECONDS, SNAPSHOT_TICK_MILLIS);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // deterministic output ordering for the test

        List<BerlinMODTrip> events = buildEvents();
        DataStreamSource<BerlinMODTrip> raw = env.fromCollection(events);
        DataStream<BerlinMODTrip> trips = raw.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<BerlinMODTrip>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((e, t) -> e.getTimestamp()));

        DataStream<Tuple3<Integer, Long, Boolean>> cont = trips
                .process(new Q3ContinuousFunction(P_LON, P_LAT, RADIUS_METRES));
        cont.print("Q3-continuous");

        DataStream<Tuple3<Long, Long, Long>> win = trips
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(WINDOW_SIZE_SECONDS)))
                .process(new Q3WindowedFunction(P_LON, P_LAT, RADIUS_METRES));
        win.print("Q3-windowed");

        DataStream<Tuple2<Long, Integer>> snap = trips
                .keyBy(BerlinMODTrip::getVehicleId)
                .process(new Q3SnapshotFunction(P_LON, P_LAT, RADIUS_METRES, SNAPSHOT_TICK_MILLIS));
        snap.print("Q3-snapshot");

        env.execute("BerlinMODQ3LocalTest");
        LOG.info("BerlinMODQ3LocalTest done");
    }

    private static List<BerlinMODTrip> buildEvents() {
        List<BerlinMODTrip> events = new ArrayList<>();
        // Vehicle 100 — Brussels city centre (= P), 7 events at t0, t0+2s, …, t0+12s
        for (int i = 0; i <= 12; i += 2) {
            events.add(make(100, T0 + i * 1000L, 4.3517, 50.8503));
        }
        // Vehicle 200 — Anderlecht ~4.1 km from P, 7 events at t0+1s, t0+3s, …, t0+13s
        for (int i = 1; i <= 13; i += 2) {
            events.add(make(200, T0 + i * 1000L, 4.3060, 50.8270));
        }
        // Vehicle 300 — Forest ~15.4 km from P, 7 events at t0, t0+2s, …, t0+12s
        for (int i = 0; i <= 12; i += 2) {
            events.add(make(300, T0 + i * 1000L, 4.2000, 50.7500));
        }
        return events;
    }

    private static BerlinMODTrip make(int vid, long t, double lon, double lat) {
        BerlinMODTrip trip = new BerlinMODTrip();
        trip.setVehicleId(vid);
        trip.setTimestamp(t);
        trip.setLon(lon);
        trip.setLat(lat);
        return trip;
    }
}
