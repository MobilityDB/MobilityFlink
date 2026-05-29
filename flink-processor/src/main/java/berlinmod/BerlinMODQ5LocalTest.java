package berlinmod;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
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
 * Local end-to-end test driver for the BerlinMOD-Q5 three streaming forms.
 *
 * <p>Same stationary-vehicle corpus as Q1/Q2/Q3/Q9. Reference point P =
 * Brussels city centre (4.3517, 50.8503); {@code dP = 5 km} (vehicle near P);
 * {@code dMeet = 5 km} (pair-meeting threshold).
 *
 * <p>Pairs:
 * <ul>
 *   <li><b>(100, 200)</b> — both near P; distance 4.1 km ≤ dMeet → <b>MEET</b></li>
 *   <li>(100, 300) — v300 not near P → don't qualify</li>
 *   <li>(200, 300) — v300 not near P → don't qualify</li>
 * </ul>
 *
 * <p>Expected output (only the (100, 200) pair qualifies):
 * <ul>
 *   <li><b>Q5-continuous</b>: pair (100, 200) emits on every event from t=1
 *       onward (the first t=0 events of v100 and v300 happen before v200 is
 *       known, so no pair exists yet). 21 - 2 = 19 emissions.</li>
 *   <li><b>Q5-windowed</b>: each of the two 10-second windows contains
 *       events for v100 and v200 — both qualify, the pair meets. 2 emissions.</li>
 *   <li><b>Q5-snapshot</b>: 3 ticks × 1 meeting pair = 3 emissions.</li>
 * </ul>
 */
public class BerlinMODQ5LocalTest {

    private static final Logger LOG = LoggerFactory.getLogger(BerlinMODQ5LocalTest.class);

    private static final double P_LON = 4.3517;
    private static final double P_LAT = 50.8503;
    private static final double D_P_METRES = 5_000.0;
    private static final double D_MEET_METRES = 5_000.0;
    private static final long WINDOW_SIZE_SECONDS = 10L;
    private static final long SNAPSHOT_TICK_MILLIS = 5_000L;
    private static final long T0 = 1_735_711_200_000L;

    public static void main(String[] args) throws Exception {
        LOG.info("BerlinMODQ5LocalTest starting; P=({}, {}) dP={}m dMeet={}m",
                P_LON, P_LAT, D_P_METRES, D_MEET_METRES);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<BerlinMODTrip> events = buildEvents();
        DataStreamSource<BerlinMODTrip> raw = env.fromCollection(events);
        DataStream<BerlinMODTrip> trips = raw.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<BerlinMODTrip>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((e, t) -> e.getTimestamp()));

        DataStream<Tuple4<Integer, Integer, Long, Double>> cont = trips
                .keyBy(t -> 0)
                .process(new Q5ContinuousFunction(P_LON, P_LAT, D_P_METRES, D_MEET_METRES));
        cont.print("Q5-continuous");

        DataStream<Tuple5<Long, Long, Integer, Integer, Double>> win = trips
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(WINDOW_SIZE_SECONDS)))
                .process(new Q5WindowedFunction(P_LON, P_LAT, D_P_METRES, D_MEET_METRES));
        win.print("Q5-windowed");

        DataStream<Tuple4<Long, Integer, Integer, Double>> snap = trips
                .keyBy(t -> 0)
                .process(new Q5SnapshotFunction(P_LON, P_LAT, D_P_METRES, D_MEET_METRES, SNAPSHOT_TICK_MILLIS));
        snap.print("Q5-snapshot");

        env.execute("BerlinMODQ5LocalTest");
        LOG.info("BerlinMODQ5LocalTest done");
    }

    private static List<BerlinMODTrip> buildEvents() {
        List<BerlinMODTrip> events = new ArrayList<>();
        for (int i = 0; i <= 12; i += 2) {
            events.add(make(100, T0 + i * 1000L, 4.3517, 50.8503));
        }
        for (int i = 1; i <= 13; i += 2) {
            events.add(make(200, T0 + i * 1000L, 4.3060, 50.8270));
        }
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
