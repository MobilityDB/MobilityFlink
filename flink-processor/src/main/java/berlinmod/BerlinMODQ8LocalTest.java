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
 * Local end-to-end test driver for the BerlinMOD-Q8 three streaming forms.
 *
 * <p>Same stationary-vehicle corpus as the other Qs. Road segment runs
 * from (4.30, 50.83) to (4.36, 50.87) — a diagonal across the Brussels-
 * centre region. With a {@code d = 5 km} proximity threshold:
 *
 * <ul>
 *   <li><b>v100</b> at (4.3517, 50.8503) — ~1.1 km from segment → <b>near</b></li>
 *   <li><b>v200</b> at (4.3060, 50.8270) — ~0.5 km from segment → <b>near</b></li>
 *   <li><b>v300</b> at (4.2000, 50.7500) — ~13 km from segment → <b>not near</b></li>
 * </ul>
 *
 * <p>Expected output shape:
 * <ul>
 *   <li><b>Q8-continuous</b>: 21 events (14 near=true for v100/v200, 7 near=false for v300)</li>
 *   <li><b>Q8-windowed</b>: 2 windows, each with {@code distinctCount=2} (vehicles 100 and 200)</li>
 *   <li><b>Q8-snapshot</b>: 3 ticks × 2 near vehicles = 6 emissions</li>
 * </ul>
 *
 * <p>Same shape as Q3 with a segment-distance predicate substituted for the
 * point-radius one — the algebraic pattern parity intentional.
 */
public class BerlinMODQ8LocalTest {

    private static final Logger LOG = LoggerFactory.getLogger(BerlinMODQ8LocalTest.class);

    // Road segment endpoints
    private static final double S1_LON = 4.30, S1_LAT = 50.83;
    private static final double S2_LON = 4.36, S2_LAT = 50.87;
    private static final double RADIUS_METRES = 5_000.0;
    private static final long WINDOW_SIZE_SECONDS = 10L;
    private static final long SNAPSHOT_TICK_MILLIS = 5_000L;
    private static final long T0 = 1_735_711_200_000L;

    public static void main(String[] args) throws Exception {
        System.setProperty("mobilityflink.meos.enabled", "false");
        LOG.info("BerlinMODQ8LocalTest starting; segment=({},{}) → ({},{}) d={}m",
                S1_LON, S1_LAT, S2_LON, S2_LAT, RADIUS_METRES);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<BerlinMODTrip> events = buildEvents();
        DataStreamSource<BerlinMODTrip> raw = env.fromCollection(events);
        DataStream<BerlinMODTrip> trips = raw.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<BerlinMODTrip>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((e, t) -> e.getTimestamp()));

        DataStream<Tuple3<Integer, Long, Boolean>> cont = trips
                .process(new Q8ContinuousFunction(S1_LON, S1_LAT, S2_LON, S2_LAT, RADIUS_METRES));
        cont.print("Q8-continuous");

        DataStream<Tuple3<Long, Long, Long>> win = trips
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(WINDOW_SIZE_SECONDS)))
                .process(new Q8WindowedFunction(S1_LON, S1_LAT, S2_LON, S2_LAT, RADIUS_METRES));
        win.print("Q8-windowed");

        DataStream<Tuple2<Long, Integer>> snap = trips
                .keyBy(BerlinMODTrip::getVehicleId)
                .process(new Q8SnapshotFunction(S1_LON, S1_LAT, S2_LON, S2_LAT, RADIUS_METRES, SNAPSHOT_TICK_MILLIS));
        snap.print("Q8-snapshot");

        env.execute("BerlinMODQ8LocalTest");
        LOG.info("BerlinMODQ8LocalTest done");
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
