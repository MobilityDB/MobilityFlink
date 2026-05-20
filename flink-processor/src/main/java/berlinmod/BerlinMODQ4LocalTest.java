package berlinmod;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
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
 * Local end-to-end test driver for the BerlinMOD-Q4 three streaming forms.
 *
 * <p>Region R = bounding box {@code (4.30, 50.84, 4.36, 50.86)} — a rectangle
 * around Brussels city centre. The synthetic corpus is designed to produce
 * <i>multiple</i> outside → inside transitions so the entry-detection logic
 * is exercised non-trivially:
 *
 * <ul>
 *   <li><b>Vehicle 100</b> sits inside R for all 7 events (no transitions).</li>
 *   <li><b>Vehicle 200</b> oscillates: outside at t=1, inside at t=3 (entry),
 *       outside at t=5, inside at t=7 (entry), outside at t=9, inside at
 *       t=11 (entry), outside at t=13 → <b>three entries</b>.</li>
 *   <li><b>Vehicle 300</b> stays in Forest (outside R) for all 7 events.</li>
 * </ul>
 *
 * <p>Expected output:
 * <ul>
 *   <li><b>Q4-continuous</b>: 3 entries (v200's three outside → inside transitions)</li>
 *   <li><b>Q4-windowed</b>: per the intra-window scoping convention — window
 *       [0, 10 s) contains v100's first-seen-inside event AND v200's two entries
 *       (t=3, t=7); window [10, 20 s) contains v100's first-event-in-window
 *       AND v200's third entry (t=11). 5 emissions total.</li>
 *   <li><b>Q4-snapshot</b>: cumulative entries up to each tick. Tick 5: 1
 *       (v200 t=3). Tick 10: 2 (v200 t=3, t=7). Tick 15: 3 (v200 t=3, t=7,
 *       t=11). v100 contributes 0 (always inside, no transition). v300
 *       contributes 0. 6 emissions total (1+2+3).</li>
 * </ul>
 */
public class BerlinMODQ4LocalTest {

    private static final Logger LOG = LoggerFactory.getLogger(BerlinMODQ4LocalTest.class);

    // Region R — Brussels centre rectangle
    private static final double XMIN = 4.30;
    private static final double YMIN = 50.84;
    private static final double XMAX = 4.36;
    private static final double YMAX = 50.86;

    private static final long WINDOW_SIZE_SECONDS = 10L;
    private static final long SNAPSHOT_TICK_MILLIS = 5_000L;
    private static final long T0 = 1_735_711_200_000L;

    public static void main(String[] args) throws Exception {
        LOG.info("BerlinMODQ4LocalTest starting; R=({},{},{},{}) window={}s tick={}ms",
                XMIN, YMIN, XMAX, YMAX, WINDOW_SIZE_SECONDS, SNAPSHOT_TICK_MILLIS);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<BerlinMODTrip> events = buildEvents();
        DataStreamSource<BerlinMODTrip> raw = env.fromCollection(events);
        DataStream<BerlinMODTrip> trips = raw.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<BerlinMODTrip>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((e, t) -> e.getTimestamp()));

        DataStream<Tuple2<Integer, Long>> cont = trips
                .keyBy(BerlinMODTrip::getVehicleId)
                .process(new Q4ContinuousFunction(XMIN, YMIN, XMAX, YMAX));
        cont.print("Q4-continuous");

        DataStream<Tuple4<Long, Long, Integer, Long>> win = trips
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(WINDOW_SIZE_SECONDS)))
                .process(new Q4WindowedFunction(XMIN, YMIN, XMAX, YMAX));
        win.print("Q4-windowed");

        DataStream<Tuple3<Long, Integer, Long>> snap = trips
                .keyBy(BerlinMODTrip::getVehicleId)
                .process(new Q4SnapshotFunction(XMIN, YMIN, XMAX, YMAX, SNAPSHOT_TICK_MILLIS));
        snap.print("Q4-snapshot");

        env.execute("BerlinMODQ4LocalTest");
        LOG.info("BerlinMODQ4LocalTest done");
    }

    private static List<BerlinMODTrip> buildEvents() {
        List<BerlinMODTrip> events = new ArrayList<>();
        // v100 always inside R
        for (int i = 0; i <= 12; i += 2) {
            events.add(make(100, T0 + i * 1000L, 4.3517, 50.8503));
        }
        // v200 oscillates in/out: out, IN, out, IN, out, IN, out
        double[][] v200Path = {
                {4.3060, 50.8270}, // t=1  out (lat<50.84)
                {4.3060, 50.8500}, // t=3  IN
                {4.3060, 50.8300}, // t=5  out
                {4.3060, 50.8500}, // t=7  IN
                {4.3060, 50.8100}, // t=9  out
                {4.3060, 50.8500}, // t=11 IN
                {4.3060, 50.8300}, // t=13 out
        };
        int idx = 0;
        for (int i = 1; i <= 13; i += 2, idx++) {
            events.add(make(200, T0 + i * 1000L, v200Path[idx][0], v200Path[idx][1]));
        }
        // v300 always outside R
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
