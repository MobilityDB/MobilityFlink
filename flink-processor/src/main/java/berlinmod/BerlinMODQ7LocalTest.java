package berlinmod;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
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
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

/**
 * Local end-to-end test driver for the BerlinMOD-Q7 three streaming forms.
 *
 * <p>Same stationary-vehicle corpus as Q1/Q2/Q3/Q5/Q9. POI list:
 * <ul>
 *   <li>POI 1 = Brussels city centre (4.3517, 50.8503), radius 2000 m</li>
 *   <li>POI 2 = Anderlecht (4.3060, 50.8270), radius 1000 m</li>
 *   <li>POI 3 = south of Brussels (4.2100, 50.7600), radius 2000 m</li>
 * </ul>
 *
 * <p>Per (vehicle, POI) match-up:
 * <ul>
 *   <li>v100 is inside POI 1 (0 m), outside POI 2 (~4.1 km) and POI 3 (~13 km)</li>
 *   <li>v200 is inside POI 2 (0 m), outside POI 1 and POI 3</li>
 *   <li>v300 is inside POI 3 (~1.3 km), outside POI 1 and POI 2</li>
 * </ul>
 *
 * <p>Expected output:
 * <ul>
 *   <li><b>Q7-continuous</b>: 3 emissions — first-passages on each vehicle's
 *       very first event (v100 t=0 → POI 1; v200 t=1 → POI 2; v300 t=0 →
 *       POI 3)</li>
 *   <li><b>Q7-windowed</b>: per-window intra-window first-passages —
 *       window [0, 10 s) sees all 3 first-passages; window [10, 20 s) sees
 *       all 3 again (intra-window scoping has no cross-window memory). 6 lines.</li>
 *   <li><b>Q7-snapshot</b>: 3 ticks × 3 cumulative (vehicle, POI) first-passages = 9 lines</li>
 * </ul>
 */
public class BerlinMODQ7LocalTest {

    private static final Logger LOG = LoggerFactory.getLogger(BerlinMODQ7LocalTest.class);

    private static final long WINDOW_SIZE_SECONDS = 10L;
    private static final long SNAPSHOT_TICK_MILLIS = 5_000L;
    private static final long T0 = 1_735_711_200_000L;

    private static final List<PointOfInterest> POIS = Arrays.asList(
            new PointOfInterest(1, 4.3517, 50.8503, 2_000.0),
            new PointOfInterest(2, 4.3060, 50.8270, 1_000.0),
            new PointOfInterest(3, 4.2100, 50.7600, 2_000.0));

    public static void main(String[] args) throws Exception {
        LOG.info("BerlinMODQ7LocalTest starting; #POIs={} window={}s tick={}ms",
                POIS.size(), WINDOW_SIZE_SECONDS, SNAPSHOT_TICK_MILLIS);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<BerlinMODTrip> events = buildEvents();
        DataStreamSource<BerlinMODTrip> raw = env.fromCollection(events);
        DataStream<BerlinMODTrip> trips = raw.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<BerlinMODTrip>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((e, t) -> e.getTimestamp()));

        DataStream<Tuple3<Integer, Integer, Long>> cont = trips
                .keyBy(BerlinMODTrip::getVehicleId)
                .process(new Q7ContinuousFunction(POIS));
        cont.print("Q7-continuous");

        DataStream<Tuple5<Long, Long, Integer, Integer, Long>> win = trips
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(WINDOW_SIZE_SECONDS)))
                .process(new Q7WindowedFunction(POIS));
        win.print("Q7-windowed");

        DataStream<Tuple4<Long, Integer, Integer, Long>> snap = trips
                .keyBy(BerlinMODTrip::getVehicleId)
                .process(new Q7SnapshotFunction(POIS, SNAPSHOT_TICK_MILLIS));
        snap.print("Q7-snapshot");

        env.execute("BerlinMODQ7LocalTest");
        LOG.info("BerlinMODQ7LocalTest done");
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
