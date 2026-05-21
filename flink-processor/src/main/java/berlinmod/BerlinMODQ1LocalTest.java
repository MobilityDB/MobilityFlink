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
 * Local end-to-end test driver for the BerlinMOD-Q1 three streaming forms.
 *
 * <p>Same 21-event synthetic corpus as Q2/Q3 local tests. Q1 has no spatial
 * predicate and no per-event filter parameter — it simply enumerates vehicles
 * seen.
 *
 * <p>Expected output:
 * <ul>
 *   <li><b>Q1-continuous</b>: 3 lines, one per distinct vehicle (firstSeenTime)</li>
 *   <li><b>Q1-windowed</b>: 2 windows, each with distinctCount=3</li>
 *   <li><b>Q1-snapshot</b>: 9 lines (3 ticks × 3 vehicles all seen by source-close)</li>
 * </ul>
 */
public class BerlinMODQ1LocalTest {

    private static final Logger LOG = LoggerFactory.getLogger(BerlinMODQ1LocalTest.class);

    private static final long WINDOW_SIZE_SECONDS = 10L;
    private static final long SNAPSHOT_TICK_MILLIS = 5_000L;
    private static final long T0 = 1_735_711_200_000L;

    public static void main(String[] args) throws Exception {
        System.setProperty("mobilityflink.meos.enabled", "false");
        LOG.info("BerlinMODQ1LocalTest starting; window={}s tick={}ms",
                WINDOW_SIZE_SECONDS, SNAPSHOT_TICK_MILLIS);

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
                .process(new Q1ContinuousFunction());
        cont.print("Q1-continuous");

        DataStream<Tuple3<Long, Long, Long>> win = trips
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(WINDOW_SIZE_SECONDS)))
                .process(new Q1WindowedFunction());
        win.print("Q1-windowed");

        DataStream<Tuple2<Long, Integer>> snap = trips
                .keyBy(BerlinMODTrip::getVehicleId)
                .process(new Q1SnapshotFunction(SNAPSHOT_TICK_MILLIS));
        snap.print("Q1-snapshot");

        env.execute("BerlinMODQ1LocalTest");
        LOG.info("BerlinMODQ1LocalTest done");
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
