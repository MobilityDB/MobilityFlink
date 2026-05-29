package berlinmod;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

/**
 * Throughput benchmark for the BerlinMOD-9 × 3-form streaming matrix.
 *
 * <p>Runs all 27 cells (9 queries × {continuous, windowed, snapshot}) on the
 * Flink local mini-cluster over a scaled synthetic BerlinMOD corpus, with the
 * spatial predicates evaluating through MEOS (see {@link MEOSBridge}). Each cell
 * runs its own job terminated by a counting sink; the harness records input
 * events, output rows, wall-clock, and throughput (events per second), then
 * prints a Markdown results table.
 *
 * <p>Usage (from {@code flink-processor/}, with an extended libmeos on the
 * loader path):
 * <pre>
 *   LD_LIBRARY_PATH=&lt;libmeos-dir&gt; java \
 *     --add-opens=java.base/java.lang=ALL-UNNAMED \
 *     --add-opens=java.base/java.util=ALL-UNNAMED \
 *     --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
 *     --add-opens=java.base/java.io=ALL-UNNAMED \
 *     --add-opens=java.base/java.time=ALL-UNNAMED \
 *     -cp target/classes:jar/JMEOS.jar:&lt;deps&gt; \
 *     berlinmod.BerlinMODBenchmark [numVehicles] [eventsPerVehicle]
 * </pre>
 */
public final class BerlinMODBenchmark {

    // Query parameters — identical to the BerlinMODQ*LocalTest drivers.
    private static final double P_LON = 4.3517, P_LAT = 50.8503;
    private static final double RADIUS_METRES = 5_000.0;
    private static final double D_P_METRES = 5_000.0, D_MEET_METRES = 5_000.0;
    private static final double XMIN = 4.30, YMIN = 50.84, XMAX = 4.36, YMAX = 50.86;
    private static final double S1_LON = 4.30, S1_LAT = 50.83, S2_LON = 4.36, S2_LAT = 50.87;
    // Vehicle ids run 100 .. 100+numVehicles-1; the per-vehicle query targets are
    // chosen inside that range so each cell does representative work.
    private static int TARGET_VEHICLE_ID, X_VEHICLE_ID, Y_VEHICLE_ID;
    private static final long WINDOW_SIZE_SECONDS = 10L;
    private static final long SNAPSHOT_TICK_MILLIS = 5_000L;
    private static final long T0 = 1_735_711_200_000L;
    private static final double CENTRE_LON = 4.3517, CENTRE_LAT = 50.8503;
    private static final double SPREAD_DEG = 0.12;        // ~±13 km grid around the centre
    private static final long SPAN_MILLIS = 600_000L;     // 10 min of event time

    private static final List<PointOfInterest> POIS = Arrays.asList(
            new PointOfInterest(1, 4.3517, 50.8503, 2_000.0),
            new PointOfInterest(2, 4.3060, 50.8270, 1_000.0),
            new PointOfInterest(3, 4.2100, 50.7600, 2_000.0));

    private static final ConcurrentHashMap<String, LongAdder> COUNTS = new ConcurrentHashMap<>();

    private BerlinMODBenchmark() { /* utility */ }

    public static void main(String[] args) throws Exception {
        int numVehicles = args.length > 0 ? Integer.parseInt(args[0]) : 50;
        int eventsPerVehicle = args.length > 1 ? Integer.parseInt(args[1]) : 600;
        TARGET_VEHICLE_ID = 100 + numVehicles / 2;
        X_VEHICLE_ID = 100;
        Y_VEHICLE_ID = 100 + numVehicles / 2;
        List<BerlinMODTrip> corpus = buildCorpus(numVehicles, eventsPerVehicle);
        int n = corpus.size();
        System.out.printf("Corpus: %d vehicles × %d events = %d events over %ds%n",
                numVehicles, eventsPerVehicle, n, SPAN_MILLIS / 1000);

        Map<String, Function<DataStream<BerlinMODTrip>, DataStream<?>>> cells = new LinkedHashMap<>();
        cells.put("Q1-continuous", t -> t.keyBy(BerlinMODTrip::getVehicleId).process(new Q1ContinuousFunction()));
        cells.put("Q1-windowed", t -> t.windowAll(tumble()).process(new Q1WindowedFunction()));
        cells.put("Q1-snapshot", t -> t.keyBy(BerlinMODTrip::getVehicleId).process(new Q1SnapshotFunction(SNAPSHOT_TICK_MILLIS)));
        cells.put("Q2-continuous", t -> t.process(new Q2ContinuousFunction(TARGET_VEHICLE_ID)));
        cells.put("Q2-windowed", t -> t.windowAll(tumble()).process(new Q2WindowedFunction(TARGET_VEHICLE_ID)));
        cells.put("Q2-snapshot", t -> t.keyBy(BerlinMODTrip::getVehicleId).process(new Q2SnapshotFunction(TARGET_VEHICLE_ID, SNAPSHOT_TICK_MILLIS)));
        cells.put("Q3-continuous", t -> t.process(new Q3ContinuousFunction(P_LON, P_LAT, RADIUS_METRES)));
        cells.put("Q3-windowed", t -> t.windowAll(tumble()).process(new Q3WindowedFunction(P_LON, P_LAT, RADIUS_METRES)));
        cells.put("Q3-snapshot", t -> t.keyBy(BerlinMODTrip::getVehicleId).process(new Q3SnapshotFunction(P_LON, P_LAT, RADIUS_METRES, SNAPSHOT_TICK_MILLIS)));
        cells.put("Q4-continuous", t -> t.keyBy(BerlinMODTrip::getVehicleId).process(new Q4ContinuousFunction(XMIN, YMIN, XMAX, YMAX)));
        cells.put("Q4-windowed", t -> t.windowAll(tumble()).process(new Q4WindowedFunction(XMIN, YMIN, XMAX, YMAX)));
        cells.put("Q4-snapshot", t -> t.keyBy(BerlinMODTrip::getVehicleId).process(new Q4SnapshotFunction(XMIN, YMIN, XMAX, YMAX, SNAPSHOT_TICK_MILLIS)));
        cells.put("Q5-continuous", t -> t.keyBy(x -> 0).process(new Q5ContinuousFunction(P_LON, P_LAT, D_P_METRES, D_MEET_METRES)));
        cells.put("Q5-windowed", t -> t.windowAll(tumble()).process(new Q5WindowedFunction(P_LON, P_LAT, D_P_METRES, D_MEET_METRES)));
        cells.put("Q5-snapshot", t -> t.keyBy(x -> 0).process(new Q5SnapshotFunction(P_LON, P_LAT, D_P_METRES, D_MEET_METRES, SNAPSHOT_TICK_MILLIS)));
        cells.put("Q6-continuous", t -> t.keyBy(BerlinMODTrip::getVehicleId).process(new Q6ContinuousFunction()));
        cells.put("Q6-windowed", t -> t.keyBy(BerlinMODTrip::getVehicleId).window(tumble()).process(new Q6WindowedFunction()));
        cells.put("Q6-snapshot", t -> t.keyBy(BerlinMODTrip::getVehicleId).process(new Q6SnapshotFunction(SNAPSHOT_TICK_MILLIS)));
        cells.put("Q7-continuous", t -> t.keyBy(BerlinMODTrip::getVehicleId).process(new Q7ContinuousFunction(POIS)));
        cells.put("Q7-windowed", t -> t.windowAll(tumble()).process(new Q7WindowedFunction(POIS)));
        cells.put("Q7-snapshot", t -> t.keyBy(BerlinMODTrip::getVehicleId).process(new Q7SnapshotFunction(POIS, SNAPSHOT_TICK_MILLIS)));
        cells.put("Q8-continuous", t -> t.process(new Q8ContinuousFunction(S1_LON, S1_LAT, S2_LON, S2_LAT, RADIUS_METRES)));
        cells.put("Q8-windowed", t -> t.windowAll(tumble()).process(new Q8WindowedFunction(S1_LON, S1_LAT, S2_LON, S2_LAT, RADIUS_METRES)));
        cells.put("Q8-snapshot", t -> t.keyBy(BerlinMODTrip::getVehicleId).process(new Q8SnapshotFunction(S1_LON, S1_LAT, S2_LON, S2_LAT, RADIUS_METRES, SNAPSHOT_TICK_MILLIS)));
        cells.put("Q9-continuous", t -> t.keyBy(x -> 0).process(new Q9ContinuousFunction(X_VEHICLE_ID, Y_VEHICLE_ID)));
        cells.put("Q9-windowed", t -> t.windowAll(tumble()).process(new Q9WindowedFunction(X_VEHICLE_ID, Y_VEHICLE_ID)));
        cells.put("Q9-snapshot", t -> t.keyBy(x -> 0).process(new Q9SnapshotFunction(X_VEHICLE_ID, Y_VEHICLE_ID, SNAPSHOT_TICK_MILLIS)));

        String only = args.length > 2 ? args[2] : null;
        List<String[]> rows = new ArrayList<>();
        for (Map.Entry<String, Function<DataStream<BerlinMODTrip>, DataStream<?>>> cell : cells.entrySet()) {
            if (only != null && !cell.getKey().contains(only)) {
                continue;
            }
            long[] r = runCell(cell.getKey(), cell.getValue(), corpus);
            double secs = r[1] / 1000.0;
            double tput = secs > 0 ? n / secs : 0;
            rows.add(new String[]{cell.getKey(), String.valueOf(n), String.valueOf(r[0]),
                    String.valueOf(r[1]), String.format("%,.0f", tput)});
            System.out.printf("  %-14s out=%-8d %6d ms  %,.0f ev/s%n", cell.getKey(), r[0], r[1], tput);
        }

        System.out.println();
        System.out.println("| Cell | Events in | Output rows | Wall (ms) | Throughput (ev/s) |");
        System.out.println("|---|---:|---:|---:|---:|");
        for (String[] r : rows) {
            System.out.printf("| %s | %s | %s | %s | %s |%n", r[0], r[1], r[2], r[3], r[4]);
        }
    }

    /** @return {outputRows, wallMillis} for one cell. */
    private static long[] runCell(String name,
                                  Function<DataStream<BerlinMODTrip>, DataStream<?>> wiring,
                                  List<BerlinMODTrip> corpus) throws Exception {
        COUNTS.put(name, new LongAdder());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<BerlinMODTrip> trips = env.fromCollection(corpus)
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<BerlinMODTrip>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((e, ts) -> e.getTimestamp()));
        @SuppressWarnings("unchecked")
        DataStream<Object> out = (DataStream<Object>) wiring.apply(trips);
        out.addSink(new CountingSink(name));
        long t0 = System.nanoTime();
        env.execute(name);
        long wall = (System.nanoTime() - t0) / 1_000_000L;
        return new long[]{COUNTS.get(name).sum(), wall};
    }

    private static TumblingEventTimeWindows tumble() {
        return TumblingEventTimeWindows.of(Time.seconds(WINDOW_SIZE_SECONDS));
    }

    /** Deterministic synthetic corpus: vehicles spread on a disc around the
     * centre, drifting per event, with monotonically increasing timestamps. */
    private static List<BerlinMODTrip> buildCorpus(int vehicles, int perVehicle) {
        int total = vehicles * perVehicle;
        long step = Math.max(1L, SPAN_MILLIS / total);
        List<BerlinMODTrip> events = new ArrayList<>(total);
        long g = 0;
        for (int e = 0; e < perVehicle; e++) {
            for (int v = 0; v < vehicles; v++) {
                // Stable per-vehicle base on a deterministic grid, plus a small per-event drift.
                double ang = (v * 2.399963) % (2 * Math.PI);            // golden-angle spread
                double rad = SPREAD_DEG * ((v % 17) / 17.0);
                double drift = 0.0005 * Math.sin((e + v) * 0.13);
                double lon = CENTRE_LON + rad * Math.cos(ang) + drift;
                double lat = CENTRE_LAT + rad * Math.sin(ang) + drift;
                BerlinMODTrip trip = new BerlinMODTrip();
                trip.setVehicleId(100 + v);
                trip.setTimestamp(T0 + g * step);
                trip.setLon(lon);
                trip.setLat(lat);
                events.add(trip);
                g++;
            }
        }
        return events;
    }

    /** Counts records into the shared per-cell {@link LongAdder}. */
    private static final class CountingSink extends RichSinkFunction<Object> {
        private final String cell;
        CountingSink(String cell) { this.cell = cell; }
        @Override public void open(Configuration p) { }
        @Override public void invoke(Object value, Context context) {
            COUNTS.get(cell).increment();
        }
    }
}
