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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

/**
 * Throughput benchmark for the BerlinMOD-9 × 3-form streaming matrix.
 *
 * <p>Runs all 27 cells (9 queries × {continuous, windowed, snapshot} = 27 cells)
 * on the Flink local mini-cluster, with the spatial predicates evaluating
 * through MEOS (see {@link MEOSBridge}). Each cell runs its own job terminated by
 * a counting sink; the harness records input events, output rows, wall-clock,
 * and throughput (events per second), then prints a Markdown results table.
 *
 * <p>The corpus is the real BerlinMOD instants CSV ({@code --csv <path>}, required);
 * the per-query parameters and the window/tick granularity are derived from the
 * corpus by {@link BerlinMODCorpus}.
 *
 * <p>Usage (from {@code flink-processor/}, with an extended libmeos on the
 * loader path and the Flink-on-Java-21 {@code --add-opens} flags):
 * <pre>
 *   java … berlinmod.BerlinMODBenchmark --csv &lt;berlinmod_instants.csv&gt; [--max N] [--only Q3]
 * </pre>
 */
public final class BerlinMODBenchmark {

    private static final ConcurrentHashMap<String, LongAdder> COUNTS = new ConcurrentHashMap<>();

    private BerlinMODBenchmark() { /* utility */ }

    public static void main(String[] args) throws Exception {
        String csv = null, only = null;
        int maxRows = 0;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--csv": csv = args[++i]; break;
                case "--max": maxRows = Integer.parseInt(args[++i]); break;
                case "--only": only = args[++i]; break;
                default: break;
            }
        }

        if (csv == null) {
            System.err.println("--csv <berlinmod_instants.csv> is required: the benchmark runs "
                    + "on the canonical BerlinMOD corpus only.");
            System.exit(2);
        }
        List<BerlinMODTrip> corpus = BerlinMODCorpus.fromInstantsCsv(csv, maxRows);
        int n = corpus.size();
        BerlinMODCorpus.Params p = BerlinMODCorpus.derive(corpus);
        System.out.printf("Corpus: real BerlinMOD instants, %d events; window=%ds tick=%dms; P=(%.5f,%.5f) targets=%d/%d/%d%n",
                n, p.windowSeconds, p.snapshotTickMillis, p.pLon, p.pLat, p.targetId, p.xId, p.yId);

        Map<String, Function<DataStream<BerlinMODTrip>, DataStream<?>>> cells = new LinkedHashMap<>();
        cells.put("Q1-continuous", t -> t.keyBy(BerlinMODTrip::getVehicleId).process(new Q1ContinuousFunction()));
        cells.put("Q1-windowed", t -> t.windowAll(tumble(p)).process(new Q1WindowedFunction()));
        cells.put("Q1-snapshot", t -> t.keyBy(BerlinMODTrip::getVehicleId).process(new Q1SnapshotFunction(p.snapshotTickMillis)));
        cells.put("Q2-continuous", t -> t.process(new Q2ContinuousFunction(p.targetId)));
        cells.put("Q2-windowed", t -> t.windowAll(tumble(p)).process(new Q2WindowedFunction(p.targetId)));
        cells.put("Q2-snapshot", t -> t.keyBy(BerlinMODTrip::getVehicleId).process(new Q2SnapshotFunction(p.targetId, p.snapshotTickMillis)));
        cells.put("Q3-continuous", t -> t.process(new Q3ContinuousFunction(p.pLon, p.pLat, p.radiusMetres)));
        cells.put("Q3-windowed", t -> t.windowAll(tumble(p)).process(new Q3WindowedFunction(p.pLon, p.pLat, p.radiusMetres)));
        cells.put("Q3-snapshot", t -> t.keyBy(BerlinMODTrip::getVehicleId).process(new Q3SnapshotFunction(p.pLon, p.pLat, p.radiusMetres, p.snapshotTickMillis)));
        cells.put("Q4-continuous", t -> t.keyBy(BerlinMODTrip::getVehicleId).process(new Q4ContinuousFunction(p.xmin, p.ymin, p.xmax, p.ymax)));
        cells.put("Q4-windowed", t -> t.windowAll(tumble(p)).process(new Q4WindowedFunction(p.xmin, p.ymin, p.xmax, p.ymax)));
        cells.put("Q4-snapshot", t -> t.keyBy(BerlinMODTrip::getVehicleId).process(new Q4SnapshotFunction(p.xmin, p.ymin, p.xmax, p.ymax, p.snapshotTickMillis)));
        // Q5 (pairwise meet) and Q9 (all-pairs) need every event co-located, so keyBy(x -> 0)
        // routes the whole stream to a single subtask on purpose — a global, non-parallel view.
        cells.put("Q5-continuous", t -> t.keyBy(x -> 0).process(new Q5ContinuousFunction(p.pLon, p.pLat, p.radiusMetres, p.dMeetMetres)));
        cells.put("Q5-windowed", t -> t.windowAll(tumble(p)).process(new Q5WindowedFunction(p.pLon, p.pLat, p.radiusMetres, p.dMeetMetres)));
        cells.put("Q5-snapshot", t -> t.keyBy(x -> 0).process(new Q5SnapshotFunction(p.pLon, p.pLat, p.radiusMetres, p.dMeetMetres, p.snapshotTickMillis)));
        cells.put("Q6-continuous", t -> t.keyBy(BerlinMODTrip::getVehicleId).process(new Q6ContinuousFunction()));
        cells.put("Q6-windowed", t -> t.keyBy(BerlinMODTrip::getVehicleId).window(tumble(p)).process(new Q6WindowedFunction()));
        cells.put("Q6-snapshot", t -> t.keyBy(BerlinMODTrip::getVehicleId).process(new Q6SnapshotFunction(p.snapshotTickMillis)));
        cells.put("Q7-continuous", t -> t.keyBy(BerlinMODTrip::getVehicleId).process(new Q7ContinuousFunction(p.pois)));
        cells.put("Q7-windowed", t -> t.windowAll(tumble(p)).process(new Q7WindowedFunction(p.pois)));
        cells.put("Q7-snapshot", t -> t.keyBy(BerlinMODTrip::getVehicleId).process(new Q7SnapshotFunction(p.pois, p.snapshotTickMillis)));
        cells.put("Q8-continuous", t -> t.process(new Q8ContinuousFunction(p.s1Lon, p.s1Lat, p.s2Lon, p.s2Lat, p.radiusMetres)));
        cells.put("Q8-windowed", t -> t.windowAll(tumble(p)).process(new Q8WindowedFunction(p.s1Lon, p.s1Lat, p.s2Lon, p.s2Lat, p.radiusMetres)));
        cells.put("Q8-snapshot", t -> t.keyBy(BerlinMODTrip::getVehicleId).process(new Q8SnapshotFunction(p.s1Lon, p.s1Lat, p.s2Lon, p.s2Lat, p.radiusMetres, p.snapshotTickMillis)));
        cells.put("Q9-continuous", t -> t.keyBy(x -> 0).process(new Q9ContinuousFunction(p.xId, p.yId)));
        cells.put("Q9-windowed", t -> t.windowAll(tumble(p)).process(new Q9WindowedFunction(p.xId, p.yId)));
        cells.put("Q9-snapshot", t -> t.keyBy(x -> 0).process(new Q9SnapshotFunction(p.xId, p.yId, p.snapshotTickMillis)));

        List<String[]> rows = new ArrayList<>();
        for (Map.Entry<String, Function<DataStream<BerlinMODTrip>, DataStream<?>>> cell : cells.entrySet()) {
            if (only != null && !cell.getKey().contains(only)) {
                continue;
            }
            try {
                long[] r = runCell(cell.getKey(), cell.getValue(), corpus);
                double secs = r[1] / 1000.0;
                double tput = secs > 0 ? n / secs : 0;
                rows.add(new String[]{cell.getKey(), String.valueOf(n), String.valueOf(r[0]),
                        String.valueOf(r[1]), String.format("%,.0f", tput)});
                System.out.printf("  %-14s out=%-8d %6d ms  %,.0f ev/s%n", cell.getKey(), r[0], r[1], tput);
            } catch (Exception e) {
                // One failing cell (e.g. its Flink job) must not abort the whole matrix:
                // record the failure and continue with the next cell.
                rows.add(new String[]{cell.getKey(), String.valueOf(n), "ERROR", "-", "-"});
                System.err.printf("  %-14s FAILED: %s%n", cell.getKey(), e.getMessage());
            }
        }

        System.out.println();
        System.out.println("| Cell | Events in | Output rows | Wall (ms) | Throughput (ev/s) |");
        System.out.println("|---|---:|---:|---:|---:|");
        for (String[] r : rows) {
            System.out.printf("| %s | %s | %s | %s | %s |%n", r[0], r[1], r[2], r[3], r[4]);
        }
    }

    /** Runs one benchmark cell as its own Flink job. Package-private so the test suite
     *  can exercise the cell-run path on a small corpus.
     *  @return {outputRows, wallMillis} for one cell. */
    static long[] runCell(String name,
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

    private static TumblingEventTimeWindows tumble(BerlinMODCorpus.Params p) {
        return TumblingEventTimeWindows.of(Time.seconds(p.windowSeconds));
    }

    /** Counts records into the shared per-cell {@link LongAdder}. */
    private static final class CountingSink extends RichSinkFunction<Object> {
        private final String cell;
        CountingSink(String cell) { this.cell = cell; }
        @Override public void open(Configuration cfg) { }
        @Override public void invoke(Object value, Context context) {
            COUNTS.get(cell).increment();
        }
    }
}
