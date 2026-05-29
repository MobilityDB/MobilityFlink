package berlinmod;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.BiPredicate;

/**
 * Snapshot-form parity check for the BerlinMOD streaming benchmark.
 *
 * <p>The streaming parity contract is that a streaming query computes the same
 * result as a batch evaluation of the same MEOS predicate. This driver verifies
 * it on the continuous form, which is timing-independent: the continuous form
 * emits {@code predicate(event)} for every event, and a batch pass over the same
 * corpus computes {@code predicate(event)} directly through the same
 * {@link MEOSBridge} call. The two must agree event-for-event.
 *
 * <p>Checked queries: Q3 (within {@code d} of point {@code P}, MEOS
 * {@code edwithin_tgeo_geo}) and Q8 (within {@code d} of a road segment, MEOS
 * {@code edwithin_tgeo_geo} against a line). The corpus and parameters are the
 * same as {@link BerlinMODBenchmark}.
 *
 * <pre>
 *   java … berlinmod.BerlinMODParity --csv &lt;berlinmod_instants.csv&gt; [--max N]
 *   java … berlinmod.BerlinMODParity --vehicles 50 --events 600
 * </pre>
 */
public final class BerlinMODParity {

    // Continuous-form outputs in arrival order (parallelism 1, no keyBy → stream
    // order equals corpus order), so element i corresponds to corpus event i.
    private static final ConcurrentLinkedQueue<Boolean> STREAMED = new ConcurrentLinkedQueue<>();

    private BerlinMODParity() { /* utility */ }

    public static void main(String[] args) throws Exception {
        String csv = null;
        int maxRows = 0, vehicles = 50, events = 600;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--csv": csv = args[++i]; break;
                case "--max": maxRows = Integer.parseInt(args[++i]); break;
                case "--vehicles": vehicles = Integer.parseInt(args[++i]); break;
                case "--events": events = Integer.parseInt(args[++i]); break;
                default: break;
            }
        }
        List<BerlinMODTrip> corpus = csv != null
                ? BerlinMODCorpus.fromInstantsCsv(csv, maxRows)
                : BerlinMODCorpus.synthetic(vehicles, events);
        BerlinMODCorpus.Params p = BerlinMODCorpus.derive(corpus);
        System.out.printf("Corpus: %s, %d events; P=(%.5f,%.5f) r=%.0fm%n",
                csv != null ? "real BerlinMOD instants" : "synthetic", corpus.size(),
                p.pLon, p.pLat, p.radiusMetres);

        MeosWiringInit();
        BiPredicate<Double, Double> q3 = (lon, lat) ->
                MEOSBridge.dwithinMetres(lon, lat, p.pLon, p.pLat, p.radiusMetres);
        BiPredicate<Double, Double> q8 = (lon, lat) ->
                MEOSBridge.dwithinSegmentMetres(lon, lat, p.s1Lon, p.s1Lat, p.s2Lon, p.s2Lat, p.radiusMetres);

        List<String[]> rows = new ArrayList<>();
        rows.add(check("Q3", corpus,
                t -> t.process(new Q3ContinuousFunction(p.pLon, p.pLat, p.radiusMetres)), q3));
        rows.add(check("Q8", corpus,
                t -> t.process(new Q8ContinuousFunction(p.s1Lon, p.s1Lat, p.s2Lon, p.s2Lat, p.radiusMetres)), q8));

        System.out.println();
        System.out.println("| Query | Events | Streaming-true | Batch-true | Mismatches | Parity |");
        System.out.println("|---|---:|---:|---:|---:|---|");
        for (String[] r : rows) {
            System.out.printf("| %s | %s | %s | %s | %s | %s |%n", r[0], r[1], r[2], r[3], r[4], r[5]);
        }
    }

    private static String[] check(String query, List<BerlinMODTrip> corpus,
                                  java.util.function.Function<DataStream<BerlinMODTrip>, DataStream<Tuple3<Integer, Long, Boolean>>> wiring,
                                  BiPredicate<Double, Double> batch) throws Exception {
        STREAMED.clear();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<BerlinMODTrip> trips = env.fromCollection(corpus)
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<BerlinMODTrip>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((e, ts) -> e.getTimestamp()));
        wiring.apply(trips).addSink(new CollectSink());
        env.execute("parity-" + query);

        Boolean[] streamed = STREAMED.toArray(new Boolean[0]);
        long streamingTrue = 0, batchTrue = 0, mismatches = 0;
        for (int i = 0; i < corpus.size(); i++) {
            boolean expected = batch.test(corpus.get(i).getLon(), corpus.get(i).getLat());
            if (expected) {
                batchTrue++;
            }
            if (i < streamed.length && streamed[i]) {
                streamingTrue++;
            }
            if (i >= streamed.length || streamed[i].booleanValue() != expected) {
                mismatches++;
            }
        }
        boolean parity = mismatches == 0 && streamed.length == corpus.size();
        System.out.printf("  %s: events=%d streaming-out=%d streaming-true=%d batch-true=%d mismatches=%d parity=%s%n",
                query, corpus.size(), streamed.length, streamingTrue, batchTrue, mismatches, parity ? "YES" : "NO");
        return new String[]{query, String.valueOf(corpus.size()), String.valueOf(streamingTrue),
                String.valueOf(batchTrue), String.valueOf(mismatches), parity ? "exact" : "MISMATCH"};
    }

    private static void MeosWiringInit() {
        org.mobilitydb.flink.meos.wirings.MeosWiringRuntime.ensureInitializedOnThread();
    }

    /** Records each continuous-form output's {@code near} flag in arrival order. */
    private static final class CollectSink extends RichSinkFunction<Tuple3<Integer, Long, Boolean>> {
        @Override public void open(Configuration cfg) { }
        @Override public void invoke(Tuple3<Integer, Long, Boolean> v, Context context) {
            STREAMED.add(v.f2);
        }
    }
}
