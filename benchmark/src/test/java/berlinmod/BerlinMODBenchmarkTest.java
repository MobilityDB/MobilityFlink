package berlinmod;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises the {@link BerlinMODBenchmark} cell-run path end-to-end on a small
 * in-memory corpus, so the streaming benchmark is covered by the suite rather than
 * only runnable as a {@code main}. Runs one keyed cell and one single-subtask
 * ({@code keyBy(x -> 0)}) cell through {@link BerlinMODBenchmark#runCell}, which
 * builds and executes a real Flink job whose spatial predicate evaluates through
 * MEOS. Runs only with an extended libmeos on the loader path
 * ({@code -Dmeos.enabled=true}), like the other MEOS-backed tests.
 */
@EnabledIfSystemProperty(named = "meos.enabled", matches = "true")
class BerlinMODBenchmarkTest {

    /** Two vehicles moving on nearby WGS84 tracks — enough for the Flink jobs to run. */
    private static List<BerlinMODTrip> sampleCorpus() {
        List<BerlinMODTrip> events = new ArrayList<>();
        long t0 = 1_600_000_000_000L;
        for (int step = 0; step < 20; step++) {
            for (int vid = 1; vid <= 2; vid++) {
                BerlinMODTrip e = new BerlinMODTrip();
                e.setVehicleId(vid);
                e.setTimestamp(t0 + step * 1000L);
                e.setLon(13.40 + vid * 0.001 + step * 0.0001);
                e.setLat(52.50 + vid * 0.001 + step * 0.0001);
                events.add(e);
            }
        }
        return events;
    }

    @Test
    void keyedCellRunsAndCounts() throws Exception {
        List<BerlinMODTrip> corpus = sampleCorpus();
        long[] r = BerlinMODBenchmark.runCell("Q1-continuous",
                t -> t.keyBy(BerlinMODTrip::getVehicleId).process(new Q1ContinuousFunction()),
                corpus);
        assertNotNull(r);
        assertTrue(r[1] >= 0, "wall-clock millis must be non-negative");
        assertTrue(r[0] >= 0, "output rows must be non-negative");
    }

    @Test
    void singleSubtaskCellRuns() throws Exception {
        List<BerlinMODTrip> corpus = sampleCorpus();
        BerlinMODCorpus.Params p = BerlinMODCorpus.derive(corpus);
        // keyBy(x -> 0) routes every event to one subtask on purpose (the Q5 pairwise-meet
        // query needs a global, non-parallel view of all vehicles).
        long[] r = BerlinMODBenchmark.runCell("Q5-continuous",
                t -> t.keyBy(x -> 0).process(
                        new Q5ContinuousFunction(p.pLon, p.pLat, p.radiusMetres, p.dMeetMetres)),
                corpus);
        assertNotNull(r);
        assertTrue(r[0] >= 0, "output rows must be non-negative");
    }
}
