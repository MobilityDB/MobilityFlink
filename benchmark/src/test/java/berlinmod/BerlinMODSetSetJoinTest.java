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

import functions.GeneratedFunctions;
import jnr.ffi.Pointer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.mobilitydb.meos.MeosSetSetJoin;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Verifies the BerlinMOD trip-level NxN spatial join (the kernel-pruned
 * {@link MeosSetSetJoin} set-set family) against an independent per-pair scalar
 * baseline ({@code edwithin_tgeo_tgeo} / {@code eintersects_tgeo_tgeo}). The two
 * code paths must agree exactly on which trip pairs ever meet / are always
 * disjoint. Runs only with {@code -Dmeos.enabled=true} and an extended libmeos
 * on the library path.
 */
@EnabledIfSystemProperty(named = "meos.enabled", matches = "true")
class BerlinMODSetSetJoinTest {

    // Four trajectory trips: T1 crosses T0's path mid-window; T3 coincides with
    // T0; T2 is far from everything.
    private static final String[] TRIPS = {
        "[POINT(0 0)@2000-01-01, POINT(10 0)@2000-01-02]",
        "[POINT(5 -100)@2000-01-01, POINT(5 100)@2000-01-02]",
        "[POINT(100 100)@2000-01-01, POINT(110 100)@2000-01-02]",
        "[POINT(0 0)@2000-01-01, POINT(10 0)@2000-01-02]",
    };
    private static final double MEET_DIST = 1.0;

    private static Pointer[] trips;

    @BeforeAll
    static void init() {
        GeneratedFunctions.meos_initialize_error_handler((level, code, message) -> { });
        GeneratedFunctions.meos_initialize();
        trips = new Pointer[TRIPS.length];
        for (int i = 0; i < TRIPS.length; i++) trips[i] = GeneratedFunctions.tgeompoint_in(TRIPS[i]);
    }

    @AfterAll
    static void fini() {
        GeneratedFunctions.meos_finalize();
    }

    private static Set<Long> pairSet(int[][] pairs) {
        Set<Long> s = new HashSet<>();
        for (int[] p : pairs) s.add(((long) p[0] << 32) | (p[1] & 0xffffffffL));
        return s;
    }

    @Test
    void eDwithinPairsMatchesScalarBaseline() {
        Set<Long> kernel = pairSet(MeosSetSetJoin.eDwithinPairs(trips, trips, MEET_DIST));
        Set<Long> baseline = new HashSet<>();
        for (int i = 0; i < trips.length; i++)
            for (int j = 0; j < trips.length; j++)
                if (GeneratedFunctions.edwithin_tgeo_tgeo(trips[i], trips[j], MEET_DIST) == 1)
                    baseline.add(((long) i << 32) | j);
        assertEquals(baseline, kernel, "set-set eDwithinPairs must equal the per-pair edwithin scalar");
        // T0/T3 coincide and T1 crosses T0 — the join is non-empty.
        org.junit.jupiter.api.Assertions.assertFalse(kernel.isEmpty());
    }

    @Test
    void aDisjointPairsMatchesScalarBaseline() {
        Set<Long> kernel = pairSet(MeosSetSetJoin.aDisjointPairs(trips, trips));
        Set<Long> baseline = new HashSet<>();
        for (int i = 0; i < trips.length; i++)
            for (int j = 0; j < trips.length; j++)
                if (GeneratedFunctions.eintersects_tgeo_tgeo(trips[i], trips[j]) == 0)
                    baseline.add(((long) i << 32) | j);
        assertEquals(baseline, kernel, "set-set aDisjointPairs must equal the never-intersecting scalar baseline");
    }

    @Test
    void tDwithinPairsSupersetOfEverWithinWithPeriods() {
        MeosSetSetJoin.TDwithin t = MeosSetSetJoin.tDwithinPairs(trips, trips, MEET_DIST);
        Set<Long> tdw = pairSet(t.pairs);
        Set<Long> ever = pairSet(MeosSetSetJoin.eDwithinPairs(trips, trips, MEET_DIST));
        // Continuous tDwithin also reports transient trajectory crossings (e.g. T0/T1
        // coincide at the mid-window crossing) that the ever-within predicate misses,
        // so the within-interval pairs are a superset of the ever-within pairs.
        org.junit.jupiter.api.Assertions.assertTrue(tdw.containsAll(ever),
            "every ever-within pair has a within-interval");
        for (int k = 0; k < t.pairs.length; k++)
            assertNotNull(t.periodsHexwkb[k], "every within pair carries its in-range period spanset");
    }
}
