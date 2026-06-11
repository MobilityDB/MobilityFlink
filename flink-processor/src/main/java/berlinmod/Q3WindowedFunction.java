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

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * BerlinMOD-Q3 — <b>windowed form</b>.
 *
 * <p><i>"Per N-second window, how many distinct vehicles were within
 * {@code d} metres of point P at any time during the window?"</i>
 *
 * <p>Tumbling event-time window of configurable size. For each window, scan
 * all events whose timestamp falls in the window, count distinct vehicleIds
 * for which at least one event satisfies the radius predicate, and emit
 * {@code (windowStart, windowEnd, distinctCount)}.
 *
 * <p>Predicate: {@link MEOSBridge#dwithinMetres} — MEOS
 * {@code edwithin_tgeo_geo} over WGS84 geographies.
 */
public class Q3WindowedFunction
        extends ProcessAllWindowFunction<BerlinMODTrip, Tuple3<Long, Long, Long>, TimeWindow> {

    private static final Logger LOG = LoggerFactory.getLogger(Q3WindowedFunction.class);

    private final double pLon;
    private final double pLat;
    private final double radiusMetres;

    public Q3WindowedFunction(double pLon, double pLat, double radiusMetres) {
        this.pLon = pLon;
        this.pLat = pLat;
        this.radiusMetres = radiusMetres;
    }

    @Override
    public void process(
            Context ctx,
            Iterable<BerlinMODTrip> elements,
            Collector<Tuple3<Long, Long, Long>> out) {
        Set<Integer> distinctNear = new HashSet<>();
        for (BerlinMODTrip trip : elements) {
            if (MEOSBridge.dwithinMetres(trip.getLon(), trip.getLat(), pLon, pLat, radiusMetres)) {
                distinctNear.add(trip.getVehicleId());
            }
        }
        long count = distinctNear.size();
        out.collect(new Tuple3<>(ctx.window().getStart(), ctx.window().getEnd(), count));
        LOG.info("Q3-windowed: [{}, {}) distinct-near={}",
                ctx.window().getStart(), ctx.window().getEnd(), count);
    }
}
