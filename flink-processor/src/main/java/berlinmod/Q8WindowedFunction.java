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

import java.util.HashSet;
import java.util.Set;

/**
 * BerlinMOD-Q8 — <b>windowed form</b>.
 *
 * <p><i>"Per N-second tumbling window, how many distinct vehicles were within
 * {@code d} metres of the road segment at any time during the window?"</i>
 *
 * <p>Tumbling event-time window. Walk all events in the window, count
 * distinct vehicleIds for which at least one event satisfies the
 * segment-distance predicate. Emit {@code (windowStart, windowEnd,
 * distinctCount)}.
 */
public class Q8WindowedFunction
        extends ProcessAllWindowFunction<BerlinMODTrip, Tuple3<Long, Long, Long>, TimeWindow> {

    private final double s1Lon, s1Lat, s2Lon, s2Lat, radiusMetres;

    public Q8WindowedFunction(double s1Lon, double s1Lat,
                              double s2Lon, double s2Lat,
                              double radiusMetres) {
        this.s1Lon = s1Lon;
        this.s1Lat = s1Lat;
        this.s2Lon = s2Lon;
        this.s2Lat = s2Lat;
        this.radiusMetres = radiusMetres;
    }

    @Override
    public void process(
            Context ctx,
            Iterable<BerlinMODTrip> elements,
            Collector<Tuple3<Long, Long, Long>> out) {
        Set<Integer> distinctNear = new HashSet<>();
        for (BerlinMODTrip trip : elements) {
            if (MEOSBridge.dwithinSegmentMetres(
                    trip.getLon(), trip.getLat(),
                    s1Lon, s1Lat, s2Lon, s2Lat,
                    radiusMetres)) {
                distinctNear.add(trip.getVehicleId());
            }
        }
        out.collect(new Tuple3<>(ctx.window().getStart(), ctx.window().getEnd(), (long) distinctNear.size()));
    }
}
