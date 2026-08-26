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

/**
 * BerlinMOD-Q9 — <b>windowed form</b>.
 *
 * <p><i>"Per N-second tumbling window, what is the distance between vehicles
 * X and Y at the end of the window (their last-seen positions within
 * the window)?"</i>
 *
 * <p>Scans the window's events, keeps the latest X and the latest Y
 * positions, and emits {@code (windowStart, windowEnd, distanceMetres)} if
 * both X and Y were seen in the window. If either was missing, emits nothing
 * (no triangulation against earlier windows — the windowed form is strictly
 * intra-window).
 */
public class Q9WindowedFunction
        extends ProcessAllWindowFunction<BerlinMODTrip, Tuple3<Long, Long, Double>, TimeWindow> {

    private final int xVehicleId;
    private final int yVehicleId;

    public Q9WindowedFunction(int xVehicleId, int yVehicleId) {
        this.xVehicleId = xVehicleId;
        this.yVehicleId = yVehicleId;
    }

    @Override
    public void process(
            Context ctx,
            Iterable<BerlinMODTrip> elements,
            Collector<Tuple3<Long, Long, Double>> out) {
        BerlinMODTrip latestX = null, latestY = null;
        for (BerlinMODTrip trip : elements) {
            if (trip.getVehicleId() == xVehicleId
                    && (latestX == null || trip.getTimestamp() > latestX.getTimestamp())) {
                latestX = trip;
            } else if (trip.getVehicleId() == yVehicleId
                    && (latestY == null || trip.getTimestamp() > latestY.getTimestamp())) {
                latestY = trip;
            }
        }
        if (latestX != null && latestY != null) {
            double d = MEOSBridge.distanceMetres(
                    latestX.getLon(), latestX.getLat(),
                    latestY.getLon(), latestY.getLat());
            out.collect(new Tuple3<>(ctx.window().getStart(), ctx.window().getEnd(), d));
        }
    }
}
