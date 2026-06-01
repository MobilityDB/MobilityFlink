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

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BerlinMOD-Q2 — <b>windowed form</b>.
 *
 * <p><i>"Per N-second tumbling window, what is vehicle X's most recent
 * position seen within the window?"</i>
 *
 * <p>For each window, filter to events matching {@code targetVehicleId}, keep
 * the event with the largest timestamp, and emit
 * {@code (windowStart, windowEnd, vehicleId, lon, lat)}. If the vehicle had
 * no events in the window, emit nothing.
 */
public class Q2WindowedFunction
        extends ProcessAllWindowFunction<BerlinMODTrip, Tuple5<Long, Long, Integer, Double, Double>, TimeWindow> {

    private static final Logger LOG = LoggerFactory.getLogger(Q2WindowedFunction.class);

    private final int targetVehicleId;

    public Q2WindowedFunction(int targetVehicleId) {
        this.targetVehicleId = targetVehicleId;
    }

    @Override
    public void process(
            Context ctx,
            Iterable<BerlinMODTrip> elements,
            Collector<Tuple5<Long, Long, Integer, Double, Double>> out) {
        BerlinMODTrip latest = null;
        for (BerlinMODTrip trip : elements) {
            if (trip.getVehicleId() != targetVehicleId) {
                continue;
            }
            if (latest == null || trip.getTimestamp() > latest.getTimestamp()) {
                latest = trip;
            }
        }
        if (latest != null) {
            out.collect(new Tuple5<>(
                    ctx.window().getStart(),
                    ctx.window().getEnd(),
                    latest.getVehicleId(),
                    latest.getLon(),
                    latest.getLat()));
            LOG.info("Q2-windowed: [{}, {}) vehicle={} last=({}, {})",
                    ctx.window().getStart(), ctx.window().getEnd(),
                    latest.getVehicleId(), latest.getLon(), latest.getLat());
        }
    }
}
