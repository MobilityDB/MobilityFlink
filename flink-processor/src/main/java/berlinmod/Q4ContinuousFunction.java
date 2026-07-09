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

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * BerlinMOD-Q4 — <b>continuous form</b>.
 *
 * <p><i>"Which vehicles entered region R (transition outside → inside),
 * and when?"</i>
 *
 * <p>Keyed by vehicleId. Per-vehicle state tracks the last seen
 * inside-or-outside flag for R. On each event, computes the current
 * inside-or-outside, and if the transition is outside→inside, emits
 * {@code (vehicleId, entryTime)}.
 *
 * <p>Predicate: {@link MEOSBridge#intersectsBox} — MEOS
 * {@code eintersects_tgeo_geo} between the point's {@code tgeompoint} instant
 * and the region polygon.
 */
public class Q4ContinuousFunction
        extends KeyedProcessFunction<Integer, BerlinMODTrip, Tuple2<Integer, Long>> {

    private final double xmin, ymin, xmax, ymax;
    private transient ValueState<Boolean> wasInside;

    public Q4ContinuousFunction(double xmin, double ymin, double xmax, double ymax) {
        this.xmin = xmin;
        this.ymin = ymin;
        this.xmax = xmax;
        this.ymax = ymax;
    }

    @Override
    public void open(OpenContext parameters) {
        wasInside = getRuntimeContext().getState(
                new ValueStateDescriptor<>("q4WasInside", Boolean.class));
    }

    @Override
    public void processElement(
            BerlinMODTrip trip,
            Context ctx,
            Collector<Tuple2<Integer, Long>> out) throws Exception {
        boolean isInside = inBox(trip.getLon(), trip.getLat());
        Boolean prev = wasInside.value();
        boolean prevInside = prev != null && prev;
        if (isInside && !prevInside) {
            out.collect(new Tuple2<>(trip.getVehicleId(), trip.getTimestamp()));
        }
        wasInside.update(isInside);
    }

    private boolean inBox(double lon, double lat) {
        return MEOSBridge.intersectsBox(lon, lat, xmin, ymin, xmax, ymax);
    }
}
