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
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * BerlinMOD-Q8 — <b>snapshot form</b>.
 *
 * <p><i>"At time T, which vehicles are within {@code d} metres of the road
 * segment (using their last-known position on or before T)?"</i>
 *
 * <p>This is the parity-oracle form: streaming output at watermark T must
 * equal the batch BerlinMOD-Q8 result on the same data up to T.
 *
 * <p>Keyed by vehicleId. State: per-vehicle last-known {@code (lon, lat, t)}.
 * On each event, update state and register an event-time timer at the next
 * snapshot tick. On timer fire at T, evaluate the segment-distance predicate
 * against the latest stored position and emit {@code (T, vehicleId)} for
 * each near vehicle.
 */
public class Q8SnapshotFunction
        extends KeyedProcessFunction<Integer, BerlinMODTrip, Tuple2<Long, Integer>> {

    private final double s1Lon, s1Lat, s2Lon, s2Lat, radiusMetres;
    private final long snapshotTickMillis;
    private transient ValueState<Tuple3<Double, Double, Long>> lastKnown;

    public Q8SnapshotFunction(double s1Lon, double s1Lat,
                              double s2Lon, double s2Lat,
                              double radiusMetres,
                              long snapshotTickMillis) {
        this.s1Lon = s1Lon;
        this.s1Lat = s1Lat;
        this.s2Lon = s2Lon;
        this.s2Lat = s2Lat;
        this.radiusMetres = radiusMetres;
        this.snapshotTickMillis = snapshotTickMillis;
    }

    @Override
    public void open(Configuration parameters) {
        TypeInformation<Tuple3<Double, Double, Long>> tInfo =
                TypeInformation.of(new TypeHint<Tuple3<Double, Double, Long>>() {});
        lastKnown = getRuntimeContext().getState(
                new ValueStateDescriptor<>("q8LastKnown", tInfo));
    }

    @Override
    public void processElement(
            BerlinMODTrip trip,
            Context ctx,
            Collector<Tuple2<Long, Integer>> out) throws Exception {
        lastKnown.update(new Tuple3<>(trip.getLon(), trip.getLat(), trip.getTimestamp()));
        long nextTick = ((trip.getTimestamp() / snapshotTickMillis) + 1) * snapshotTickMillis;
        ctx.timerService().registerEventTimeTimer(nextTick);
    }

    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext ctx,
            Collector<Tuple2<Long, Integer>> out) throws Exception {
        Tuple3<Double, Double, Long> p = lastKnown.value();
        if (p == null) {
            return;
        }
        if (MEOSBridge.dwithinSegmentMetres(p.f0, p.f1, s1Lon, s1Lat, s2Lon, s2Lat, radiusMetres)) {
            out.collect(new Tuple2<>(timestamp, ctx.getCurrentKey()));
        }
    }
}
