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
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * BerlinMOD-Q6 — <b>snapshot form</b>.
 *
 * <p><i>"At time T, what is each vehicle's total distance travelled up to T?"</i>
 *
 * <p>This is the parity-oracle form: streaming output at watermark T must
 * equal the batch BerlinMOD-Q6 result on the same data up to T.
 *
 * <p>Keyed by vehicleId. Per event, update {@code lastPos}/{@code totalDist}
 * state (matching {@link Q6ContinuousFunction}) and register an event-time
 * timer at the next snapshot tick. On timer fire at T, emit
 * {@code (T, vehicleId, totalMetres)}.
 */
public class Q6SnapshotFunction
        extends KeyedProcessFunction<Integer, BerlinMODTrip, Tuple3<Long, Integer, Double>> {

    private final long snapshotTickMillis;
    private transient ValueState<Tuple2<Double, Double>> lastPos;
    private transient ValueState<Double> totalDist;

    public Q6SnapshotFunction(long snapshotTickMillis) {
        this.snapshotTickMillis = snapshotTickMillis;
    }

    @Override
    public void open(OpenContext parameters) {
        TypeInformation<Tuple2<Double, Double>> posType =
                TypeInformation.of(new TypeHint<Tuple2<Double, Double>>() {});
        lastPos = getRuntimeContext().getState(
                new ValueStateDescriptor<>("q6SnapLastPos", posType));
        totalDist = getRuntimeContext().getState(
                new ValueStateDescriptor<>("q6SnapTotalDist", Double.class));
    }

    @Override
    public void processElement(
            BerlinMODTrip trip,
            Context ctx,
            Collector<Tuple3<Long, Integer, Double>> out) throws Exception {
        Tuple2<Double, Double> prev = lastPos.value();
        Double total = totalDist.value();
        if (total == null) {
            total = 0.0;
        }
        if (prev != null) {
            total += MEOSBridge.distanceMetres(prev.f0, prev.f1, trip.getLon(), trip.getLat());
        }
        lastPos.update(new Tuple2<>(trip.getLon(), trip.getLat()));
        totalDist.update(total);
        long nextTick = ((trip.getTimestamp() / snapshotTickMillis) + 1) * snapshotTickMillis;
        ctx.timerService().registerEventTimeTimer(nextTick);
    }

    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext ctx,
            Collector<Tuple3<Long, Integer, Double>> out) throws Exception {
        Double total = totalDist.value();
        if (total != null) {
            out.collect(new Tuple3<>(timestamp, ctx.getCurrentKey(), total));
        }
    }
}
