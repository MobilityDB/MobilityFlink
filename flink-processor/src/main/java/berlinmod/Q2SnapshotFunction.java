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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BerlinMOD-Q2 — <b>snapshot form</b>.
 *
 * <p><i>"At time T, where is vehicle X?"</i>
 *
 * <p>This is the parity-oracle form: streaming output at watermark T must
 * equal the batch BerlinMOD-Q2 result on the same data up to T (the most
 * recent known position of vehicle X on or before T).
 *
 * <p>Keyed by vehicleId (so the operator scales naturally if the queried
 * vehicle changes, and so reuse across multiple queried vehicles is a
 * fan-out keying choice rather than a code change). For events whose key
 * matches {@code targetVehicleId}, update last-known state and register an
 * event-time timer for the next snapshot tick. When the timer fires, emit
 * {@code (T, lon, lat, t_of_last_event)}.
 */
public class Q2SnapshotFunction
        extends KeyedProcessFunction<Integer, BerlinMODTrip, Tuple4<Long, Double, Double, Long>> {

    private static final Logger LOG = LoggerFactory.getLogger(Q2SnapshotFunction.class);

    private final int targetVehicleId;
    private final long snapshotTickMillis;

    private transient ValueState<Tuple3<Double, Double, Long>> lastKnown; // (lon, lat, ts)

    public Q2SnapshotFunction(int targetVehicleId, long snapshotTickMillis) {
        this.targetVehicleId = targetVehicleId;
        this.snapshotTickMillis = snapshotTickMillis;
    }

    @Override
    public void open(Configuration parameters) {
        TypeInformation<Tuple3<Double, Double, Long>> tInfo =
                TypeInformation.of(new TypeHint<Tuple3<Double, Double, Long>>() {});
        ValueStateDescriptor<Tuple3<Double, Double, Long>> desc =
                new ValueStateDescriptor<>("q2LastKnownPosition", tInfo);
        lastKnown = getRuntimeContext().getState(desc);
    }

    @Override
    public void processElement(
            BerlinMODTrip trip,
            Context ctx,
            Collector<Tuple4<Long, Double, Double, Long>> out) throws Exception {
        if (trip.getVehicleId() != targetVehicleId) {
            return;
        }
        lastKnown.update(new Tuple3<>(trip.getLon(), trip.getLat(), trip.getTimestamp()));
        long nextTick = ((trip.getTimestamp() / snapshotTickMillis) + 1) * snapshotTickMillis;
        ctx.timerService().registerEventTimeTimer(nextTick);
    }

    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext ctx,
            Collector<Tuple4<Long, Double, Double, Long>> out) throws Exception {
        Tuple3<Double, Double, Long> p = lastKnown.value();
        if (p == null) {
            return;
        }
        out.collect(new Tuple4<>(timestamp, p.f0, p.f1, p.f2));
        if (LOG.isDebugEnabled()) {
            LOG.debug("Q2-snapshot: T={} vehicle={} ({}, {}) at t={}",
                    timestamp, ctx.getCurrentKey(), p.f0, p.f1, p.f2);
        }
    }
}
