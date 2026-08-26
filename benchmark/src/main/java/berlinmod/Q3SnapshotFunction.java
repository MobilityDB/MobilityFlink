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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BerlinMOD-Q3 — <b>snapshot form</b>.
 *
 * <p><i>"At time T, which vehicles are within {@code d} metres of point P?"</i>
 *
 * <p>This is the <b>parity-oracle form</b>: streaming output at watermark T
 * must equal the batch BerlinMOD-Q3 result on the same data up to T.
 *
 * <p>Keyed by vehicleId. Maintains a per-vehicle {@code lastKnownPosition}
 * state. On each event, update the state, then register an event-time timer
 * for the snapshot tick. When the timer fires at time T, evaluate the radius
 * predicate against the most recent known position and emit
 * {@code (T, vehicleId)} if the vehicle is within {@code d} of P at that
 * snapshot.
 *
 * <p>Predicate: {@link MEOSBridge#dwithinMetres} — MEOS
 * {@code edwithin_tgeo_geo} over WGS84 geographies. The snapshot-form output
 * at watermark T is equal to the batch BerlinMOD-Q3 result up to T.
 */
public class Q3SnapshotFunction
        extends KeyedProcessFunction<Integer, BerlinMODTrip, Tuple2<Long, Integer>> {

    private static final Logger LOG = LoggerFactory.getLogger(Q3SnapshotFunction.class);

    private final double pLon;
    private final double pLat;
    private final double radiusMetres;
    private final long snapshotTickMillis;

    private transient ValueState<Tuple3<Double, Double, Long>> lastKnown; // (lon, lat, ts)

    public Q3SnapshotFunction(
            double pLon, double pLat, double radiusMetres, long snapshotTickMillis) {
        this.pLon = pLon;
        this.pLat = pLat;
        this.radiusMetres = radiusMetres;
        this.snapshotTickMillis = snapshotTickMillis;
    }

    @Override
    public void open(OpenContext parameters) {
        TypeInformation<Tuple3<Double, Double, Long>> tInfo =
                TypeInformation.of(new TypeHint<Tuple3<Double, Double, Long>>() {});
        ValueStateDescriptor<Tuple3<Double, Double, Long>> desc =
                new ValueStateDescriptor<>("lastKnownPosition", tInfo);
        lastKnown = getRuntimeContext().getState(desc);
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
        if (MEOSBridge.dwithinMetres(p.f0, p.f1, pLon, pLat, radiusMetres)) {
            Integer vehicleId = ctx.getCurrentKey();
            out.collect(new Tuple2<>(timestamp, vehicleId));
            if (LOG.isDebugEnabled()) {
                LOG.debug("Q3-snapshot: T={} vehicle={}", timestamp, vehicleId);
            }
        }
    }
}
