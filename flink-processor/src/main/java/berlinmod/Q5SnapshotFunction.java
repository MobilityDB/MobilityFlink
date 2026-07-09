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

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * BerlinMOD-Q5 — <b>snapshot form</b>.
 *
 * <p><i>"At time T, which pairs of vehicles are meeting near P (using each
 * vehicle's most-recent-known position on or before T)?"</i>
 *
 * <p>This is the parity-oracle form: streaming output at watermark T must
 * equal the batch BerlinMOD-Q5 result on the same data up to T.
 *
 * <p>Caller is expected to key the stream by a constant; the
 * cross-vehicle last-known state is a {@link MapState}. On each event,
 * update last-known and register an event-time timer at the next snapshot
 * tick. On timer fire at T, snapshot the map and emit all meeting pairs.
 */
public class Q5SnapshotFunction
        extends KeyedProcessFunction<Integer, BerlinMODTrip, Tuple4<Long, Integer, Integer, Double>> {

    private final double pLon, pLat, dPMetres, dMeetMetres;
    private final long snapshotTickMillis;
    private transient MapState<Integer, Tuple2<Double, Double>> lastPos;

    public Q5SnapshotFunction(double pLon, double pLat, double dPMetres,
                              double dMeetMetres, long snapshotTickMillis) {
        this.pLon = pLon;
        this.pLat = pLat;
        this.dPMetres = dPMetres;
        this.dMeetMetres = dMeetMetres;
        this.snapshotTickMillis = snapshotTickMillis;
    }

    @Override
    public void open(OpenContext parameters) {
        TypeInformation<Tuple2<Double, Double>> vInfo =
                TypeInformation.of(new TypeHint<Tuple2<Double, Double>>() {});
        lastPos = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("q5SnapLastPos", TypeInformation.of(Integer.class), vInfo));
    }

    @Override
    public void processElement(
            BerlinMODTrip trip,
            Context ctx,
            Collector<Tuple4<Long, Integer, Integer, Double>> out) throws Exception {
        lastPos.put(trip.getVehicleId(), new Tuple2<>(trip.getLon(), trip.getLat()));
        long nextTick = ((trip.getTimestamp() / snapshotTickMillis) + 1) * snapshotTickMillis;
        ctx.timerService().registerEventTimeTimer(nextTick);
    }

    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext ctx,
            Collector<Tuple4<Long, Integer, Integer, Double>> out) throws Exception {
        Map<Integer, Tuple2<Double, Double>> snap = new HashMap<>();
        for (Map.Entry<Integer, Tuple2<Double, Double>> e : lastPos.entries()) {
            snap.put(e.getKey(), e.getValue());
        }
        List<Map.Entry<Integer, Tuple2<Double, Double>>> nearP = new ArrayList<>();
        for (Map.Entry<Integer, Tuple2<Double, Double>> e : snap.entrySet()) {
            Tuple2<Double, Double> p = e.getValue();
            if (MEOSBridge.dwithinMetres(p.f0, p.f1, pLon, pLat, dPMetres)) {
                nearP.add(e);
            }
        }
        nearP.sort(Comparator.comparingInt(Map.Entry::getKey));

        for (int i = 0; i < nearP.size(); i++) {
            for (int j = i + 1; j < nearP.size(); j++) {
                Tuple2<Double, Double> a = nearP.get(i).getValue();
                Tuple2<Double, Double> b = nearP.get(j).getValue();
                double d = MEOSBridge.distanceMetres(a.f0, a.f1, b.f0, b.f1);
                if (d <= dMeetMetres) {
                    out.collect(new Tuple4<>(timestamp,
                            nearP.get(i).getKey(), nearP.get(j).getKey(), d));
                }
            }
        }
    }
}
