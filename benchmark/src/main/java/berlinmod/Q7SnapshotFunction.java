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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * BerlinMOD-Q7 — <b>snapshot form</b>.
 *
 * <p><i>"At time T, for each (vehicle, POI), the first time the vehicle came
 * within the POI's radius on or before T."</i>
 *
 * <p>This is the parity-oracle form: streaming output at watermark T must
 * equal the batch BerlinMOD-Q7 result on the same data up to T.
 *
 * <p>Keyed by vehicleId. Per-vehicle {@code MapState<poiId, firstPassageTime>}.
 * On each event, detect new first-passages (matching {@link Q7ContinuousFunction})
 * and register an event-time timer at the next snapshot tick. On timer fire
 * at T, emit one {@code (T, vehicleId, poiId, firstPassageTime)} per recorded
 * first-passage with {@code firstPassageTime ≤ T}.
 */
public class Q7SnapshotFunction
        extends KeyedProcessFunction<Integer, BerlinMODTrip, Tuple4<Long, Integer, Integer, Long>> {

    private final List<PointOfInterest> pois;
    private final long snapshotTickMillis;
    private transient MapState<Integer, Long> firstPassed;

    public Q7SnapshotFunction(List<PointOfInterest> pois, long snapshotTickMillis) {
        this.pois = pois;
        this.snapshotTickMillis = snapshotTickMillis;
    }

    @Override
    public void open(OpenContext parameters) {
        firstPassed = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("q7SnapFirstPassed",
                        TypeInformation.of(Integer.class),
                        TypeInformation.of(Long.class)));
    }

    @Override
    public void processElement(
            BerlinMODTrip trip,
            Context ctx,
            Collector<Tuple4<Long, Integer, Integer, Long>> out) throws Exception {
        for (PointOfInterest poi : pois) {
            if (firstPassed.contains(poi.id)) {
                continue;
            }
            if (MEOSBridge.dwithinMetres(trip.getLon(), trip.getLat(),
                                       poi.lon, poi.lat, poi.radiusMetres)) {
                firstPassed.put(poi.id, trip.getTimestamp());
            }
        }
        long nextTick = ((trip.getTimestamp() / snapshotTickMillis) + 1) * snapshotTickMillis;
        ctx.timerService().registerEventTimeTimer(nextTick);
    }

    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext ctx,
            Collector<Tuple4<Long, Integer, Integer, Long>> out) throws Exception {
        // Iterate in poiId order for deterministic output
        Map<Integer, Long> sorted = new TreeMap<>(Comparator.naturalOrder());
        for (Map.Entry<Integer, Long> e : firstPassed.entries()) {
            sorted.put(e.getKey(), e.getValue());
        }
        for (Map.Entry<Integer, Long> e : sorted.entrySet()) {
            if (e.getValue() <= timestamp) {
                out.collect(new Tuple4<>(timestamp, ctx.getCurrentKey(), e.getKey(), e.getValue()));
            }
        }
    }
}
