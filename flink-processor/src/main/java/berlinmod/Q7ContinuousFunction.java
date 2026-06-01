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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * BerlinMOD-Q7 — <b>continuous form</b>.
 *
 * <p><i>"For each (vehicle, POI) pair, when did the vehicle first come within
 * the POI's radius?"</i>
 *
 * <p>Keyed by vehicleId. State is a per-vehicle {@code MapState<poiId,
 * firstPassageTime>}. On each event, walk the POI list; if the vehicle is
 * within a POI's radius AND no first-passage has been recorded for that
 * (vehicle, POI), record it and emit {@code (vehicleId, poiId, firstPassageTime)}.
 */
public class Q7ContinuousFunction
        extends KeyedProcessFunction<Integer, BerlinMODTrip, Tuple3<Integer, Integer, Long>> {

    private final List<PointOfInterest> pois;
    private transient MapState<Integer, Long> firstPassed;

    public Q7ContinuousFunction(List<PointOfInterest> pois) {
        this.pois = pois;
    }

    @Override
    public void open(Configuration parameters) {
        firstPassed = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("q7FirstPassed",
                        TypeInformation.of(Integer.class),
                        TypeInformation.of(Long.class)));
    }

    @Override
    public void processElement(
            BerlinMODTrip trip,
            Context ctx,
            Collector<Tuple3<Integer, Integer, Long>> out) throws Exception {
        for (PointOfInterest poi : pois) {
            if (firstPassed.contains(poi.id)) {
                continue;
            }
            if (MEOSBridge.dwithinMetres(trip.getLon(), trip.getLat(), poi.lon, poi.lat, poi.radiusMetres)) {
                firstPassed.put(poi.id, trip.getTimestamp());
                out.collect(new Tuple3<>(trip.getVehicleId(), poi.id, trip.getTimestamp()));
            }
        }
    }
}
