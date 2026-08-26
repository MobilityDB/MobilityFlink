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
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BerlinMOD-Q3 — <b>continuous form</b>.
 *
 * <p><i>"At every moment, which vehicles are currently within {@code d} metres
 * of point P?"</i>
 *
 * <p>For each incoming GPS event {@link BerlinMODTrip}, evaluate the radius
 * predicate and emit {@code (vehicleId, eventTimeMillis, isNear)} per event.
 * No windowing — output updates per event, watermark-independent.
 *
 * <p>Predicate: {@link MEOSBridge#dwithinMetres} — MEOS
 * {@code edwithin_tgeo_geo} over WGS84 geographies.
 */
public class Q3ContinuousFunction extends ProcessFunction<BerlinMODTrip, Tuple3<Integer, Long, Boolean>> {

    private static final Logger LOG = LoggerFactory.getLogger(Q3ContinuousFunction.class);

    private final double pLon;
    private final double pLat;
    private final double radiusMetres;

    public Q3ContinuousFunction(double pLon, double pLat, double radiusMetres) {
        this.pLon = pLon;
        this.pLat = pLat;
        this.radiusMetres = radiusMetres;
    }

    @Override
    public void processElement(
            BerlinMODTrip trip,
            Context ctx,
            Collector<Tuple3<Integer, Long, Boolean>> out) {
        boolean near = MEOSBridge.dwithinMetres(trip.getLon(), trip.getLat(), pLon, pLat, radiusMetres);
        out.collect(new Tuple3<>(trip.getVehicleId(), trip.getTimestamp(), near));
        if (LOG.isDebugEnabled()) {
            LOG.debug("Q3-continuous: vehicle={} ts={} near={}", trip.getVehicleId(), trip.getTimestamp(), near);
        }
    }
}
