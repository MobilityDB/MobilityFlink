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

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BerlinMOD-Q2 — <b>continuous form</b>.
 *
 * <p><i>"Where is vehicle X right now?"</i>
 *
 * <p>For each incoming GPS event {@link BerlinMODTrip}, emit it unchanged if it
 * belongs to the queried vehicle, otherwise drop. No windowing, no state —
 * a per-event filter against {@code targetVehicleId}.
 */
public class Q2ContinuousFunction extends ProcessFunction<BerlinMODTrip, BerlinMODTrip> {

    private static final Logger LOG = LoggerFactory.getLogger(Q2ContinuousFunction.class);

    private final int targetVehicleId;

    public Q2ContinuousFunction(int targetVehicleId) {
        this.targetVehicleId = targetVehicleId;
    }

    @Override
    public void processElement(
            BerlinMODTrip trip,
            Context ctx,
            Collector<BerlinMODTrip> out) {
        if (trip.getVehicleId() == targetVehicleId) {
            out.collect(trip);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Q2-continuous: vehicle={} t={} ({}, {})",
                        trip.getVehicleId(), trip.getTimestamp(), trip.getLon(), trip.getLat());
            }
        }
    }
}
