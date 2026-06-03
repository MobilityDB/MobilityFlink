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
