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
