package berlinmod;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * BerlinMOD-Q8 — <b>continuous form</b>.
 *
 * <p><i>"Which vehicles are currently within {@code d} metres of a given
 * road segment?"</i>
 *
 * <p>For each incoming GPS event {@link BerlinMODTrip}, evaluate the
 * point-to-segment distance and emit {@code (vehicleId, eventTime, near)}
 * per event. No windowing — same shape as {@link Q3ContinuousFunction} but
 * with a segment-distance predicate instead of a point-radius one.
 *
 * <p>Predicate today: pure-Java planar projection over an equirectangular
 * frame centred on the segment midpoint (see {@link SegmentDistance}).
 * TODO(meos): replace with the MEOS {@code distance(tgeompoint,
 * geometry(LINESTRING))} call via the JMEOS bridge.
 */
public class Q8ContinuousFunction extends ProcessFunction<BerlinMODTrip, Tuple3<Integer, Long, Boolean>> {

    private final double s1Lon, s1Lat, s2Lon, s2Lat, radiusMetres;

    public Q8ContinuousFunction(double s1Lon, double s1Lat,
                                double s2Lon, double s2Lat,
                                double radiusMetres) {
        this.s1Lon = s1Lon;
        this.s1Lat = s1Lat;
        this.s2Lon = s2Lon;
        this.s2Lat = s2Lat;
        this.radiusMetres = radiusMetres;
    }

    @Override
    public void processElement(
            BerlinMODTrip trip,
            Context ctx,
            Collector<Tuple3<Integer, Long, Boolean>> out) {
        boolean near = SegmentDistance.withinMetres(
                trip.getLon(), trip.getLat(),
                s1Lon, s1Lat, s2Lon, s2Lat,
                radiusMetres);
        out.collect(new Tuple3<>(trip.getVehicleId(), trip.getTimestamp(), near));
    }
}
