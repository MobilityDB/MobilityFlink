package berlinmod;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * BerlinMOD-Q3 — <b>windowed form</b>.
 *
 * <p><i>"Per N-second window, how many distinct vehicles were within
 * {@code d} metres of point P at any time during the window?"</i>
 *
 * <p>Tumbling event-time window of configurable size. For each window, scan
 * all events whose timestamp falls in the window, count distinct vehicleIds
 * for which at least one event satisfies the radius predicate, and emit
 * {@code (windowStart, windowEnd, distinctCount)}.
 *
 * <p>Predicate: {@link MEOSBridge#dwithinMetres} — MEOS {@code geog_dwithin}
 * over WGS84 geographies when libmeos is loadable, with MEOS geog_dwithin/geog_distance
 * fallback otherwise.
 */
public class Q3WindowedFunction
        extends ProcessAllWindowFunction<BerlinMODTrip, Tuple3<Long, Long, Long>, TimeWindow> {

    private static final Logger LOG = LoggerFactory.getLogger(Q3WindowedFunction.class);

    private final double pLon;
    private final double pLat;
    private final double radiusMetres;

    public Q3WindowedFunction(double pLon, double pLat, double radiusMetres) {
        this.pLon = pLon;
        this.pLat = pLat;
        this.radiusMetres = radiusMetres;
    }

    @Override
    public void process(
            Context ctx,
            Iterable<BerlinMODTrip> elements,
            Collector<Tuple3<Long, Long, Long>> out) {
        Set<Integer> distinctNear = new HashSet<>();
        for (BerlinMODTrip trip : elements) {
            if (MEOSBridge.dwithinMetres(trip.getLon(), trip.getLat(), pLon, pLat, radiusMetres)) {
                distinctNear.add(trip.getVehicleId());
            }
        }
        long count = distinctNear.size();
        out.collect(new Tuple3<>(ctx.window().getStart(), ctx.window().getEnd(), count));
        LOG.info("Q3-windowed: [{}, {}) distinct-near={}",
                ctx.window().getStart(), ctx.window().getEnd(), count);
    }
}
