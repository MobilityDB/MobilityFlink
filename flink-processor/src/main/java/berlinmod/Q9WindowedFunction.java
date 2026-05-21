package berlinmod;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * BerlinMOD-Q9 — <b>windowed form</b>.
 *
 * <p><i>"Per N-second tumbling window, what is the distance between vehicles
 * X and Y at the end of the window (their last-seen positions within
 * the window)?"</i>
 *
 * <p>Scans the window's events, keeps the latest X and the latest Y
 * positions, and emits {@code (windowStart, windowEnd, distanceMetres)} if
 * both X and Y were seen in the window. If either was missing, emits nothing
 * (no triangulation against earlier windows — the windowed form is strictly
 * intra-window).
 */
public class Q9WindowedFunction
        extends ProcessAllWindowFunction<BerlinMODTrip, Tuple3<Long, Long, Double>, TimeWindow> {

    private final int xVehicleId;
    private final int yVehicleId;

    public Q9WindowedFunction(int xVehicleId, int yVehicleId) {
        this.xVehicleId = xVehicleId;
        this.yVehicleId = yVehicleId;
    }

    @Override
    public void process(
            Context ctx,
            Iterable<BerlinMODTrip> elements,
            Collector<Tuple3<Long, Long, Double>> out) {
        BerlinMODTrip latestX = null, latestY = null;
        for (BerlinMODTrip trip : elements) {
            if (trip.getVehicleId() == xVehicleId
                    && (latestX == null || trip.getTimestamp() > latestX.getTimestamp())) {
                latestX = trip;
            } else if (trip.getVehicleId() == yVehicleId
                    && (latestY == null || trip.getTimestamp() > latestY.getTimestamp())) {
                latestY = trip;
            }
        }
        if (latestX != null && latestY != null) {
            double d = MEOSBridge.distanceMetres(
                    latestX.getLon(), latestX.getLat(),
                    latestY.getLon(), latestY.getLat());
            out.collect(new Tuple3<>(ctx.window().getStart(), ctx.window().getEnd(), d));
        }
    }
}
