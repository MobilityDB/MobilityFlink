package berlinmod;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * BerlinMOD-Q4 — <b>windowed form</b>.
 *
 * <p><i>"Per N-second tumbling window, which vehicles entered region R during
 * the window, and at what time?"</i>
 *
 * <p>Scans all events in the window, sorted per-vehicle by time, and detects
 * outside → inside transitions within the window. Emits one
 * {@code (windowStart, windowEnd, vehicleId, entryTime)} per detected entry.
 *
 * <p>Note: a vehicle's "outside" state at window start is inferred only from
 * the window's first event (no cross-window state). This intra-window
 * scoping matches BerlinMOD-Q4's "first N entries during a period" form.
 */
public class Q4WindowedFunction
        extends ProcessAllWindowFunction<BerlinMODTrip, Tuple4<Long, Long, Integer, Long>, TimeWindow> {

    private final double xmin, ymin, xmax, ymax;

    public Q4WindowedFunction(double xmin, double ymin, double xmax, double ymax) {
        this.xmin = xmin;
        this.ymin = ymin;
        this.xmax = xmax;
        this.ymax = ymax;
    }

    @Override
    public void process(
            Context ctx,
            Iterable<BerlinMODTrip> elements,
            Collector<Tuple4<Long, Long, Integer, Long>> out) {
        Map<Integer, List<BerlinMODTrip>> perVehicle = new HashMap<>();
        for (BerlinMODTrip trip : elements) {
            perVehicle.computeIfAbsent(trip.getVehicleId(), k -> new ArrayList<>()).add(trip);
        }
        for (Map.Entry<Integer, List<BerlinMODTrip>> e : perVehicle.entrySet()) {
            List<BerlinMODTrip> sorted = e.getValue();
            sorted.sort((a, b) -> Long.compare(a.getTimestamp(), b.getTimestamp()));
            boolean prevInside = false; // intra-window only — treats first event as the prior
            for (int i = 0; i < sorted.size(); i++) {
                BerlinMODTrip t = sorted.get(i);
                boolean curr = inBox(t.getLon(), t.getLat());
                if (i == 0) {
                    if (curr) {
                        // first event already inside — count as entry per the
                        // intra-window scoping convention (no prior visibility)
                        out.collect(new Tuple4<>(ctx.window().getStart(), ctx.window().getEnd(),
                                e.getKey(), t.getTimestamp()));
                    }
                } else if (curr && !prevInside) {
                    out.collect(new Tuple4<>(ctx.window().getStart(), ctx.window().getEnd(),
                            e.getKey(), t.getTimestamp()));
                }
                prevInside = curr;
            }
        }
    }

    private boolean inBox(double lon, double lat) {
        return MEOSBridge.intersectsBox(lon, lat, xmin, ymin, xmax, ymax);
    }
}
